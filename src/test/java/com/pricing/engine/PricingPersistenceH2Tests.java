package com.pricing.engine;

import java.math.BigDecimal;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import com.sun.net.httpserver.HttpServer;
import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.pricingStore.PricingRepository;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.RuleEngine;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "engine.max-workers=1")
@AutoConfigureMockMvc
@Import(PricingServiceTests.Configuration.class)
class PricingPersistenceH2Tests {

	static final ExecutorService HTTP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
	static final List<WakeUp> WAKE_UPS = new CopyOnWriteArrayList<>();
	static final AtomicInteger ACTIVE_REQUESTS = new AtomicInteger();
	static final HttpServer TRANSACTION_PROCESSOR = startTransactionProcessor();
	static volatile JdbcTemplate recommendationStore;
	static volatile int[] responses = {204};
	static volatile long responseDelayMillis;
	static volatile CountDownLatch responseGate = new CountDownLatch(0);

	static final AtomicReference<Runnable> EMPTY_SCAN = new AtomicReference<>();

	@DynamicPropertySource
	static void databases(DynamicPropertyRegistry properties) {
		properties.add("transaction-processor.wake-up-url", () ->
				"http://localhost:" + TRANSACTION_PROCESSOR.getAddress().getPort()
						+ "/api/v1/mock/txn/wake");
		properties.add("database.retry-delay", () -> "200ms");
		properties.add("database.recovery-check-interval", () -> "100ms");
		properties.add("transaction-processor.retry-delay", () -> "50ms");
		properties.add("transaction-processor.http-timeout", () -> "100ms");
		properties.add("spring.datasource.url", () -> url("pricingconfig"));
		properties.add("spring.datasource.username", () -> "sa");
		properties.add("spring.datasource.password", () -> "");
		properties.add("spring.datasource.driver-class-name", () -> "org.h2.Driver");
		properties.add("account-store.datasource.url", () -> url("accountdb"));
		properties.add("account-store.datasource.username", () -> "sa");
		properties.add("account-store.datasource.password", () -> "");
		properties.add("account-store.datasource.driver-class-name", () -> "org.h2.Driver");
		properties.add("account-store.jpa.ddl-auto", () -> "update");
		properties.add("recommendation-store.datasource.url", () -> url("recommendationdb"));
		properties.add("recommendation-store.datasource.username", () -> "sa");
		properties.add("recommendation-store.datasource.password", () -> "");
		properties.add("recommendation-store.datasource.driver-class-name", () -> "org.h2.Driver");
	}

	static String url(String database) {
		return "jdbc:h2:mem:pricing_recovery_" + database
				+ ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;NON_KEYWORDS=VALUE"
				+ ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
	}

	@Autowired
	EngineService service;

	@Autowired
	ConfigurableApplicationContext application;

	@Autowired
	MockMvc mockMvc;

	@MockitoSpyBean(name = "accountDataSource")
	DataSource accountDataSource;

	@MockitoSpyBean(name = "recommendationDataSource")
	DataSource recommendationDataSource;

	@Autowired
	PricingRepository pricingRepository;

	@MockitoSpyBean
	RuleEngine ruleEngine;

	@Autowired
	RecommendationRepository recommendationRepository;

	JdbcTemplate accounts;
	JdbcTemplate recommendations;

	@Autowired
	void stores(@Qualifier("accountDataSource") DataSource accountStore,
			@Qualifier("recommendationDataSource") DataSource recommendationStore) {
		accounts = new JdbcTemplate(accountStore);
		recommendations = new JdbcTemplate(recommendationStore);
		PricingPersistenceH2Tests.recommendationStore = recommendations;
	}

	@BeforeEach
	void cleanUp() {
		PricingServiceTests.configurationRead = () -> {};
		PricingServiceTests.januaryAmount = "2.50";
		PricingServiceTests.COMPLETED_BEFORE_EVALUATION.clear();
		EMPTY_SCAN.set(null);
		WAKE_UPS.clear();
		responses = new int[] {204};
		responseDelayMillis = 0;
		responseGate = new CountDownLatch(0);
		recommendations.update("delete from recommendation_eligibility_reasons");
		recommendations.update("delete from fee_request_recommendations");
		recommendations.update("delete from account_recommendations");
		recommendations.update("delete from recommendation_outbox");
		accounts.update("delete from account_attributes");
		accounts.update("delete from fee_requests");
		accounts.update("delete from accounts");
		accounts.update("delete from batch_outbox");
	}

	@AfterEach
	void finishNotifications() {
		responseGate.countDown();
		responseDelayMillis = 0;
		await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(3))
				.until(() -> ACTIVE_REQUESTS.get() == 0);
		PricingServiceTests.configurationRead = () -> {};
		PricingServiceTests.januaryAmount = "2.50";
		PricingServiceTests.COMPLETED_BEFORE_EVALUATION.clear();
	}

	@AfterAll
	static void stopTransactionProcessor() {
		TRANSACTION_PROCESSOR.stop(0);
		HTTP_EXECUTOR.close();
	}

	@Test
	void migrationMakesOutboxTheBatchParentAndPreservesRecommendations() {
		var dataSource = new DriverManagerDataSource(url(UUID.randomUUID().toString()), "sa", "");
		Flyway.configure().dataSource(dataSource).locations("classpath:db/recommendation")
				.target("1").load().migrate();
		var database = new JdbcTemplate(dataSource);
		UUID batchId = UUID.randomUUID();
		database.update("insert into recommendation_batches(batch_id) values (?)", batchId);
		database.update("""
				insert into recommendation_outbox(batch_id, processed) values (?, true)
				""", batchId);
		database.update("""
				insert into account_recommendations(batch_id, account_number, status, pricing_plan_code)
				values (?, 'A1', 'OK', 'PLAN1')
				""", batchId);
		Long accountId = database.queryForObject(
				"select id from account_recommendations", Long.class);
		database.update("""
				insert into fee_request_recommendations(account_id, fee_request_id, status,
				decision, amount) values (?, 42, 'OK', 'WAIVED', 0)
				""", accountId);
		Long feeId = database.queryForObject(
				"select id from fee_request_recommendations", Long.class);
		database.update("""
				insert into recommendation_eligibility_reasons(fee_recommendation_id, reason_code)
				values (?, 'VIP')
				""", feeId);
		var committed = storedRecommendations(database);

		Flyway.configure().dataSource(dataSource).locations("classpath:db/recommendation")
				.load().migrate();

		assertEquals(committed, storedRecommendations(database));
		assertEquals(0, database.queryForObject("""
				select count(*) from information_schema.tables
				where table_schema = 'public' and table_name = 'recommendation_batches'
				""", Integer.class));
		assertThrows(DataIntegrityViolationException.class, () -> database.update("""
				insert into account_recommendations(batch_id, account_number, status)
				values (?, 'A2', 'OK')
				""", UUID.randomUUID()));
		assertThrows(DataIntegrityViolationException.class, () -> database.update(
				"delete from recommendation_outbox where batch_id = ?", batchId));
		assertThrows(DataIntegrityViolationException.class, () -> database.update(
				"insert into recommendation_outbox(batch_id) values (?)", batchId));
	}

	@ParameterizedTest
	@ValueSource(ints = {0, 1})
	void retriesATemporaryAccountStoreFailure(int failedConnection) throws Exception {
		UUID batchId = UUID.randomUUID();
		addAccount(batchId);
		AtomicInteger connections = new AtomicInteger();
		doAnswer(invocation -> {
			if (connections.getAndIncrement() == failedConnection) {
				throw new SQLTransientConnectionException("Temporary outage", "08001");
			}
			return invocation.callRealMethod();
		}).when(accountDataSource).getConnection();

		service.processPendingAccounts();

		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
		assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
	}

	@Test
	void defaultCapRefillsSlotsAndCoordinatesOverlappingWakeUps() throws Exception {
		try (var application = startApplication("--engine.max-workers=3")) {
			EngineService service = application.getBean(EngineService.class);
			var entered = new LinkedBlockingQueue<Boolean>();
			var permits = new Semaphore(0);
			var active = new AtomicInteger();
			var peak = new AtomicInteger();
			PricingServiceTests.configurationRead = () -> {
				peak.accumulateAndGet(active.incrementAndGet(), Math::max);
				entered.add(true);
				try {
					assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(ex);
				} finally {
					active.decrementAndGet();
				}
			};
			for (int i = 0; i < 4; i++) {
				addAccount(UUID.randomUUID());
			}
			try {
				service.wakeUp();
				for (int i = 0; i < 3; i++) {
					assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
				}
				service.wakeUp();
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
				assertEquals(3, active.get());
				for (int i = 0; i < 12; i++) {
					addAccount(UUID.randomUUID());
				}
				try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
					for (int i = 0; i < 20; i++) {
						requests.submit(service::wakeUp);
					}
					permits.release(20);
				}
				await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertEquals(16,
						accounts.queryForObject("select count(*) from batch_outbox where processed",
								Integer.class)));
				assertEquals(3, peak.get());
				assertEquals(16, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
				assertEquals(16, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
			} finally {
				permits.release(30);
			}
		}
	}

	@Test
	void competingInstancesRespectConfiguredCapsAndKeepClaimsUntilCompletion() throws Exception {
		try (var primary = startApplication("--engine.max-workers=3");
				var other = startApplication("--engine.max-workers=2")) {
			EngineService service = primary.getBean(EngineService.class);
			var entered = new LinkedBlockingQueue<Boolean>();
			var permits = new Semaphore(0);
			PricingServiceTests.configurationRead = () -> {
				entered.add(true);
				try {
					assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(ex);
				}
			};
			UUID locked = UUID.randomUUID();
			UUID errored = UUID.randomUUID();
			UUID processed = UUID.randomUUID();
			addAccount(locked);
			addAccount(errored);
			addAccount(processed);
			accounts.update("update batch_outbox set error = '' where batch_id = ?", errored);
			accounts.update("update batch_outbox set processed = true where batch_id = ?", processed);
			for (int i = 0; i < 6; i++) {
				addAccount(UUID.randomUUID());
			}
			try (var connection = new DriverManagerDataSource(url("accountdb"), "sa", "").getConnection()) {
				connection.setAutoCommit(false);
				try (var statement = connection.prepareStatement(
						"select batch_id from batch_outbox where batch_id = ? for update")) {
					statement.setObject(1, locked);
					statement.executeQuery().close();
				}
				try {
					var second = other.getBean(EngineService.class);
					second.wakeUp();
					for (int i = 0; i < 2; i++) {
						assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
					}
					second.wakeUp();
					assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
					service.wakeUp();
					for (int i = 0; i < 3; i++) {
						assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
					}
					assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
					assertEquals(1, accounts.queryForList("""
							select batch_id from batch_outbox
							where not processed and error is null for update skip locked
							""", UUID.class).size());
				} finally {
					permits.release(20);
				}
				await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertEquals(6,
						recommendations.queryForObject(
								"select count(*) from recommendation_outbox", Integer.class)));
				assertEquals(6, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, locked));
				connection.rollback();
			}
			service.processPendingAccounts();
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, locked));
			assertFalse(accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, errored));
			assertEquals(7, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
		}
	}

	@Test
	void competingResultsCommitOnlyOneRecommendationBatch() throws Exception {
		try (var other = startApplication();
				var work = Executors.newVirtualThreadPerTaskExecutor()) {
			UUID batchId = UUID.randomUUID();
			var ready = new CountDownLatch(2);
			var start = new CountDownLatch(1);
			var attempts = new ArrayList<java.util.concurrent.Future<String>>();
			for (var repository : List.of(recommendationRepository,
					other.getBean(RecommendationRepository.class))) {
				String plan = attempts.isEmpty() ? "FIRST" : "SECOND";
				attempts.add(work.submit(() -> {
					ready.countDown();
					assertTrue(start.await(5, TimeUnit.SECONDS));
					try {
						repository.save(new RuleEngineBatchResult(batchId, List.of(
								new AccountResult("A1", AccountStatus.OK, plan, List.of(
										new FeeResult(42L, FeeStatus.OK, Decision.CHARGED,
												new BigDecimal("2.50"), null))))));
						return plan;
					} catch (DataIntegrityViolationException ex) {
						return null;
					}
				}));
			}
			assertTrue(ready.await(5, TimeUnit.SECONDS));
			start.countDown();
			String first = attempts.getFirst().get(5, TimeUnit.SECONDS);
			String second = attempts.getLast().get(5, TimeUnit.SECONDS);
			assertTrue((first == null) != (second == null));
			assertEquals(first == null ? second : first, recommendations.queryForObject(
					"select pricing_plan_code from account_recommendations", String.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from fee_request_recommendations", Integer.class));
		}
	}

	@Test
	void ignoresProcessedAndErroredBatches() {
		UUID eligible = UUID.randomUUID();
		UUID errored = UUID.randomUUID();
		UUID processed = UUID.randomUUID();
		addAccount(eligible);
		addAccount(errored);
		addAccount(processed);
		accounts.update("update batch_outbox set error = '' where batch_id = ?", errored);
		accounts.update("update batch_outbox set processed = true where batch_id = ?", processed);

		service.processPendingAccounts();

		assertEquals(List.of(eligible), recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class));
		assertFalse(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, errored));
	}

	@ParameterizedTest
	@ValueSource(strings = {"startup", "wake-up", "completion"})
	void persistsRuleEngineErrorsUntilManuallyCleared(String trigger) {
		UUID failed = UUID.randomUUID();
		addAccount(failed);
		String message = trigger.equals("completion") ? null : "Invalid \"rule\"\nvalue";
		var failure = new IllegalStateException(message, new IllegalArgumentException("Bad rule"));
		doAnswer(call -> {
			throw failure;
		}).when(ruleEngine).price(any(RuleEngineBatch.class));

		service.processPendingAccounts();

		String error = accounts.queryForObject(
				"select error from batch_outbox where batch_id = ?", String.class, failed);
		var details = JsonMapper.builder().build().readTree(error);
		assertEquals(3, details.size());
		assertEquals(IllegalStateException.class.getName(), details.get("type").asString());
		if (message == null) {
			assertTrue(details.get("message").isNull());
		} else {
			assertEquals(message, details.get("message").asString());
		}
		assertTrue(details.get("stackTrace").asString().contains(failure.getStackTrace()[0].toString()));
		assertTrue(details.get("stackTrace").asString()
				.contains("Caused by: java.lang.IllegalArgumentException: Bad rule"));
		assertFalse(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, failed));
		assertEquals(List.of(failed), accounts.queryForList("""
				select batch_id from batch_outbox for update skip locked
				""", UUID.class));
		assertEquals(0, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));

		try (var restarted = startApplication()) {
			assertEquals(error, accounts.queryForObject(
					"select error from batch_outbox where batch_id = ?", String.class, failed));
			assertEquals(0, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			if (trigger.equals("completion")) {
				addAccount(UUID.randomUUID());
				PricingServiceTests.configurationRead = () -> {
					accounts.update("update batch_outbox set error = NULL where batch_id = ?", failed);
					PricingServiceTests.configurationRead = () -> {};
				};
				restarted.getBean(EngineService.class).processPendingAccounts();
			} else if (trigger.equals("wake-up")) {
				accounts.update("update batch_outbox set error = NULL where batch_id = ?", failed);
				restarted.getBean(EngineService.class).wakeUp();
				await().atMost(Duration.ofSeconds(3)).until(() -> accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, failed));
			}
		}
		if (trigger.equals("startup")) {
			accounts.update("update batch_outbox set error = NULL where batch_id = ?", failed);
			try (var restarted = startApplication()) {
				assertTrue(restarted.isActive());
				await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
						accounts.queryForObject("select processed from batch_outbox where batch_id = ?",
								Boolean.class, failed)));
			}
		}
		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, failed));
		assertNull(accounts.queryForObject(
				"select error from batch_outbox where batch_id = ?", String.class, failed));
	}

	@ParameterizedTest
	@ValueSource(strings = {"valid", "reclaim", "parallel", "owner", "processed", "error",
			"recommendations", "completion"})
	void retainsRuleEngineErrorAfterAccountTransactionLoss(String outcome) throws Exception {
		boolean heldClaim = Set.of("valid", "parallel").contains(outcome);
		UUID batch = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(batch);
		var repository = application.getBean(AccountRepository.class);
		var retainedRepository = org.mockito.Mockito.mock(AccountRepository.class,
				org.mockito.AdditionalAnswers.delegatesTo(repository));
		var worker = new EngineService(
				retainedRepository, ruleEngine,
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200),
				outcome.equals("valid") ? Duration.ofSeconds(5) : Duration.ofMillis(100),
				outcome.equals("parallel") ? 2 : 1,
				application.getBean("accountTransactionManager", PlatformTransactionManager.class));
		var owner = new AtomicReference<Thread>();
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		var probes = new AtomicInteger();
		var completionFailures = new AtomicInteger();
		var evaluations = new AtomicInteger();
		var lastFailure = new AtomicReference<Long>();
		var firstProbe = new AtomicReference<Long>();
		var failure = new IllegalStateException("Invalid \"rule\"\nvalue",
				new IllegalArgumentException("Bad rule"));
		if (heldClaim) {
			doAnswer(call -> {
				if (unavailable.get()) {
					attempts.incrementAndGet();
					lastFailure.set(System.nanoTime());
					throw new DataAccessResourceFailureException("Error storage unavailable");
				}
				repository.recordError(batch, failure);
				return null;
			}).when(retainedRepository).recordError(batch, failure);
			doAnswer(call -> {
				if (unavailable.get()) {
					probes.incrementAndGet();
					firstProbe.compareAndSet(null, System.nanoTime());
					throw new DataAccessResourceFailureException("Account Store unavailable");
				}
				repository.checkConnection();
				return null;
			}).when(retainedRepository).checkConnection();
		}
		doAnswer(call -> {
			if (!((RuleEngineBatch) call.getArgument(0)).batchId().equals(batch)) {
				return call.callRealMethod();
			}
			owner.set(Thread.currentThread());
			evaluations.incrementAndGet();
			throw failure;
		}).when(ruleEngine).price(any(RuleEngineBatch.class));
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[] {Connection.class}, (proxy, method, arguments) -> {
					String sql = method.getName().equals("prepareStatement")
							? (String) arguments[0] : "";
					if (!heldClaim && Thread.currentThread() == owner.get()
							&& unavailable.get()
							&& sql.equals("select 1")) {
						probes.incrementAndGet();
						throw new SQLTransientConnectionException("Account Store outage", "08001");
					}
					if (Thread.currentThread() == owner.get() && !unavailable.get()
							&& outcome.equals("completion") && method.getName().equals("commit")
							&& completionFailures.getAndIncrement() < 3) {
						connection.rollback();
						throw new SQLTransientConnectionException("Completion outage", "08001");
					}
					if (!heldClaim && Thread.currentThread() == owner.get()
							&& unavailable.get()
							&& method.getName().equals("commit")) {
						connection.rollback();
						attempts.incrementAndGet();
						throw new SQLTransientConnectionException("Lost error transaction", "08001");
					}
					try {
						return method.invoke(connection, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				});
		}).when(accountDataSource).getConnection();
		var available = new DriverManagerDataSource(url("accountdb"), "sa", "");
		try (var lock = available.getConnection();
				var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				await().atMost(Duration.ofSeconds(7)).until(() -> probes.get() > 0);
				assertFalse(processing.isDone());
				assertEquals(1, evaluations.get());
				assertEquals(3, attempts.get());
				if (outcome.equals("valid")) {
					assertTrue(firstProbe.get() - lastFailure.get()
							>= Duration.ofSeconds(5).toNanos());
					lock.setAutoCommit(false);
					try (var claim = lock.prepareStatement(
							"select batch_id from batch_outbox where batch_id = ? for update skip locked")) {
						claim.setObject(1, batch);
						try (var rows = claim.executeQuery()) {
							assertFalse(rows.next());
						}
					}
					lock.rollback();
					lock.setAutoCommit(true);
				}
				addAccount(next);
				worker.wakeUp();
				if (outcome.equals("parallel")) {
					await().atMost(Duration.ofSeconds(3)).until(() -> accounts.queryForObject(
							"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
					assertFalse(processing.isDone());
				} else {
					assertEquals(0, recommendations.queryForObject(
							"select count(*) from recommendation_outbox", Integer.class));
				}
				if (outcome.equals("owner")) {
					lock.setAutoCommit(false);
					try (var claim = lock.prepareStatement(
							"select batch_id from batch_outbox where batch_id = ? for update")) {
						claim.setObject(1, batch);
						try (var rows = claim.executeQuery()) {
							assertTrue(rows.next());
						}
					}
				} else if (outcome.equals("processed")) {
					accounts.update("update batch_outbox set processed = true where batch_id = ?", batch);
				} else if (outcome.equals("error")) {
					accounts.update("update batch_outbox set error = '' where batch_id = ?", batch);
				} else if (outcome.equals("recommendations") || outcome.equals("completion")) {
					recommendationRepository.save(new RuleEngineBatchResult(batch, List.of()));
				}
			} finally {
				unavailable.set(false);
				processing.get(8, TimeUnit.SECONDS);
			}
			assertEquals(1, evaluations.get());
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
			String error = accounts.queryForObject(
					"select error from batch_outbox where batch_id = ?", String.class, batch);
			if (Set.of("reclaim", "valid", "parallel").contains(outcome)) {
				var details = JsonMapper.builder().build().readTree(error);
				assertEquals(failure.getClass().getName(), details.get("type").asString());
				assertEquals(failure.getMessage(), details.get("message").asString());
				assertTrue(details.get("stackTrace").asString().contains("Bad rule"));
			} else if (outcome.equals("error")) {
				assertEquals("", error);
			} else {
				assertNull(error);
			}
			boolean completed = Set.of("processed", "recommendations", "completion").contains(outcome);
			assertEquals(completed, accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, batch));
			assertEquals(Set.of("recommendations", "completion").contains(outcome) ? 2 : 1,
					recommendations.queryForObject(
							"select count(*) from recommendation_outbox", Integer.class));
			if (outcome.equals("completion")) {
				assertTrue(completionFailures.get() >= 4);
			}
			if (outcome.equals("owner")) {
				lock.rollback();
				accounts.update("update batch_outbox set processed = true where batch_id = ?", batch);
			}
		}
	}

	@Test
	void ruleEngineFailureReleasesCapacityWhileAnotherWorkerKeepsItsClaim() throws Exception {
		var worker = new EngineService(
				application.getBean(AccountRepository.class), ruleEngine,
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), Duration.ofMillis(100), 2,
				application.getBean("accountTransactionManager",
						PlatformTransactionManager.class));
		UUID held = UUID.randomUUID();
		UUID failed = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(held);
		var heldEntered = new CountDownLatch(1);
		var failedEntered = new CountDownLatch(1);
		var releaseHeld = new CountDownLatch(1);
		var releaseFailure = new CountDownLatch(1);
		doAnswer(call -> {
			UUID batchId = call.getArgument(0, RuleEngineBatch.class).batchId();
			if (batchId.equals(held)) {
				heldEntered.countDown();
				assertTrue(releaseHeld.await(5, TimeUnit.SECONDS));
			} else if (batchId.equals(failed)) {
				failedEntered.countDown();
				assertTrue(releaseFailure.await(5, TimeUnit.SECONDS));
				throw new IllegalArgumentException("Invalid rule");
			}
			return call.callRealMethod();
		}).when(ruleEngine).price(any(RuleEngineBatch.class));

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				assertTrue(heldEntered.await(3, TimeUnit.SECONDS));
				addAccount(failed);
				worker.wakeUp();
				assertTrue(failedEntered.await(3, TimeUnit.SECONDS));
				addAccount(next);
				releaseFailure.countDown();
				await().atMost(Duration.ofSeconds(3)).until(() -> accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
				assertEquals(List.of(failed), accounts.queryForList("""
						select batch_id from batch_outbox where error is not null
						and processed = false for update skip locked
						""", UUID.class));
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, held));
				assertTrue(accounts.queryForList("""
						select batch_id from batch_outbox where batch_id = ? for update skip locked
						""", UUID.class, held).isEmpty());
				assertFalse(processing.isDone());
			} finally {
				releaseFailure.countDown();
				releaseHeld.countDown();
				processing.get(5, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void retriesConfigurationAccessBeforeCalculating() {
		addAccount(UUID.randomUUID());
		AtomicInteger reads = new AtomicInteger();
		PricingServiceTests.configurationRead = () -> {
			if (reads.getAndIncrement() == 0) {
				throw new DataAccessResourceFailureException("Temporary configuration outage");
			}
		};

		service.processPendingAccounts();

		assertEquals(2, reads.get());
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertNull(accounts.queryForObject("select error from batch_outbox", String.class));
		assertEquals(0, recommendations.queryForObject(
				"select amount from fee_request_recommendations", BigDecimal.class)
						.compareTo(new BigDecimal("2.50")));
	}

	@Test
	void recoveryStoresRetainedResultsBeforeFreshAndSubsequentScans() throws Exception {
		UUID retained = UUID.randomUUID();
		UUID missed = UUID.randomUUID();
		UUID subsequent = UUID.randomUUID();
		addAccount(retained);
		var availableRecommendations = new JdbcTemplate(new DriverManagerDataSource(
				url("recommendationdb"), "sa", ""));
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		doAnswer(invocation -> {
			if (!PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty()) {
				attempts.incrementAndGet();
				if (unavailable.get()) {
					throw new SQLTransientConnectionException("Recommendation Store outage", "08001");
				}
			}
			return invocation.callRealMethod();
		}).when(recommendationDataSource).getConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertFalse(processing.isDone()));
				assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
				assertEquals(0, availableRecommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
				assertFalse(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
				PricingServiceTests.januaryAmount = "9.99";
				addAccount(missed);
				var addSubsequent = new AtomicBoolean(true);
				PricingServiceTests.configurationRead = () -> {
					if (addSubsequent.getAndSet(false)) {
						assertEquals(0, recommendations.queryForObject(
								"select amount from fee_request_recommendations", BigDecimal.class)
										.compareTo(new BigDecimal("2.50")));
						assertTrue(accounts.queryForObject(
								"select processed from batch_outbox where batch_id = ?",
								Boolean.class, retained));
						addAccount(subsequent);
					}
				};
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
			} finally {
				unavailable.set(false);
				try {
					processing.get(5, TimeUnit.SECONDS);
				} finally {
					PricingServiceTests.configurationRead = () -> {};
				}
			}
		}
		assertEquals(Set.of(retained, missed, subsequent), Set.copyOf(recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class)));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
		assertEquals(List.of(0L, 1L, 2L), PricingServiceTests.COMPLETED_BEFORE_EVALUATION);
	}

	@Test
	void storageRecoveryUsesFiveSecondChecksAndKeepsItsResultClaimAndSlot() throws Exception {
		var defaults = PropertiesLoaderUtils.loadProperties(
				new ClassPathResource("application.properties"));
		var interval = DurationStyle.detectAndParse(application.getEnvironment().resolvePlaceholders(
				defaults.getProperty("database.recovery-check-interval")));
		assertEquals(Duration.ofSeconds(5), interval);
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), interval, 1,
				application.getBean("accountTransactionManager",
						PlatformTransactionManager.class));
		UUID retained = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(retained);
		var calculated = new AtomicBoolean();
		var unavailable = new AtomicBoolean(true);
		var attempts = new LinkedBlockingQueue<Long>();
		var queries = new CopyOnWriteArrayList<String>();
		PricingServiceTests.configurationRead = () -> calculated.set(true);
		doAnswer(invocation -> {
			if (calculated.get()) {
				attempts.add(System.nanoTime());
				if (unavailable.get()) {
					throw new SQLTransientConnectionException("Recommendation Store outage", "08001");
				}
			}
			Connection connection = (Connection) invocation.callRealMethod();
			return Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[] {Connection.class}, (proxy, method, arguments) -> {
					if (calculated.get() && method.getName().equals("prepareStatement")) {
						queries.add((String) arguments[0]);
					}
					try {
						return method.invoke(connection, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				});
		}).when(recommendationDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				Long first = attempts.poll(3, TimeUnit.SECONDS);
				Long second = attempts.poll(3, TimeUnit.SECONDS);
				Long third = attempts.poll(3, TimeUnit.SECONDS);
				assertTrue(first != null && second != null && third != null);
				assertTrue(second - first >= Duration.ofMillis(200).toNanos());
				assertTrue(third - second >= Duration.ofMillis(200).toNanos());
				Long probe = attempts.poll(7, TimeUnit.SECONDS);
				assertTrue(probe != null);
				assertTrue(probe - third >= interval.toNanos());
				assertFalse(processing.isDone());
				assertTrue(queries.isEmpty());
				assertTrue(accounts.queryForList("""
						select batch_id from batch_outbox where batch_id = ?
						for update skip locked
						""", UUID.class, retained).isEmpty());
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, retained));
				PricingServiceTests.januaryAmount = "9.99";
				addAccount(next);
				worker.wakeUp();
				await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertEquals(1,
								PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size()));
				unavailable.set(false);
				Long restored = attempts.poll(7, TimeUnit.SECONDS);
				assertTrue(restored != null);
				assertTrue(restored - probe >= interval.toNanos());
				processing.get(5, TimeUnit.SECONDS);
			} finally {
				unavailable.set(false);
				processing.get(8, TimeUnit.SECONDS);
			}
		}
		assertEquals("select 1", queries.getFirst());
		assertTrue(queries.get(1).startsWith("insert into recommendation_outbox"));
		assertEquals(2, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
	}

	@Test
	void recoveringStorageDoesNotPauseOtherWorkersOrReleaseItsSlot() throws Exception {
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), Duration.ofMillis(100), 2,
				application.getBean("accountTransactionManager",
						PlatformTransactionManager.class));
		UUID retained = UUID.randomUUID();
		addAccount(retained);
		var owner = new AtomicReference<Thread>();
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		var entered = new LinkedBlockingQueue<Boolean>();
		var permits = new Semaphore(0);
		PricingServiceTests.configurationRead = () -> {
			if (!owner.compareAndSet(null, Thread.currentThread())) {
				entered.add(true);
				try {
					assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(ex);
				}
			}
		};
		doAnswer(invocation -> {
			if (Thread.currentThread() == owner.get() && unavailable.get()) {
				attempts.incrementAndGet();
				throw new SQLTransientConnectionException("Worker-local storage outage", "08001");
			}
			return invocation.callRealMethod();
		}).when(recommendationDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 4);
				PricingServiceTests.januaryAmount = "9.99";
				addAccount(UUID.randomUUID());
				addAccount(UUID.randomUUID());
				worker.wakeUp();
				assertEquals(Boolean.TRUE, entered.poll(3, TimeUnit.SECONDS));
				worker.wakeUp();
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				assertEquals(Boolean.TRUE, entered.poll(3, TimeUnit.SECONDS));
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertEquals(2,
						accounts.queryForObject(
								"select count(*) from batch_outbox where processed", Integer.class)));
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, retained));
				assertTrue(accounts.queryForList("""
						select batch_id from batch_outbox where batch_id = ?
						for update skip locked
						""", UUID.class, retained).isEmpty());
				assertFalse(processing.isDone());
			} finally {
				permits.release(3);
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertEquals(3, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(3, accounts.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
	}

	@Test
	void accountStoreOutageIgnoresWakeUpsAndRecoversWithoutAnotherRequest() throws Exception {
		UUID first = UUID.randomUUID();
		UUID second = UUID.randomUUID();
		UUID missed = UUID.randomUUID();
		addAccount(first);
		addAccount(second);
		var availableAccounts = new JdbcTemplate(new DriverManagerDataSource(
				url("accountdb"), "sa", ""));
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		doAnswer(invocation -> {
			attempts.incrementAndGet();
			if (unavailable.get()) {
				throw new SQLTransientConnectionException("Account Store outage", "08001");
			}
			return invocation.callRealMethod();
		}).when(accountDataSource).getConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertFalse(processing.isDone()));
				availableAccounts.update(
						"insert into batch_outbox(batch_id, processed) values (?, false)", missed);
				availableAccounts.update("""
						insert into accounts(batch_id, account_number, product_code,
						branch_code, pricing_date)
						values (?, 'A1', 'CHECKING', 'NY', '2026-01-15')
						""", missed);
				availableAccounts.update("""
						insert into fee_requests(account_id, fee_request_id, code)
						select id, 42, 'SERVICE' from accounts where batch_id = ?
						""", missed);
				for (int i = 0; i < 3; i++) {
					mockMvc.perform(post("/api/v1/work-available"))
							.andExpect(status().isAccepted()).andExpect(content().string(""));
				}
				assertTrue(PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty());
				assertEquals(3, availableAccounts.queryForObject(
						"select count(*) from batch_outbox where processed = false", Integer.class));
				assertEquals(0, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
			} finally {
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertEquals(Set.of(first, second, missed), Set.copyOf(recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class)));
		assertEquals(List.of(0L, 1L, 2L), PricingServiceTests.COMPLETED_BEFORE_EVALUATION);
		assertEquals(Set.of("batch_id", "processed", "error"),
				Set.copyOf(availableAccounts.queryForList("""
				select column_name from information_schema.columns
				where table_schema = 'public' and table_name = 'batch_outbox'
						""", String.class)));
		int idleConnections = attempts.get();
		await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
				.untilAsserted(() -> assertEquals(idleConnections, attempts.get()));
	}

	@Test
	void wakeUpsDoNotStartAnotherBatchDuringTheCurrentRetryDelay() throws Exception {
		addAccount(UUID.randomUUID());
		AtomicInteger connections = new AtomicInteger();
		var retrying = new CompletableFuture<Long>();
		var failedAt = new AtomicReference<Long>();
		var retryGate = new CountDownLatch(1);
		doAnswer(invocation -> {
			int attempt = connections.getAndIncrement();
			if (attempt == 0) {
				failedAt.set(System.nanoTime());
				throw new SQLTransientConnectionException("Temporary outage", "08001");
			}
			if (attempt == 1) {
				retrying.complete(System.nanoTime() - failedAt.get());
				assertTrue(retryGate.await(5, TimeUnit.SECONDS));
			}
			return invocation.callRealMethod();
		}).when(accountDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var run = work.submit(service::processPendingAccounts);
			try {
				assertTrue(retrying.get(5, TimeUnit.SECONDS) >= Duration.ofMillis(200).toNanos());
				addAccount(UUID.randomUUID());
				service.wakeUp();
				assertTrue(PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty());
			} finally {
				retryGate.countDown();
			}
			run.get(5, TimeUnit.SECONDS);
		}
		assertEquals(List.of(0L, 1L), PricingServiceTests.COMPLETED_BEFORE_EVALUATION);
	}

	@Test
	void readinessDoesNotWaitForPendingAccountProcessing() throws Exception {
		int port;
		try (var available = new ServerSocket(0)) {
			port = available.getLocalPort();
		}
		addAccount(UUID.randomUUID());
		var initializing = new CompletableFuture<Void>();
		var finishInitialization = new CompletableFuture<Void>();
		PricingServiceTests.configurationRead = () -> {
			initializing.complete(null);
			finishInitialization.join();
		};
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var application = work.submit(() -> startApplication("--server.port=" + port));
			try {
				initializing.get(10, TimeUnit.SECONDS);
				application.get(10, TimeUnit.SECONDS);
				assertEquals(200, RestClient.create().get()
						.uri("http://127.0.0.1:" + port + "/openapi.yaml")
						.retrieve().toBodilessEntity().getStatusCode().value());
				assertFalse(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
			} finally {
				finishInitialization.complete(null);
				try (var started = application.get(10, TimeUnit.SECONDS)) {
					await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
							accounts.queryForObject("select processed from batch_outbox", Boolean.class)));
					assertAttempts(1);
				}
			}
		}
	}

	@Test
	void readinessDoesNotWaitForUnavailableDownstreamServices() throws Exception {
		String recommendationUrl = url(UUID.randomUUID().toString());
		var dataSource = new DriverManagerDataSource(recommendationUrl, "sa", "");
		Flyway.configure().dataSource(dataSource).locations("classpath:db/recommendation")
				.load().migrate();
		var pendingRecommendations = new JdbcTemplate(dataSource);
		pendingRecommendations.update("insert into recommendation_outbox(batch_id) values (?)",
				UUID.randomUUID());
		responses = new int[] {503};
		responseGate = new CountDownLatch(1);
		try (var application = startApplication("--transaction-processor.http-timeout=5s",
				"--recommendation-store.datasource.url=" + recommendationUrl,
				"--account-store.datasource.url=" + url(UUID.randomUUID().toString()))) {
			await().atMost(Duration.ofSeconds(5)).until(() -> ACTIVE_REQUESTS.get() > 0);
			String port = application.getEnvironment().getProperty("local.server.port");
			assertEquals(200, RestClient.create().get()
					.uri("http://127.0.0.1:" + port + "/openapi.yaml")
					.retrieve().toBodilessEntity().getStatusCode().value());
			assertFalse(pendingRecommendations.queryForObject(
					"select processed from recommendation_outbox", Boolean.class));
			responseGate.countDown();
			assertAttempts(3);
		} finally {
			responseGate.countDown();
		}
	}

	@Test
	void startupWaitsForRecommendationStoreRecoveryBeforeDrainingAccounts() throws Exception {
		addAccount(UUID.randomUUID());
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		doAnswer(invocation -> {
			attempts.incrementAndGet();
			if (unavailable.get()) {
				throw new SQLTransientConnectionException("Startup Recommendation Store outage", "08001");
			}
			return invocation.callRealMethod();
		}).when(recommendationDataSource).getConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var startup = work.submit(service::afterSingletonsInstantiated);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertTrue(startup.isDone()));
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
				assertTrue(PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty());
				assertFalse(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
			} finally {
				unavailable.set(false);
				startup.get(5, TimeUnit.SECONDS);
			}
		}
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
				accounts.queryForObject("select processed from batch_outbox", Boolean.class)));
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
	}

	@Test
	void accountAndFeeRequestValidationOutcomesDoNotRetry() {
		UUID invalidAccount = UUID.randomUUID();
		addAccount(invalidAccount);
		accounts.update("update accounts set product_code = 'UNKNOWN' where batch_id = ?",
				invalidAccount);
		UUID invalidFee = UUID.randomUUID();
		long accountId = addAccount(invalidFee);
		accounts.update("update fee_requests set code = 'UNKNOWN' where account_id = ?", accountId);

		service.processPendingAccounts();

		assertEquals(2, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from account_recommendations where status = 'PLAN_NOT_FOUND'",
				Integer.class));
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from fee_request_recommendations where status = 'FEE_NOT_FOUND'",
				Integer.class));
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed = true", Integer.class));
	}

	@Test
	void configurationOutageChecksConnectivityBeforeRediscoveringBatches() throws Exception {
		addAccount(UUID.randomUUID());
		var unavailable = new AtomicBoolean(true);
		var failedReads = new AtomicInteger();
		var checks = new AtomicInteger();
		PricingServiceTests.configurationRead = () -> {
			if (unavailable.get()) {
				failedReads.incrementAndGet();
				throw new DataAccessResourceFailureException("Pricing Configuration outage");
			}
		};
		doAnswer(invocation -> {
			checks.incrementAndGet();
			if (unavailable.get()) {
				throw new DataAccessResourceFailureException("Pricing Configuration outage");
			}
			return null;
		}).when(pricingRepository).checkConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> checks.get() >= 2);
				int exhaustedReads = failedReads.get();
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertEquals(exhaustedReads, failedReads.get()));
				addAccount(UUID.randomUUID());
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
				assertEquals(2, accounts.queryForObject(
						"select count(*) from batch_outbox where processed = false", Integer.class));
				assertEquals(0, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
			} finally {
				unavailable.set(false);
				try {
					processing.get(5, TimeUnit.SECONDS);
				} finally {
					doNothing().when(pricingRepository).checkConnection();
					PricingServiceTests.configurationRead = () -> {};
				}
			}
		}
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed = true", Integer.class));
		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
	}

	@Test
	void recoveryFinishesKnownSourceCompletionWithoutRecalculating() throws Exception {
		addAccount(UUID.randomUUID());
		var availableAccounts = new JdbcTemplate(new DriverManagerDataSource(
				url("accountdb"), "sa", ""));
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		doAnswer(invocation -> {
			if (unavailable.get() && recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class) > 0) {
				attempts.incrementAndGet();
				throw new SQLTransientConnectionException("Source completion outage", "08001");
			}
			Connection connection = (Connection) invocation.callRealMethod();
			return Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[] {Connection.class}, (proxy, method, arguments) -> {
					if (method.getName().equals("prepareStatement") && unavailable.get()
							&& recommendations.queryForObject(
									"select count(*) from recommendation_outbox", Integer.class) > 0) {
						attempts.incrementAndGet();
						throw new SQLTransientConnectionException("Source completion outage", "08001");
					}
					try {
						return method.invoke(connection, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				});
		}).when(accountDataSource).getConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				assertAttempts(1);
				assertFalse(availableAccounts.queryForObject(
						"select processed from batch_outbox", Boolean.class));
				PricingServiceTests.januaryAmount = "9.99";
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
			} finally {
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertTrue(availableAccounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
		assertEquals(0, recommendations.queryForObject(
				"select amount from fee_request_recommendations", BigDecimal.class)
						.compareTo(new BigDecimal("2.50")));
	}

	@ParameterizedTest
	@ValueSource(strings = {"prepareStatement", "commit"})
	void completionRecoveryRetainsItsSlotAfterClaimTransactionLoss(String failedOperation)
			throws Exception {
		var defaults = PropertiesLoaderUtils.loadProperties(
				new ClassPathResource("application.properties"));
		var interval = DurationStyle.detectAndParse(application.getEnvironment().resolvePlaceholders(
				defaults.getProperty("database.recovery-check-interval")));
		assertEquals(Duration.ofSeconds(5), interval);
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), interval, 1,
				application.getBean("accountTransactionManager", PlatformTransactionManager.class));
		UUID retained = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(retained);
		var availableAccounts = new JdbcTemplate(new DriverManagerDataSource(
				url("accountdb"), "sa", ""));
		var owner = new AtomicReference<Thread>();
		var unavailable = new AtomicBoolean(true);
		var lost = new AtomicBoolean();
		var failures = new LinkedBlockingQueue<Long>();
		var probes = new LinkedBlockingQueue<Long>();
		PricingServiceTests.configurationRead = () -> owner.compareAndSet(null, Thread.currentThread());
		doAnswer(invocation -> {
			assertFalse(lost.get() && Thread.currentThread() == owner.get(),
					"Completion recovery must not access recommendations again");
			return invocation.callRealMethod();
		}).when(recommendationDataSource).getConnection();
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[] {Connection.class}, (proxy, method, arguments) -> {
					if (Thread.currentThread() == owner.get()) {
						String sql = method.getName().equals("prepareStatement")
								? (String) arguments[0] : "";
						assertFalse(lost.get() && sql.contains("skip locked"),
								"Completion recovery must not reclaim the batch");
						if (unavailable.get() && sql.equals("select 1")) {
							probes.add(System.nanoTime());
							throw new SQLTransientConnectionException("Account Store outage", "08001");
						}
						if (unavailable.get() && method.getName().equals(failedOperation)) {
							connection.rollback();
							lost.set(true);
							failures.add(System.nanoTime());
							throw new SQLTransientConnectionException("Lost Account transaction", "08001");
						}
					}
					try {
						return method.invoke(connection, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				});
		}).when(accountDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				Long first = failures.poll(3, TimeUnit.SECONDS);
				Long second = failures.poll(3, TimeUnit.SECONDS);
				Long third = failures.poll(3, TimeUnit.SECONDS);
				assertTrue(first != null && second != null && third != null);
				assertTrue(second - first >= Duration.ofMillis(200).toNanos());
				assertTrue(third - second >= Duration.ofMillis(200).toNanos());
				Long probe = probes.poll(7, TimeUnit.SECONDS);
				assertTrue(probe != null);
				assertTrue(probe - third >= interval.toNanos());
				assertTrue(failures.isEmpty());
				assertFalse(processing.isDone());
				assertFalse(availableAccounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, retained));
				assertEquals(List.of(retained), availableAccounts.queryForList(
						"select batch_id from batch_outbox for update skip locked", UUID.class));
				var committed = storedRecommendations(recommendations);
				PricingServiceTests.januaryAmount = "9.99";
				addAccount(next);
				worker.wakeUp();
				assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
				assertEquals(committed, storedRecommendations(recommendations));
			} finally {
				unavailable.set(false);
				processing.get(12, TimeUnit.SECONDS);
			}
		}
		assertEquals(2, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(2, availableAccounts.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
	}

	@Test
	void completionRecoveryDoesNotPauseOtherWorkersOrReleaseItsSlot() throws Exception {
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), Duration.ofMillis(100), 2,
				application.getBean("accountTransactionManager", PlatformTransactionManager.class));
		UUID retained = UUID.randomUUID();
		addAccount(retained);
		var owner = new AtomicReference<Thread>();
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		var entered = new LinkedBlockingQueue<Boolean>();
		var permits = new Semaphore(0);
		PricingServiceTests.configurationRead = () -> {
			if (!owner.compareAndSet(null, Thread.currentThread())) {
				entered.add(true);
				try {
					assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(ex);
				}
			}
		};
		doAnswer(invocation -> {
			if (Thread.currentThread() == owner.get() && unavailable.get()) {
				attempts.incrementAndGet();
				throw new SQLTransientConnectionException("Worker-local Account outage", "08001");
			}
			Connection connection = (Connection) invocation.callRealMethod();
			return Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[] {Connection.class}, (proxy, method, arguments) -> {
					if (Thread.currentThread() == owner.get() && unavailable.get()
							&& method.getName().equals("commit")) {
						connection.rollback();
						attempts.incrementAndGet();
						throw new SQLTransientConnectionException("Lost Account transaction", "08001");
					}
					try {
						return method.invoke(connection, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				});
		}).when(accountDataSource).getConnection();
		var availableAccounts = new DriverManagerDataSource(url("accountdb"), "sa", "");
		try (var lock = availableAccounts.getConnection();
				var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 4);
				// Another instance may own the row after the original Account transaction is lost.
				lock.setAutoCommit(false);
				try (var claim = lock.prepareStatement(
						"select batch_id from batch_outbox where batch_id = ? for update")) {
					claim.setObject(1, retained);
					try (var rows = claim.executeQuery()) {
						assertTrue(rows.next());
					}
				}
				PricingServiceTests.januaryAmount = "9.99";
				addAccount(UUID.randomUUID());
				addAccount(UUID.randomUUID());
				worker.wakeUp();
				assertEquals(Boolean.TRUE, entered.poll(3, TimeUnit.SECONDS));
				worker.wakeUp();
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				assertEquals(Boolean.TRUE, entered.poll(3, TimeUnit.SECONDS));
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertEquals(2,
						accounts.queryForObject(
								"select count(*) from batch_outbox where processed", Integer.class)));
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, retained));
				assertFalse(processing.isDone());
			} finally {
				permits.release(3);
				lock.rollback();
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertEquals(3, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(3, accounts.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
	}

	@Test
	void retriesUntilTheConsumerAcknowledgesWithoutChangingRecommendations() {
		responses = new int[] {503, 204};
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		var committed = storedRecommendations(recommendations);

		assertAttempts(2);
		assertCompletedBatchUnchanged(committed);
	}

	@Test
	void stopsAfterTheAttemptLimitWithoutChangingRecommendations() {
		responses = new int[] {503};
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		var committed = storedRecommendations(recommendations);

		assertAttempts(3);
		assertCompletedBatchUnchanged(committed);
	}

	@Test
	void countsTimedOutRequestsWithoutChangingRecommendations() {
		responseGate = new CountDownLatch(1);
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		var committed = storedRecommendations(recommendations);

		try {
			assertAttempts(3);
			assertCompletedBatchUnchanged(committed);
		} finally {
			responseGate.countDown();
		}
	}

	@Test
	void restartRetriesAfterDeliveryExhaustionWithoutRecalculation() {
		responses = new int[] {503};
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		assertAttempts(3);
		var committed = storedRecommendations(recommendations);
		responses = new int[] {204};
		PricingServiceTests.configurationRead = () -> {
			throw new AssertionError("HTTP failure must not cause recalculation");
		};

		try (var application = startApplication()) {
			assertAttempts(4);
			assertCompletedBatchUnchanged(committed);
		}
	}

	@Test
	void startupDoesNotNotifyAnEmptyStore() {
		try (var application = startApplication()) {
			assertAttempts(0);
		}
	}

	@Test
	void startupDoesNotNotifyProcessedRecommendations() {
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		assertAttempts(1);
		recommendations.update("update recommendation_outbox set processed = true");
		var committed = storedRecommendations(recommendations);

		try (var application = startApplication()) {
			assertAttempts(1);
			assertEquals(committed, storedRecommendations(recommendations));
			assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		}
	}

	@Test
	void startupWakesExistingRecommendationsBeforeDrainingAccounts() {
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		assertAttempts(1);
		addAccount(UUID.randomUUID());
		PricingServiceTests.configurationRead = () -> await().atMost(Duration.ofSeconds(5))
				.until(() -> WAKE_UPS.size() >= 2);

		try (var application = startApplication()) {
			assertAttempts(3);
			assertEquals(1, WAKE_UPS.get(1).rows().getFirst().size());
			assertEquals(2, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertEquals(2, accounts.queryForObject(
					"select count(*) from batch_outbox where processed = true", Integer.class));
		}
	}

	@Test
	void recoveryUsesTheConfiguredAttemptLimitDelayAndTimeout() {
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		assertAttempts(1);
		var committed = storedRecommendations(recommendations);
		responseDelayMillis = 750;

		try (var application = startApplication("--transaction-processor.max-attempts=2",
				"--transaction-processor.retry-delay=150ms",
				"--transaction-processor.http-timeout=100ms")) {
			assertAttempts(3);
			long interval = WAKE_UPS.get(2).receivedAt() - WAKE_UPS.get(1).receivedAt();
			assertTrue(interval >= Duration.ofMillis(150).toNanos());
			assertTrue(interval < Duration.ofMillis(750).toNanos());
			assertCompletedBatchUnchanged(committed);
		}
	}

	@Test
	void restartWakesUnprocessedRecommendationsWithoutRecalculation() {
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		assertAttempts(1);
		List<List<Map<String, Object>>> committed = storedRecommendations(recommendations);
		PricingServiceTests.configurationRead = () -> {
			throw new AssertionError("Completed batches must not be recalculated");
		};

		try (var application = startApplication()) {
			assertAttempts(2);
			assertEquals(committed, storedRecommendations(recommendations));
			assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
			WAKE_UPS.forEach(wakeUp -> assertEquals(committed, wakeUp.rows()));
		}
	}

	@Test
	void simultaneousWakeUpsDrainNewBatchesWithoutOverlappingEvaluation() throws Exception {
		UUID first = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(first);
		var evaluating = new CompletableFuture<Void>();
		var resume = new CompletableFuture<Void>();
		PricingServiceTests.configurationRead = () -> {
			evaluating.complete(null);
			resume.join();
		};

		try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
			try {
				mockMvc.perform(post("/api/v1/work-available"))
						.andExpect(status().isAccepted()).andExpect(content().string(""));
				evaluating.get(5, TimeUnit.SECONDS);
				addAccount(next);
				var wakeUps = new ArrayList<Future<?>>();
				for (int i = 0; i < 8; i++) {
					wakeUps.add(requests.submit(() -> {
						mockMvc.perform(post("/api/v1/work-available"))
								.andExpect(status().isAccepted());
						return null;
					}));
				}
				for (var wakeUp : wakeUps) {
					wakeUp.get(5, TimeUnit.SECONDS);
				}
				service.processPendingAccounts();
			} finally {
				resume.complete(null);
			}
		}

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(2,
				accounts.queryForObject("select count(*) from batch_outbox where processed = true",
						Integer.class)));
		assertEquals(Set.of(first, next), Set.copyOf(recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class)));
		assertEquals(List.of(0L, 1L), PricingServiceTests.COMPLETED_BEFORE_EVALUATION);
	}

	@Test
	void idleEngineWaitsForAWakeUpInsteadOfPolling() throws Exception {
		service.processPendingAccounts();
		UUID batchId = UUID.randomUUID();
		addAccount(batchId);

		await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
			assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
			assertEquals(0, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
		});
		mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
				accounts.queryForObject("select processed from batch_outbox where batch_id = ?",
						Boolean.class, batchId)));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
	}

	@Test
	void emptyDiscoveryKeepsItsSlotUntilTheAccountTransactionFinishes() throws Exception {
		var committing = new CompletableFuture<Void>();
		var finishCommit = new CountDownLatch(1);
		var firstCommit = new AtomicBoolean(true);
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[] {Connection.class}, (proxy, method, arguments) -> {
					if (method.getName().equals("commit") && firstCommit.getAndSet(false)) {
						committing.complete(null);
						assertTrue(finishCommit.await(5, TimeUnit.SECONDS));
					}
					try {
						return method.invoke(connection, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				});
		}).when(accountDataSource).getConnection();
		UUID batchId = UUID.randomUUID();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				committing.get(5, TimeUnit.SECONDS);
				await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
						.untilAsserted(() -> assertFalse(processing.isDone()));
				addAccount(batchId);
				service.wakeUp();
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox", Boolean.class));
			} finally {
				finishCommit.countDown();
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
	}

	@Test
	void wakeUpOverlappingAnEmptyScanDoesNotStrandCommittedWork() throws Exception {
		var scannedEmpty = new CompletableFuture<Void>();
		var finishScan = new CompletableFuture<Void>();
		EMPTY_SCAN.set(() -> {
			scannedEmpty.complete(null);
			finishScan.join();
		});
		doAnswer(invocation -> delayEmptyScan(invocation.callRealMethod()))
				.when(accountDataSource).getConnection();
		UUID batchId = UUID.randomUUID();

		try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
			try {
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
				scannedEmpty.get(5, TimeUnit.SECONDS);
				addAccount(batchId);
				var received = new CompletableFuture<Void>();
				var wakeUp = requests.submit(() -> {
					mockMvc.perform(post("/api/v1/work-available").with(request -> {
						received.complete(null);
						return request;
					})).andExpect(status().isAccepted());
					return null;
				});
				received.get(5, TimeUnit.SECONDS);
				wakeUp.get(5, TimeUnit.SECONDS);
				finishScan.complete(null);
			} finally {
				finishScan.complete(null);
			}
		}

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
				accounts.queryForObject("select processed from batch_outbox where batch_id = ?",
						Boolean.class, batchId)));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
	}

	static Object delayEmptyScan(Object target) {
		// Delay the JDBC result after H2 has found no rows, before discovery returns.
		Class<?> type = target instanceof Connection ? Connection.class
				: target instanceof PreparedStatement ? PreparedStatement.class : ResultSet.class;
		return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
				(proxy, method, arguments) -> {
					Object result;
					try {
						result = method.invoke(target, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
					if (target instanceof Connection && result instanceof PreparedStatement
							&& arguments[0] instanceof String sql
							&& sql.contains("batch_outbox")) {
						return delayEmptyScan(result);
					}
					if (result instanceof ResultSet) {
						return delayEmptyScan(result);
					}
					if (target instanceof ResultSet && method.getName().equals("next")
							&& Boolean.FALSE.equals(result)) {
						Runnable gate = EMPTY_SCAN.getAndSet(null);
						if (gate != null) {
							gate.run();
						}
					}
					return result;
				});
	}

	void assertCompletedBatchUnchanged(List<List<Map<String, Object>>> committed) {
		assertEquals(committed, storedRecommendations(recommendations));
		WAKE_UPS.forEach(wakeUp -> assertEquals(committed, wakeUp.rows()));
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
	}

	ConfigurableApplicationContext startApplication(String... properties) {
		List<String> arguments = new ArrayList<>(List.of(
				"--spring.datasource.url=" + url("pricingconfig"),
				"--spring.datasource.username=sa",
				"--spring.datasource.password=",
				"--spring.datasource.driver-class-name=org.h2.Driver",
				"--account-store.datasource.url=" + url("accountdb"),
				"--account-store.datasource.username=sa",
				"--account-store.datasource.password=",
				"--account-store.datasource.driver-class-name=org.h2.Driver",
				"--account-store.jpa.ddl-auto=update",
				"--recommendation-store.datasource.url=" + url("recommendationdb"),
				"--recommendation-store.datasource.username=sa",
				"--recommendation-store.datasource.password=",
				"--recommendation-store.datasource.driver-class-name=org.h2.Driver",
				"--transaction-processor.wake-up-url=http://localhost:"
						+ TRANSACTION_PROCESSOR.getAddress().getPort() + "/api/v1/mock/txn/wake",
				"--TRANSACTION_PROCESSOR_RETRY_DELAY=50ms",
				"--TRANSACTION_PROCESSOR_HTTP_TIMEOUT=100ms"));
		for (String property : properties) {
			String name = property.substring(0, property.indexOf('=') + 1);
			arguments.removeIf(argument -> argument.startsWith(name));
			arguments.add(property);
		}
		return new SpringApplicationBuilder(
				EngineApplication.class, PricingServiceTests.Configuration.class)
				.properties("server.port=0", "engine.max-workers=1")
				.run(arguments.toArray(String[]::new));
	}

	void assertAttempts(int count) {
		await().atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertEquals(count, WAKE_UPS.size()));
		await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
				.untilAsserted(() -> assertEquals(count, WAKE_UPS.size()));
	}

	static HttpServer startTransactionProcessor() {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
			server.setExecutor(HTTP_EXECUTOR);
			server.createContext("/api/v1/mock/txn/wake", exchange -> {
				ACTIVE_REQUESTS.incrementAndGet();
				try (exchange) {
					int attempt = WAKE_UPS.size();
					WAKE_UPS.add(new WakeUp(exchange.getRequestMethod(),
							exchange.getRequestURI().getPath(), exchange.getRequestURI().getQuery(),
							new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
							storedRecommendations(recommendationStore), System.nanoTime()));
					try {
						responseGate.await(3, TimeUnit.SECONDS);
						Thread.sleep(responseDelayMillis);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						return;
					}
					exchange.sendResponseHeaders(responses[Math.min(attempt, responses.length - 1)], -1);
				} finally {
					ACTIVE_REQUESTS.decrementAndGet();
				}
			});
			server.start();
			return server;
		} catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	static List<List<Map<String, Object>>> storedRecommendations(JdbcTemplate database) {
		return List.of(
				database.queryForList("select * from account_recommendations order by 1"),
				database.queryForList("select * from fee_request_recommendations order by 1"),
				database.queryForList("select * from recommendation_eligibility_reasons order by 1, 2"),
				database.queryForList("select * from recommendation_outbox order by 1"));
	}

	private record WakeUp(String method, String path, String query, String body,
			List<List<Map<String, Object>>> rows, long receivedAt) {
	}

	long addAccount(UUID batchId) {
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", batchId);
		accounts.update("""
				insert into accounts(batch_id, account_number, product_code, branch_code, pricing_date)
				values (?, 'A1', 'CHECKING', 'NY', '2026-01-15')
				""", batchId);
		Long accountId = accounts.queryForObject(
				"select id from accounts where batch_id = ?", Long.class, batchId);
		accounts.update("""
				insert into fee_requests(account_id, fee_request_id, code, transaction_amount)
				values (?, 42, 'SERVICE', 25.00)
				""", accountId);
		return accountId;
	}
}
