package com.pricing.engine;

import java.math.BigDecimal;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import com.sun.net.httpserver.HttpServer;
import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import com.pricing.engine.ruleEngine.RuleEngine;
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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "engine.max-workers=3")
@AutoConfigureMockMvc
@Import(PricingServiceTests.Configuration.class)
@Testcontainers(disabledWithoutDocker = true)
class PricingPersistencePostgresTests {

	static final ExecutorService HTTP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
	static final List<WakeUp> WAKE_UPS = new CopyOnWriteArrayList<>();
	static final AtomicInteger ACTIVE_REQUESTS = new AtomicInteger();
	static final HttpServer TRANSACTION_PROCESSOR = startTransactionProcessor();
	static volatile JdbcTemplate recommendationStore;
	static volatile int[] responses = {204};
	static volatile long responseDelayMillis;
	static volatile CountDownLatch responseGate = new CountDownLatch(0);

	@Container
	static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
			.withDatabaseName("pricingconfig");

	@DynamicPropertySource
	static void databases(DynamicPropertyRegistry properties) throws SQLException {
		try (var connection = DriverManager.getConnection(
				POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
				var statement = connection.createStatement()) {
			statement.execute("create database accountdb");
			statement.execute("create database recommendationdb");
		}
		String accountUrl = url("accountdb");
		Flyway.configure().dataSource(accountUrl, POSTGRES.getUsername(), POSTGRES.getPassword())
				.locations("classpath:db/account").load().migrate();
		properties.add("transaction-processor.wake-up-url", () ->
				"http://localhost:" + TRANSACTION_PROCESSOR.getAddress().getPort()
						+ "/api/v1/mock/txn/wake");
		properties.add("database.retry-delay", () -> "200ms");
		properties.add("database.recovery-check-interval", () -> "100ms");
		properties.add("transaction-processor.retry-delay", () -> "50ms");
		properties.add("transaction-processor.http-timeout", () -> "100ms");
		properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		properties.add("spring.datasource.username", POSTGRES::getUsername);
		properties.add("spring.datasource.password", POSTGRES::getPassword);
		properties.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
		properties.add("account-store.datasource.url", () -> accountUrl);
		properties.add("account-store.datasource.username", POSTGRES::getUsername);
		properties.add("account-store.datasource.password", POSTGRES::getPassword);
		properties.add("account-store.datasource.driver-class-name", () -> "org.postgresql.Driver");
		properties.add("account-store.jpa.ddl-auto", () -> "validate");
		properties.add("recommendation-store.datasource.url", () -> url("recommendationdb"));
		properties.add("recommendation-store.datasource.username", POSTGRES::getUsername);
		properties.add("recommendation-store.datasource.password", POSTGRES::getPassword);
		properties.add("recommendation-store.datasource.driver-class-name",
				() -> "org.postgresql.Driver");
	}

	static String url(String database) {
		return "jdbc:postgresql://" + POSTGRES.getHost() + ":"
				+ POSTGRES.getMappedPort(5432) + "/" + database;
	}

	@Autowired
	EngineService service;

	@MockitoSpyBean(name = "accountDataSource")
	DataSource accountDataSource;

	@MockitoSpyBean(name = "recommendationDataSource")
	DataSource recommendationDataSource;

	@Autowired
	AccountRepository accountRepository;

	@Autowired
	RecommendationRepository recommendationRepository;

	@Autowired
	RuleEngine ruleEngine;

	JdbcTemplate accounts;
	JdbcTemplate recommendations;

	@Autowired
	void stores(@Qualifier("accountDataSource") DataSource accountStore,
			@Qualifier("recommendationDataSource") DataSource recommendationStore) {
		accounts = new JdbcTemplate(accountStore);
		recommendations = new JdbcTemplate(recommendationStore);
		PricingPersistencePostgresTests.recommendationStore = recommendations;
	}

	@BeforeEach
	void cleanUp() {
		PricingServiceTests.configurationRead = () -> {};
		PricingServiceTests.januaryAmount = "2.50";
		PricingServiceTests.COMPLETED_BEFORE_EVALUATION.clear();
		WAKE_UPS.clear();
		responses = new int[] {204};
		responseDelayMillis = 0;
		responseGate = new CountDownLatch(0);
		recommendations.execute("""
				truncate recommendation_eligibility_reasons, fee_request_recommendations,
				account_recommendations, recommendation_outbox
				""");
		accounts.execute("truncate account_attributes, fee_requests, accounts, batch_outbox");
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
	void retriesAtomicStorageWithoutReloadingChangedConfiguration() throws Exception {
		addAccount(UUID.randomUUID());
		failRecommendationWrites(2);
		AtomicInteger connections = new AtomicInteger();
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			if (!PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty()
					&& connections.getAndIncrement() < 3) {
				PricingServiceTests.januaryAmount = "9.99";
				try (var statement = connection.createStatement();
						var rows = statement.executeQuery(
								"select count(*) from recommendation_outbox")) {
					rows.next();
					assertEquals(0, rows.getInt(1));
				}
			}
			return connection;
		}).when(recommendationDataSource).getConnection();
		try {
			service.processPendingAccounts();

			assertEquals(3L, recommendations.queryForObject(
					"select last_value from recommendation_write_attempts", Long.class));
			assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
			assertEquals("9.99", PricingServiceTests.januaryAmount);
			assertEquals(new BigDecimal("2.50"), recommendations.queryForObject(
					"select amount from fee_request_recommendations", BigDecimal.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		} finally {
			removeRecommendationWriteFailure();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void restartRepairsAccountCompletionWithoutChangingCommittedOutput(boolean processed) {
		addAccount(UUID.randomUUID());
		accounts.execute("""
				alter table batch_outbox add constraint reject_test_completion
				check (processed = false)
				""");
		try {
			assertThrows(DataIntegrityViolationException.class, service::processPendingAccounts);
			assertAttempts(1);
			assertFalse(accounts.queryForObject(
					"select processed from batch_outbox", Boolean.class));
		} finally {
			accounts.execute("alter table batch_outbox drop constraint reject_test_completion");
		}
		recommendations.update("update recommendation_outbox set processed = ?", processed);
		var committed = storedRecommendations(recommendations);
		PricingServiceTests.januaryAmount = "9.99";
		PricingServiceTests.configurationRead = () -> {
			throw new AssertionError("Committed output must not be recalculated on restart");
		};

		try (var application = startApplication()) {
			assertAttempts(application.getBean(WakeUpReceiver.class).wakeUps(), processed ? 0 : 1);
			assertAttempts(1);
			assertEquals(committed, storedRecommendations(recommendations));
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox", Boolean.class));
			assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
			application.getBean(EngineService.class).processPendingAccounts();
			assertEquals(committed, storedRecommendations(recommendations));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"recommendation", "account"})
	void lostCommitAcknowledgementPreservesCommittedOutput(String store) throws Exception {
		addAccount(UUID.randomUUID());
		var availableRecommendations = new JdbcTemplate(new DriverManagerDataSource(
				url("recommendationdb"), POSTGRES.getUsername(), POSTGRES.getPassword()));
		DataSource storeDataSource = store.equals("recommendation")
				? recommendationDataSource : accountDataSource;
		var loseAcknowledgement = new AtomicBoolean(true);
		var committed = new AtomicReference<List<List<Map<String, Object>>>>();
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return Proxy.newProxyInstance(Connection.class.getClassLoader(),
					new Class<?>[] {Connection.class}, (proxy, method, arguments) -> {
						boolean sourceCompletion = false;
						if (store.equals("account") && method.getName().equals("commit")
								&& loseAcknowledgement.get()) {
							try (var statement = connection.createStatement();
									var rows = statement.executeQuery("""
											select exists(select 1 from batch_outbox where processed)
											""")) {
								rows.next();
								sourceCompletion = rows.getBoolean(1);
							}
						}
						if (method.getName().equals("commit") && !connection.isReadOnly()
								&& (store.equals("recommendation") || sourceCompletion)
								&& loseAcknowledgement.compareAndSet(true, false)) {
							connection.commit();
							if (store.equals("recommendation")) {
								availableRecommendations.update(
										"update recommendation_outbox set processed = true");
							}
							committed.set(storedRecommendations(availableRecommendations));
							PricingServiceTests.januaryAmount = "9.99";
							PricingServiceTests.configurationRead = () -> {
								throw new AssertionError("A storage retry must not recalculate");
							};
							throw new SQLTransientConnectionException(
									"Commit acknowledgement lost", "08006");
						}
						try {
							return method.invoke(connection, arguments);
						} catch (InvocationTargetException ex) {
							throw ex.getCause();
						}
					});
		}).when(storeDataSource).getConnection();
		try {
			service.processPendingAccounts();
			assertFalse(loseAcknowledgement.get());
			assertAttempts(1);
			assertCompletedBatchUnchanged(committed.get());
			assertEquals(new BigDecimal("2.50"), recommendations.queryForObject(
					"select amount from fee_request_recommendations", BigDecimal.class));
		} finally {
			doCallRealMethod().when(storeDataSource).getConnection();
		}
	}

	void failRecommendationWrites(int failures) {
		recommendations.execute("create sequence recommendation_write_attempts");
		recommendations.execute("""
				create function fail_recommendation_write() returns trigger
				language plpgsql as $function$
				begin
					if nextval('recommendation_write_attempts') <= %d then
						raise exception 'Temporary write failure' using errcode = '40001';
					end if;
					return NEW;
				end;
				$function$
				""".formatted(failures));
		recommendations.execute("""
				create trigger fail_recommendation_write before insert on recommendation_outbox
				for each row execute function fail_recommendation_write()
				""");
	}

	void removeRecommendationWriteFailure() {
		recommendations.execute(
				"drop trigger fail_recommendation_write on recommendation_outbox");
		recommendations.execute("drop function fail_recommendation_write()");
		recommendations.execute("drop sequence recommendation_write_attempts");
	}

	@Test
	void notifiesOnlyAfterCommitWithoutBusinessPayload() {
		UUID batchId = UUID.randomUUID();
		addAccount(batchId);

		service.processPendingAccounts();

		assertAttempts(1);
		WakeUp wakeUp = WAKE_UPS.getFirst();
		assertEquals("POST", wakeUp.method());
		assertEquals("/api/v1/mock/txn/wake", wakeUp.path());
		assertNull(wakeUp.query());
		assertEquals("", wakeUp.body());
		assertEquals(storedRecommendations(recommendations), wakeUp.rows());
		assertFalse(recommendations.queryForObject(
				"select processed from recommendation_outbox", Boolean.class));
		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox", Boolean.class));
	}

	@Test
	void rollsBackCompleteOutputWhenTheRecommendationOutboxInsertFails() {
		UUID batchId = UUID.fromString("b566f38a-0d68-4adb-8a86-84b0d945d645");
		addAccount(batchId);
		recommendations.execute("""
				alter table recommendation_outbox add constraint reject_test_output
				check (batch_id <> 'b566f38a-0d68-4adb-8a86-84b0d945d645')
				""");
		try {
			RuleEngineBatchResult result = ruleEngine.price(accountRepository.load(batchId));
			assertThrows(DataIntegrityViolationException.class,
					() -> recommendationRepository.save(result));
			for (String table : List.of("account_recommendations",
					"fee_request_recommendations", "recommendation_eligibility_reasons",
					"recommendation_outbox")) {
				assertEquals(0, recommendations.queryForObject(
						"select count(*) from " + table, Integer.class), table);
			}
			assertFalse(accounts.queryForObject("select processed from batch_outbox",
					Boolean.class));
			await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
					.untilAsserted(() -> assertTrue(WAKE_UPS.isEmpty()));
		} finally {
			recommendations.execute(
					"alter table recommendation_outbox drop constraint reject_test_output");
		}
		PricingServiceTests.januaryAmount = "9.99";
		try (var application = startApplication()) {
			assertAttempts(application.getBean(WakeUpReceiver.class).wakeUps(), 1);
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox", Boolean.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertEquals(new BigDecimal("9.99"), recommendations.queryForObject(
					"select amount from fee_request_recommendations", BigDecimal.class));
			assertEquals(2, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		}
	}

	@Test
	void readsCanonicalAccountStoreDatesScalarAttributesAndFeeRequests() {
		UUID batchId = UUID.randomUUID();
		long accountId = addAccount(batchId);
		accounts.update("""
				insert into accounts(batch_id, account_number, product_code, branch_code, pricing_date)
				values (?, 'A2', 'CHECKING', 'NY', '2026-02-15')
				""", batchId);
		for (var attribute : Map.of("VIP", "true", "BALANCE", "12.50", "TIER", "\"GOLD\"")
				.entrySet()) {
			accounts.update("""
					insert into account_attributes(account_id, code, value)
					values (?, ?, cast(? as jsonb))
					""", accountId, attribute.getKey(), attribute.getValue());
		}

		RuleEngineBatch batch = accountRepository.load(batchId);

		assertEquals(batchId, batch.batchId());
		assertEquals(Set.of(LocalDate.of(2026, 1, 15), LocalDate.of(2026, 2, 15)),
				batch.accounts().stream().map(RuleEngineBatch.Account::pricingDate)
						.collect(Collectors.toSet()));
		var first = batch.accounts().stream().filter(a -> a.accountNumber().equals("A1"))
				.findFirst().orElseThrow();
		var attributes = first.attributes().stream().collect(Collectors.toMap(
				RuleEngineBatch.AccountAttribute::code, RuleEngineBatch.AccountAttribute::value));
		assertEquals(true, attributes.get("VIP"));
		assertEquals("GOLD", attributes.get("TIER"));
		assertEquals(0, ((BigDecimal) attributes.get("BALANCE"))
				.compareTo(new BigDecimal("12.50")));
		assertEquals(42L, first.fees().getFirst().feeRequestId());
		assertEquals("SERVICE", first.fees().getFirst().code());
		assertEquals(0, first.fees().getFirst().transactionAmount()
				.compareTo(new BigDecimal("25.00")));
	}

	@Test
	void storesAllOutcomeFieldsAndEnforcesTheUniqueRecommendationOutboxKey() {
		UUID batchId = UUID.randomUUID();
		var result = new RuleEngineBatchResult(batchId, List.of(
				new AccountResult("A1", AccountStatus.OK, "JANPLAN", List.of(
						new FeeResult(41L, FeeStatus.OK, Decision.CHARGED,
								new BigDecimal("12.50"), null),
						new FeeResult(42L, FeeStatus.OK, Decision.WAIVED, null,
								List.of("VIP", "LOYAL")),
						new FeeResult(43L, FeeStatus.FEE_NOT_FOUND, null, null, null))),
				new AccountResult("A2", AccountStatus.PLAN_NOT_FOUND, null, null)));

		recommendationRepository.save(result);

		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
		assertEquals("JANPLAN", recommendations.queryForObject("""
				select pricing_plan_code from account_recommendations where account_number = 'A1'
				""", String.class));
		assertEquals("OK", recommendations.queryForObject("""
				select status from account_recommendations where account_number = 'A1'
				""", String.class));
		assertEquals("PLAN_NOT_FOUND", recommendations.queryForObject("""
				select status from account_recommendations where account_number = 'A2'
				""", String.class));
		assertNull(recommendations.queryForObject("""
				select pricing_plan_code from account_recommendations where account_number = 'A2'
				""", String.class));
		assertEquals(List.of(41L, 42L, 43L), recommendations.queryForList("""
				select fee_request_id from fee_request_recommendations order by fee_request_id
				""", Long.class));
		assertEquals(List.of("OK", "OK", "FEE_NOT_FOUND"), recommendations.queryForList("""
				select status from fee_request_recommendations order by fee_request_id
				""", String.class));
		assertEquals("CHARGED", recommendations.queryForObject("""
				select decision from fee_request_recommendations where fee_request_id = 41
				""", String.class));
		assertEquals(0, recommendations.queryForObject("""
				select amount from fee_request_recommendations where fee_request_id = 41
				""", BigDecimal.class).compareTo(new BigDecimal("12.50")));
		assertEquals("WAIVED", recommendations.queryForObject("""
				select decision from fee_request_recommendations where fee_request_id = 42
				""", String.class));
		assertNull(recommendations.queryForObject("""
				select amount from fee_request_recommendations where fee_request_id = 42
				""", BigDecimal.class));
		assertEquals(List.of("LOYAL", "VIP"), recommendations.queryForList("""
				select reason_code from recommendation_eligibility_reasons order by reason_code
				""", String.class));
		assertFalse(recommendations.queryForObject(
				"select processed from recommendation_outbox", Boolean.class));
		assertThrows(DataIntegrityViolationException.class, () -> recommendations.update("""
				insert into recommendation_outbox(batch_id, processed) values (?, false)
				""", batchId));
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
	}

	@Test
	void keepsCommittedRecommendationsWhenAccountCompletionFails() {
		addAccount(UUID.randomUUID());
		accounts.execute("""
				alter table batch_outbox add constraint reject_test_completion
				check (processed = false)
				""");
		try {
			assertThrows(DataIntegrityViolationException.class, service::processPendingAccounts);
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from fee_request_recommendations", Integer.class));
			assertFalse(accounts.queryForObject(
					"select processed from batch_outbox", Boolean.class));
			assertAttempts(1);
			assertEquals(storedRecommendations(recommendations), WAKE_UPS.getFirst().rows());
		} finally {
			accounts.execute("alter table batch_outbox drop constraint reject_test_completion");
		}
	}

	void assertCompletedBatchUnchanged(List<List<Map<String, Object>>> committed) {
		assertEquals(committed, storedRecommendations(recommendations));
		WAKE_UPS.forEach(wakeUp -> assertEquals(committed, wakeUp.rows()));
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
	}

	ConfigurableApplicationContext startApplication(String... properties) {
		List<WakeUp> wakeUps = new CopyOnWriteArrayList<>();
		var receiver = new WakeUpReceiver(startTransactionProcessor(wakeUps), wakeUps);
		List<String> arguments = new ArrayList<>(List.of(
				"--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
				"--spring.datasource.username=" + POSTGRES.getUsername(),
				"--spring.datasource.password=" + POSTGRES.getPassword(),
				"--spring.datasource.driver-class-name=org.postgresql.Driver",
				"--account-store.datasource.url=" + url("accountdb"),
				"--account-store.datasource.username=" + POSTGRES.getUsername(),
				"--account-store.datasource.password=" + POSTGRES.getPassword(),
				"--account-store.datasource.driver-class-name=org.postgresql.Driver",
				"--account-store.jpa.ddl-auto=validate",
				"--recommendation-store.datasource.url=" + url("recommendationdb"),
				"--recommendation-store.datasource.username=" + POSTGRES.getUsername(),
				"--recommendation-store.datasource.password=" + POSTGRES.getPassword(),
				"--recommendation-store.datasource.driver-class-name=org.postgresql.Driver",
				"--transaction-processor.wake-up-url=http://localhost:"
						+ receiver.server().getAddress().getPort() + "/api/v1/mock/txn/wake",
				"--TRANSACTION_PROCESSOR_RETRY_DELAY=50ms",
				"--TRANSACTION_PROCESSOR_HTTP_TIMEOUT=100ms"));
		arguments.addAll(List.of(properties));
		try {
			return new SpringApplicationBuilder(
					EngineApplication.class, PricingServiceTests.Configuration.class)
					.initializers(context -> {
						context.getBeanFactory().registerSingleton(
								WakeUpReceiver.class.getName(), receiver);
						context.addApplicationListener(event -> {
							if (event instanceof ContextClosedEvent closed
									&& closed.getApplicationContext() == context) {
								receiver.server().stop(0);
							}
						});
					})
					.properties("server.port=0", "engine.max-workers=1")
					.run(arguments.toArray(String[]::new));
		} catch (RuntimeException | Error ex) {
			receiver.server().stop(0);
			throw ex;
		}
	}

	void assertAttempts(int count) {
		assertAttempts(WAKE_UPS, count);
	}

	void assertAttempts(List<WakeUp> wakeUps, int count) {
		await().atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertEquals(count, wakeUps.size()));
		await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
				.untilAsserted(() -> assertEquals(count, wakeUps.size()));
	}

	static HttpServer startTransactionProcessor() {
		return startTransactionProcessor(WAKE_UPS);
	}

	static HttpServer startTransactionProcessor(List<WakeUp> wakeUps) {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
			server.setExecutor(HTTP_EXECUTOR);
			server.createContext("/api/v1/mock/txn/wake", exchange -> {
				ACTIVE_REQUESTS.incrementAndGet();
				try (exchange) {
					int attempt = wakeUps.size();
					wakeUps.add(new WakeUp(exchange.getRequestMethod(),
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

	private record WakeUpReceiver(HttpServer server, List<WakeUp> wakeUps) {
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
