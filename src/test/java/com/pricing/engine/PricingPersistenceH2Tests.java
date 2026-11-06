package com.pricing.engine;

import java.math.BigDecimal;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import com.sun.net.httpserver.HttpServer;
import com.pricing.engine.pricingStore.PricingRepository;

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
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
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
	MockMvc mockMvc;

	@MockitoSpyBean(name = "accountDataSource")
	DataSource accountDataSource;

	@MockitoSpyBean(name = "recommendationDataSource")
	DataSource recommendationDataSource;

	@Autowired
	PricingRepository pricingRepository;

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
		recommendations.update("delete from recommendation_batches");
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

	@ParameterizedTest
	@ValueSource(ints = {0, 1, 2})
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
						"select count(*) from recommendation_batches", Integer.class));
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
						"select count(*) from recommendation_batches", Integer.class));
			} finally {
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertEquals(Set.of(first, second, missed), Set.copyOf(recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class)));
		assertEquals(List.of(0L, 1L, 2L), PricingServiceTests.COMPLETED_BEFORE_EVALUATION);
		assertEquals(Set.of("batch_id", "processed"), Set.copyOf(availableAccounts.queryForList("""
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
			if (attempt == 1) {
				failedAt.set(System.nanoTime());
				throw new SQLTransientConnectionException("Temporary outage", "08001");
			}
			if (attempt == 2) {
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
	void startupFinishesBeforeTheWakeUpListenerAcceptsConnections() throws Exception {
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
				try (var request = new Socket()) {
					assertThrows(IOException.class, () -> request.connect(
							new InetSocketAddress("127.0.0.1", port), 200));
				}
				finishInitialization.complete(null);
				application.get(10, TimeUnit.SECONDS);
				assertEquals(202, RestClient.create().post()
						.uri("http://127.0.0.1:" + port + "/api/v1/work-available")
						.retrieve().toBodilessEntity().getStatusCode().value());
			} finally {
				finishInitialization.complete(null);
				try (var started = application.get(10, TimeUnit.SECONDS)) {
					assertAttempts(1);
				}
			}
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
						.untilAsserted(() -> assertFalse(startup.isDone()));
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
				assertTrue(PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty());
				assertFalse(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
			} finally {
				unavailable.set(false);
				startup.get(5, TimeUnit.SECONDS);
			}
		}
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from recommendation_batches", Integer.class));
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
						"select count(*) from recommendation_batches", Integer.class));
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
					"select count(*) from recommendation_batches", Integer.class) > 0) {
				attempts.incrementAndGet();
				throw new SQLTransientConnectionException("Source completion outage", "08001");
			}
			return invocation.callRealMethod();
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
					"select count(*) from recommendation_batches", Integer.class));
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
				await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
						.untilAsserted(() -> assertFalse(wakeUp.isDone()));
				finishScan.complete(null);
				wakeUp.get(5, TimeUnit.SECONDS);
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
		arguments.addAll(List.of(properties));
		return new SpringApplicationBuilder(
				EngineApplication.class, PricingServiceTests.Configuration.class)
				.properties("server.port=0")
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
				database.queryForList("select * from recommendation_batches order by 1"),
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
