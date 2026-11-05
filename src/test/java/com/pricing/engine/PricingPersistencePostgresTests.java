package com.pricing.engine;

import com.pricing.engine.ruleEngine.RuleEngine;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import com.sun.net.httpserver.HttpServer;
import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.recommendationStore.RecommendationRepository;
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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestClient;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
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

	static final AtomicReference<Runnable> EMPTY_SCAN = new AtomicReference<>();

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
				.locations("classpath:account-store").load().migrate();
		properties.add("transaction-processor.wake-up-url", () ->
				"http://localhost:" + TRANSACTION_PROCESSOR.getAddress().getPort()
						+ "/api/v1/mock/txn/wake");
		properties.add("database.retry-delay", () -> "200ms");
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

	@Autowired
	MockMvc mockMvc;

	@MockitoSpyBean(name = "accountDataSource")
	DataSource accountDataSource;

	@MockitoSpyBean(name = "recommendationDataSource")
	DataSource recommendationDataSource;

	@Autowired
	AccountRepository accountRepository;

	@Autowired
	RecommendationRepository recommendationRepository;

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
		EMPTY_SCAN.set(null);
		WAKE_UPS.clear();
		responses = new int[] {204};
		responseDelayMillis = 0;
		responseGate = new CountDownLatch(0);
		recommendations.execute("""
				truncate recommendation_eligibility_reasons, fee_request_recommendations,
				account_recommendations, recommendation_outbox, recommendation_batches
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
		assertEquals(new BigDecimal("2.50"), recommendations.queryForObject(
				"select amount from fee_request_recommendations", BigDecimal.class));
	}

	@Test
	void retriesAtomicStorageWithoutReloadingChangedConfiguration() throws Exception {
		addAccount(UUID.randomUUID());
		failRecommendationWrites(2);
		AtomicInteger connections = new AtomicInteger();
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			if (connections.getAndIncrement() < 3) {
				PricingServiceTests.januaryAmount = "9.99";
				try (var statement = connection.createStatement();
						var rows = statement.executeQuery(
								"select count(*) from recommendation_batches")) {
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

	@Test
	void retainsResultsAfterStorageRetriesExhaustWithoutResubmittingInput() {
		UUID batchId = UUID.randomUUID();
		addAccount(batchId);
		failRecommendationWrites(3);
		try {
			assertThrows(TransientDataAccessException.class, service::processPendingAccounts);
			assertEquals(3L, recommendations.queryForObject(
					"select last_value from recommendation_write_attempts", Long.class));
			assertEquals(0, recommendations.queryForObject(
					"select count(*) from recommendation_batches", Integer.class));
			assertEquals(0, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertFalse(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
			PricingServiceTests.januaryAmount = "9.99";
		} finally {
			removeRecommendationWriteFailure();
			service.processPendingAccounts();
		}
		assertEquals(1, PricingServiceTests.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(new BigDecimal("2.50"), recommendations.queryForObject(
				"select amount from fee_request_recommendations", BigDecimal.class));
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
	}

	@Test
	void readRetryExhaustionStopsTheRunAndLeavesAllInputUnprocessed() throws Exception {
		addAccount(UUID.randomUUID());
		addAccount(UUID.randomUUID());
		AtomicInteger attempts = new AtomicInteger();
		doAnswer(invocation -> {
			attempts.incrementAndGet();
			throw new SQLTransientConnectionException("Temporary outage", "08001");
		}).when(accountDataSource).getConnection();
		try {
			assertThrows(CannotCreateTransactionException.class, service::processPendingAccounts);
			assertEquals(3, attempts.get());
		} finally {
			doCallRealMethod().when(accountDataSource).getConnection();
		}
		assertTrue(PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty());
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed = false", Integer.class));
		assertEquals(0, recommendations.queryForObject(
				"select count(*) from recommendation_batches", Integer.class));
		service.processPendingAccounts();
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed = true", Integer.class));
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
	void startupRetryExhaustionLeavesInputAvailableForAnotherRun() throws Exception {
		addAccount(UUID.randomUUID());
		AtomicInteger attempts = new AtomicInteger();
		doAnswer(invocation -> {
			attempts.incrementAndGet();
			throw new SQLTransientConnectionException("Temporary outage", "08001");
		}).when(recommendationDataSource).getConnection();
		try {
			assertThrows(CannotCreateTransactionException.class, service::afterSingletonsInstantiated);
			assertEquals(3, attempts.get());
		} finally {
			doCallRealMethod().when(recommendationDataSource).getConnection();
		}
		assertTrue(PricingServiceTests.COMPLETED_BEFORE_EVALUATION.isEmpty());
		service.processPendingAccounts();
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
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
			assertThrows(DataIntegrityViolationException.class, service::processPendingAccounts);
			for (String table : List.of("recommendation_batches", "account_recommendations",
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
				"select batch_id from recommendation_batches", UUID.class));
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
					"select count(*) from recommendation_batches", Integer.class));
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
		// Delay the JDBC result after PostgreSQL has found no rows, before discovery returns.
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
