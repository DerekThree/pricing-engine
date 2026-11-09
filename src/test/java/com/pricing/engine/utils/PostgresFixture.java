package com.pricing.engine.utils;

import java.math.BigDecimal;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
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
import com.pricing.engine.EngineApplication;
import com.pricing.engine.EngineService;
import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.RuleEngine;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import static com.pricing.engine.utils.JdbcUtils.intercept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;

@SpringBootTest(properties = "engine.max-workers=3")
@AutoConfigureMockMvc
@Import(MockPricing.class)
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(PostgresFixture.FixtureLifecycle.class)
public abstract class PostgresFixture {

	protected static final ExecutorService HTTP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
	protected static final List<WakeUp> WAKE_UPS = new CopyOnWriteArrayList<>();
	protected static final AtomicInteger ACTIVE_REQUESTS = new AtomicInteger();
	protected static final HttpServer TRANSACTION_PROCESSOR = startTransactionProcessor();
	protected static volatile JdbcTemplate recommendationStore;
	protected static volatile int[] responses = {204};
	protected static volatile long responseDelayMillis;
	protected static volatile CountDownLatch responseGate = new CountDownLatch(0);

	protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
			.withDatabaseName("pricingconfig");

	@DynamicPropertySource
	protected static void databases(DynamicPropertyRegistry properties) throws SQLException {
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

	protected static String url(String database) {
		return "jdbc:postgresql://" + POSTGRES.getHost() + ":"
				+ POSTGRES.getMappedPort(5432) + "/" + database;
	}

	@Autowired
	protected EngineService service;

	@MockitoSpyBean(name = "accountDataSource")
	protected DataSource accountDataSource;

	@MockitoSpyBean(name = "recommendationDataSource")
	protected DataSource recommendationDataSource;

	@Autowired
	protected AccountRepository accountRepository;

	@Autowired
	protected RecommendationRepository recommendationRepository;

	@Autowired
	protected RuleEngine ruleEngine;

	protected JdbcTemplate accounts;
	protected JdbcTemplate recommendations;

	@Autowired
	protected void stores(@Qualifier("accountDataSource") DataSource accountStore,
			@Qualifier("recommendationDataSource") DataSource recommendationStore) {
		accounts = new JdbcTemplate(accountStore);
		recommendations = new JdbcTemplate(recommendationStore);
		PostgresFixture.recommendationStore = recommendations;
	}

	@BeforeEach
	protected void cleanUp() {
		MockPricing.pricingRead = () -> {};
		MockPricing.januaryAmount = "2.50";
		MockPricing.COMPLETED_BEFORE_EVALUATION.clear();
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
	protected void finishNotifications() {
		responseGate.countDown();
		responseDelayMillis = 0;
		await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(3))
				.until(() -> ACTIVE_REQUESTS.get() == 0);
		MockPricing.pricingRead = () -> {};
		MockPricing.januaryAmount = "2.50";
		MockPricing.COMPLETED_BEFORE_EVALUATION.clear();
	}


	protected void assertLostCommitAcknowledgementPreservesCommittedOutput(String store)
			throws Exception {
		addAccount(UUID.randomUUID());
		var availableRecommendations = new JdbcTemplate(new DriverManagerDataSource(
				url("recommendationdb"), POSTGRES.getUsername(), POSTGRES.getPassword()));
		DataSource storeDataSource = store.equals("recommendation")
				? recommendationDataSource : accountDataSource;
		var loseAcknowledgement = new AtomicBoolean(true);
		var committed = new AtomicReference<List<List<Map<String, Object>>>>();
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return intercept(connection, Connection.class, (method, arguments, proceed) -> {
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
					MockPricing.januaryAmount = "9.99";
					MockPricing.pricingRead = () -> {
						throw new AssertionError("A storage retry must not recalculate");
					};
					throw new SQLTransientConnectionException(
							"Commit acknowledgement lost", "08006");
				}
				return proceed.get();
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

	protected void assertCompletedBatchUnchanged(List<List<Map<String, Object>>> committed) {
		assertEquals(committed, storedRecommendations(recommendations));
		WAKE_UPS.forEach(wakeUp -> assertEquals(committed, wakeUp.rows()));
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
	}

	protected ConfigurableApplicationContext startApplication(String... properties) {
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
					EngineApplication.class, MockPricing.class)
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

	protected void assertAttempts(int count) {
		assertAttempts(WAKE_UPS, count);
	}

	protected void assertAttempts(List<WakeUp> wakeUps, int count) {
		await().atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertEquals(count, wakeUps.size()));
		await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
				.untilAsserted(() -> assertEquals(count, wakeUps.size()));
	}

	protected static HttpServer startTransactionProcessor() {
		return startTransactionProcessor(WAKE_UPS);
	}

	protected static HttpServer startTransactionProcessor(List<WakeUp> wakeUps) {
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

	protected static List<List<Map<String, Object>>> storedRecommendations(JdbcTemplate database) {
		return List.of(
				database.queryForList("select * from account_recommendations order by 1"),
				database.queryForList("select * from fee_request_recommendations order by 1"),
				database.queryForList("select * from recommendation_eligibility_reasons order by 1, 2"),
				database.queryForList("select * from recommendation_outbox order by 1"));
	}

	protected record WakeUpReceiver(HttpServer server, List<WakeUp> wakeUps) {
	}

	protected record WakeUp(String method, String path, String query, String body,
			List<List<Map<String, Object>>> rows, long receivedAt) {
	}

	protected long addAccount(UUID batchId) {
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

	static class FixtureLifecycle implements BeforeAllCallback {

		@Override
		public void beforeAll(ExtensionContext context) {
			context.getRoot().getStore(ExtensionContext.Namespace.create(PostgresFixture.class))
					.getOrComputeIfAbsent(PostgresFixture.class, key -> {
						POSTGRES.start();
						return (ExtensionContext.Store.CloseableResource) () -> {
							TRANSACTION_PROCESSOR.stop(0);
							HTTP_EXECUTOR.close();
							POSTGRES.stop();
						};
					});
		}
	}
}
