package com.pricing.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import com.sun.net.httpserver.HttpServer;
import com.pricing.engine.pricingStore.PricingRepository;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.RuleEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import java.time.LocalDate;

@SpringBootTest(properties = "engine.max-workers=1")
@AutoConfigureMockMvc
@Import(MockPricing.class)
@ExtendWith(H2TestSupport.FixtureLifecycle.class)
public abstract class H2TestSupport {

	protected static final ExecutorService HTTP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
	protected static final List<WakeUp> WAKE_UPS = new CopyOnWriteArrayList<>();
	protected static final AtomicInteger ACTIVE_REQUESTS = new AtomicInteger();
	protected static final HttpServer TRANSACTION_PROCESSOR = startTransactionProcessor();
	protected static volatile JdbcTemplate recommendationStore;
	protected static volatile int[] responses = {204};
	protected static volatile long responseDelayMillis;
	protected static volatile CountDownLatch responseGate = new CountDownLatch(0);

	protected static final AtomicReference<Runnable> EMPTY_SCAN = new AtomicReference<>();

	@DynamicPropertySource
	protected static void databases(DynamicPropertyRegistry properties) {
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

	protected static String url(String database) {
		return "jdbc:h2:mem:pricing_recovery_" + database
				+ ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;NON_KEYWORDS=VALUE"
				+ ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;LOCK_TIMEOUT=10000";
	}

	@Autowired
	protected EngineService service;

	@Autowired
	protected ConfigurableApplicationContext application;

	@Autowired
	protected MockMvc mockMvc;

	@MockitoSpyBean(name = "accountDataSource")
	protected DataSource accountDataSource;

	@MockitoSpyBean(name = "recommendationDataSource")
	protected DataSource recommendationDataSource;

	@Autowired
	protected PricingRepository pricingRepository;

	@MockitoSpyBean
	protected RuleEngine ruleEngine;

	@Autowired
	protected RecommendationRepository recommendationRepository;

	protected JdbcTemplate accounts;
	protected JdbcTemplate recommendations;

	@Autowired
	protected void stores(@Qualifier("accountDataSource") DataSource accountStore,
			@Qualifier("recommendationDataSource") DataSource recommendationStore) {
		accounts = new JdbcTemplate(accountStore);
		recommendations = new JdbcTemplate(recommendationStore);
		H2TestSupport.recommendationStore = recommendations;
	}

	@BeforeEach
	protected void cleanUp() {
		MockPricing.pricingRead = () -> {};
		MockPricing.januaryAmount = "2.50";
		MockPricing.COMPLETED_BEFORE_EVALUATION.clear();
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
	protected void finishNotifications() {
		responseGate.countDown();
		responseDelayMillis = 0;
		await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(3))
				.until(() -> ACTIVE_REQUESTS.get() == 0);
		MockPricing.pricingRead = () -> {};
		MockPricing.januaryAmount = "2.50";
		MockPricing.COMPLETED_BEFORE_EVALUATION.clear();
	}

	protected void assertCompletedBatchUnchanged(List<List<Map<String, Object>>> committed) {
		assertCompletedBatchUnchanged(committed, WAKE_UPS);
	}

	protected void assertCompletedBatchUnchanged(List<List<Map<String, Object>>> committed,
			List<WakeUp> wakeUps) {
		assertEquals(committed, storedRecommendations(recommendations));
		wakeUps.forEach(wakeUp -> assertEquals(committed, wakeUp.rows()));
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
	}

	protected ConfigurableApplicationContext startApplication(String... properties) {
		List<WakeUp> wakeUps = new CopyOnWriteArrayList<>();
		return startApplication(
				new WakeUpReceiver(startTransactionProcessor(wakeUps), wakeUps), properties);
	}

	protected ConfigurableApplicationContext startApplication(WakeUpReceiver receiver, String... properties) {
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
						+ receiver.server().getAddress().getPort() + "/api/v1/mock/txn/wake",
				"--TRANSACTION_PROCESSOR_RETRY_DELAY=50ms",
				"--TRANSACTION_PROCESSOR_HTTP_TIMEOUT=100ms"));
		for (String property : properties) {
			String name = property.substring(0, property.indexOf('=') + 1);
			arguments.removeIf(argument -> argument.startsWith(name));
			arguments.add(property);
		}
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
		public WakeUpReceiver {
		}
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
			context.getRoot().getStore(ExtensionContext.Namespace.create(H2TestSupport.class))
					.getOrComputeIfAbsent(H2TestSupport.class,
							key -> (ExtensionContext.Store.CloseableResource) () -> {
								TRANSACTION_PROCESSOR.stop(0);
								HTTP_EXECUTOR.close();
							});
		}
	}

	protected void addAccount(UUID batchId, String number, LocalDate date) {
		accounts.update("""
				insert into accounts(batch_id, account_number, product_code, branch_code, pricing_date)
				values (?, ?, 'CHECKING', 'NY', ?)
				""", batchId, number, date);
		Long accountId = accounts.queryForObject(
				"select id from accounts where batch_id = ? and account_number = ?",
				Long.class, batchId, number);
		accounts.update("""
				insert into fee_requests(account_id, fee_request_id, code)
				values (?, 42, 'SERVICE')
				""", accountId);
	}
}
