package com.pricing.engine;

import com.pricing.engine.ruleEngine.RuleEngine;

import java.math.BigDecimal;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.pricing.engine.pricingStore.PricingRepository;
import com.pricing.engine.ruleEngine.PriceConfig;
import com.pricing.engine.ruleEngine.PriceConfig.*;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "engine.max-workers=1")
@AutoConfigureMockMvc
class PricingServiceTests {

	static final List<Long> COMPLETED_BEFORE_EVALUATION = new CopyOnWriteArrayList<>();
	static volatile Runnable configurationRead = () -> {};
	static volatile String januaryAmount = "2.50";
	static final AtomicInteger WAKE_UPS = new AtomicInteger();
	static HttpServer transactionProcessor;

	@DynamicPropertySource
	static void notificationConsumer(DynamicPropertyRegistry properties) throws IOException {
		transactionProcessor = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
		transactionProcessor.createContext("/api/v1/mock/txn/wake", exchange -> {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				exchange.sendResponseHeaders(204, -1);
				WAKE_UPS.incrementAndGet();
			}
		});
		transactionProcessor.start();
		properties.add("transaction-processor.wake-up-url", () ->
				"http://localhost:" + transactionProcessor.getAddress().getPort()
						+ "/api/v1/mock/txn/wake");
	}

	@AfterAll
	static void stopTransactionProcessor() {
		transactionProcessor.stop(0);
	}

	@Autowired
	EngineService service;

	@Autowired
	MockMvc mockMvc;

	JdbcTemplate accounts;
	JdbcTemplate recommendations;

	@Autowired
	void stores(@Qualifier("accountDataSource") DataSource accountStore,
			@Qualifier("recommendationDataSource") DataSource recommendationStore) {
		accounts = new JdbcTemplate(accountStore);
		recommendations = new JdbcTemplate(recommendationStore);
	}

	@AfterEach
	void cleanUp() {
		recommendations.update("delete from recommendation_eligibility_reasons");
		recommendations.update("delete from fee_request_recommendations");
		recommendations.update("delete from account_recommendations");
		recommendations.update("delete from recommendation_outbox");
		accounts.update("delete from account_attributes");
		accounts.update("delete from fee_requests");
		accounts.update("delete from accounts");
		accounts.update("delete from batch_outbox");
		COMPLETED_BEFORE_EVALUATION.clear();
		WAKE_UPS.set(0);
		configurationRead = () -> {};
		januaryAmount = "2.50";
	}

	@Test
	void startupStoresRecommendationsUsingEachAccountsOwnPricingDate() {
		UUID batchId = UUID.randomUUID();
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", batchId);
		addAccount(batchId, "JAN", LocalDate.of(2026, 1, 15));
		addAccount(batchId, "FEB", LocalDate.of(2026, 2, 15));

		service.afterSingletonsInstantiated();
		await().atMost(Duration.ofSeconds(5))
				.until(() -> WAKE_UPS.get() >= 1 && accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
		service.afterSingletonsInstantiated();
		await().atMost(Duration.ofSeconds(5))
				.until(() -> WAKE_UPS.get() >= 2);

		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
		assertEquals("JANPLAN", recommendations.queryForObject(
				"select pricing_plan_code from account_recommendations where account_number = 'JAN'",
				String.class));
		assertEquals("FEBPLAN", recommendations.queryForObject(
				"select pricing_plan_code from account_recommendations where account_number = 'FEB'",
				String.class));
		assertEquals(0, recommendations.queryForObject("""
				select fee.amount from fee_request_recommendations fee
				join account_recommendations account on account.id = fee.account_id
				where account.account_number = 'JAN'
				""", BigDecimal.class).compareTo(new BigDecimal("2.50")));
		assertEquals(0, recommendations.queryForObject("""
				select fee.amount from fee_request_recommendations fee
				join account_recommendations account on account.id = fee.account_id
				where account.account_number = 'FEB'
				""", BigDecimal.class).compareTo(new BigDecimal("3.50")));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
	}

	@Test
	void wakeUpAcceptsNoPayloadAndStoresPendingRecommendations() throws Exception {
		UUID batchId = UUID.randomUUID();
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", batchId);
		addAccount(batchId, "WAKE", LocalDate.of(2026, 1, 15));

		mockMvc.perform(post("/api/v1/work-available"))
				.andExpect(status().isAccepted()).andExpect(content().string(""));

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
			assertTrue(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
			assertEquals(batchId, recommendations.queryForObject(
						"select batch_id from recommendation_outbox", UUID.class));
			assertEquals("JANPLAN", recommendations.queryForObject(
						"select pricing_plan_code from account_recommendations", String.class));
		});
	}

	@Test
	void drainsBatchesPersistedDuringEvaluationWithoutAnotherWakeUp() {
		UUID first = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", first);
		addAccount(first, "FIRST", LocalDate.of(2026, 1, 15));
		configurationRead = () -> {
			configurationRead = () -> {};
			accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", next);
			addAccount(next, "NEXT", LocalDate.of(2026, 1, 15));
		};

		service.processPendingAccounts();

		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
		assertEquals(List.of(0L, 1L), COMPLETED_BEFORE_EVALUATION);
		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
	}

	@Test
	void duplicateWakeUpsShareTheActiveRun() throws Exception {
		UUID first = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", first);
		addAccount(first, "FIRST", LocalDate.of(2026, 1, 15));
		var evaluating = new CompletableFuture<Void>();
		var resume = new CompletableFuture<Void>();
		configurationRead = () -> {
			evaluating.complete(null);
			resume.join();
		};

		try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
			try {
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
				evaluating.get(5, TimeUnit.SECONDS);
				accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", next);
				addAccount(next, "NEXT", LocalDate.of(2026, 1, 15));
				var processing = requests.submit(service::processPendingAccounts);
				for (int i = 0; i < 8; i++) {
					mockMvc.perform(post("/api/v1/work-available"))
							.andExpect(status().isAccepted());
				}
				processing.get(2, TimeUnit.SECONDS);
			} finally {
				resume.complete(null);
			}
		}

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(2,
				accounts.queryForObject("select count(*) from batch_outbox where processed = true",
						Integer.class)));
		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
		assertEquals(List.of(0L, 1L), COMPLETED_BEFORE_EVALUATION);
	}

	@Test
	void completesEachAccountBatchBeforeEvaluatingTheNext() {
		for (String accountNumber : List.of("FIRST", "SECOND")) {
			UUID batchId = UUID.randomUUID();
			accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)",
					batchId);
			addAccount(batchId, accountNumber, LocalDate.of(2026, 1, 15));
		}

		service.processPendingAccounts();

		assertEquals(List.of(0L, 1L), COMPLETED_BEFORE_EVALUATION);
		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
	}

	void addAccount(UUID batchId, String number, LocalDate date) {
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

	@TestConfiguration(proxyBeanMethods = false)
	static class Configuration {

		@Bean
		@Primary
		PricingRepository testPricingConfiguration(
				@Qualifier("accountDataSource") DataSource accountStore) {
			PricingRepository repository = mock(PricingRepository.class);
			when(repository.load(anySet())).thenAnswer(invocation -> {
				COMPLETED_BEFORE_EVALUATION.add(new JdbcTemplate(accountStore).queryForObject(
						"select count(*) from batch_outbox where processed = true", Long.class));
				configurationRead.run();
				return new PriceConfig(Map.of("NY", new Branch("NY", "NY", "10001")),
						List.of(new Region("EAST", Set.of("NY"), Set.of(), Set.of())), List.of(
								plan("JANPLAN", 1, januaryAmount), plan("FEBPLAN", 2, "3.50")));
			});
			return repository;
		}

		private Plan plan(String code, int month, String amount) {
			LocalDate start = LocalDate.of(2026, month, 1);
			return new Plan(code, "CHECKING", "EAST", start, start.withDayOfMonth(
					start.lengthOfMonth()), List.of(new PlanFee("SERVICE", FeeType.FLAT,
					new BigDecimal(amount), List.of())));
		}
	}
}
