package com.pricing.engine.engineService;

import com.pricing.engine.H2TestSupport;
import com.pricing.engine.MockPricing;

import java.net.ServerSocket;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.RestClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class Phase1StartupH2Tests extends H2TestSupport {

	@Test
	void readinessDoesNotWaitForPendingAccountProcessing() throws Exception {
		int port;
		try (var available = new ServerSocket(0)) {
			port = available.getLocalPort();
		}
		addAccount(UUID.randomUUID());
		var initializing = new CompletableFuture<Void>();
		var finishInitialization = new CompletableFuture<Void>();
		MockPricing.pricingRead = () -> {
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
			var wakeUps = application.getBean(WakeUpReceiver.class).wakeUps();
			await().atMost(Duration.ofSeconds(5)).until(() -> !wakeUps.isEmpty());
			String port = application.getEnvironment().getProperty("local.server.port");
			assertEquals(200, RestClient.create().get()
					.uri("http://127.0.0.1:" + port + "/openapi.yaml")
					.retrieve().toBodilessEntity().getStatusCode().value());
			assertFalse(pendingRecommendations.queryForObject(
					"select processed from recommendation_outbox", Boolean.class));
			responseGate.countDown();
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
				assertTrue(MockPricing.COMPLETED_BEFORE_EVALUATION.isEmpty());
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
}
