package com.pricing.engine.engineService;

import com.pricing.engine.utils.H2Fixture;
import com.pricing.engine.utils.MockPricing;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;

class Phase6NotificationDeliveryH2Tests extends H2Fixture {

	@ParameterizedTest(name = "{0}")
	@MethodSource("deliveryOutcomes")
	void retriesDeliveryWithoutChangingRecommendations(String scenario, int[] statuses,
			boolean timeout, int expectedAttempts) {
		responses = statuses;
		responseGate = new CountDownLatch(timeout ? 1 : 0);
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		var committed = storedRecommendations(recommendations);

		try {
			assertAttempts(expectedAttempts);
			assertCompletedBatchUnchanged(committed);
		} finally {
			responseGate.countDown();
		}
	}

	static Stream<Arguments> deliveryOutcomes() {
		return Stream.of(
				Arguments.of("503 then acknowledgment", new int[] {503, 204}, false, 2),
				Arguments.of("503 attempt limit", new int[] {503}, false, 3),
				Arguments.of("timeout attempt limit", new int[] {204}, true, 3));
	}

	@ParameterizedTest(name = "restart after delivery exhaustion={0}")
	@ValueSource(booleans = {false, true})
	void restartDeliversPendingRecommendationsWithoutRecalculation(boolean exhaustAttempts) {
		responses = new int[] {exhaustAttempts ? 503 : 204};
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		int initialAttempts = exhaustAttempts ? 3 : 1;
		assertAttempts(initialAttempts);
		var committed = storedRecommendations(recommendations);
		responses = new int[] {204};
		MockPricing.pricingRead = () -> {
			throw new AssertionError("Delivery restart must not recalculate recommendations");
		};

		try (var application = startApplication()) {
			var wakeUps = application.getBean(WakeUpReceiver.class).wakeUps();
			assertAttempts(wakeUps, 1);
			assertEquals(initialAttempts, WAKE_UPS.size());
			assertCompletedBatchUnchanged(committed, wakeUps);
		}
	}

	@Test
	void startupDoesNotNotifyAnEmptyStore() {
		try (var application = startApplication()) {
			assertAttempts(application.getBean(WakeUpReceiver.class).wakeUps(), 0);
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
			assertAttempts(application.getBean(WakeUpReceiver.class).wakeUps(), 0);
			assertEquals(committed, storedRecommendations(recommendations));
			assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
		}
	}

	@Test
	void startupWakesExistingRecommendationsBeforeDrainingAccounts() {
		addAccount(UUID.randomUUID());
		service.processPendingAccounts();
		assertAttempts(1);
		addAccount(UUID.randomUUID());
		List<WakeUp> wakeUps = new CopyOnWriteArrayList<>();
		var receiver = new WakeUpReceiver(startTransactionProcessor(wakeUps), wakeUps);
		MockPricing.pricingRead = () -> await().atMost(Duration.ofSeconds(5))
				.until(() -> !wakeUps.isEmpty());

		try (var application = startApplication(receiver)) {
			assertAttempts(wakeUps, 2);
			assertEquals(1, wakeUps.getFirst().rows().getFirst().size());
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
			var wakeUps = application.getBean(WakeUpReceiver.class).wakeUps();
			assertAttempts(wakeUps, 2);
			long interval = wakeUps.get(1).receivedAt() - wakeUps.getFirst().receivedAt();
			assertTrue(interval >= Duration.ofMillis(150).toNanos());
			assertTrue(interval < Duration.ofMillis(750).toNanos());
			assertCompletedBatchUnchanged(committed, wakeUps);
		}
	}

}
