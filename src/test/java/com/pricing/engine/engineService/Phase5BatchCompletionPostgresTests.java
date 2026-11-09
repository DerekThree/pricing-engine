package com.pricing.engine.engineService;

import java.util.UUID;
import com.pricing.engine.EngineService;
import com.pricing.engine.utils.MockPricing;
import com.pricing.engine.utils.PostgresFixture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataIntegrityViolationException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase5BatchCompletionPostgresTests extends PostgresFixture {

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
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from fee_request_recommendations", Integer.class));
			assertEquals(storedRecommendations(recommendations), WAKE_UPS.getFirst().rows());
			assertFalse(accounts.queryForObject(
					"select processed from batch_outbox", Boolean.class));
		} finally {
			accounts.execute("alter table batch_outbox drop constraint reject_test_completion");
		}
		recommendations.update("update recommendation_outbox set processed = ?", processed);
		var committed = storedRecommendations(recommendations);
		MockPricing.januaryAmount = "9.99";
		MockPricing.pricingRead = () -> {
			throw new AssertionError("Committed output must not be recalculated on restart");
		};

		try (var application = startApplication()) {
			assertAttempts(application.getBean(WakeUpReceiver.class).wakeUps(), processed ? 0 : 1);
			assertAttempts(1);
			assertEquals(committed, storedRecommendations(recommendations));
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox", Boolean.class));
			assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
			application.getBean(EngineService.class).processPendingAccounts();
			assertEquals(committed, storedRecommendations(recommendations));
		}
	}

	@Test
	void lostCommitAcknowledgementPreservesCommittedOutput() throws Exception {
		assertLostCommitAcknowledgementPreservesCommittedOutput("account");
	}

}
