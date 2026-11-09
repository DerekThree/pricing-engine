package com.pricing.engine.engineService;

import java.util.UUID;
import com.pricing.engine.utils.PostgresFixture;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase6NotificationDeliveryPostgresTests extends PostgresFixture {

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

}
