package com.pricing.engine.recommendationStore;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class RecommendationMapperTests {

	@Test
	void preservesCompleteAccountAndFeeRequestOutcomesWithOneCorrelatedOutbox() {
		UUID batchId = UUID.fromString("b566f38a-0d68-4adb-8a86-84b0d945d645");
		var result = new RuleEngineBatchResult(batchId, List.of(
				new AccountResult("A1", AccountStatus.OK, "PLAN1", List.of(
						new FeeResult(41L, FeeStatus.OK, Decision.CHARGED,
								new BigDecimal("12.50"), null),
						new FeeResult(42L, FeeStatus.OK, Decision.WAIVED,
								BigDecimal.ZERO, List.of("VIP", "LOYAL")),
						new FeeResult(43L, FeeStatus.FEE_NOT_FOUND, null, null, null))),
				new AccountResult("A2", AccountStatus.PLAN_NOT_FOUND, null, null)));

		var stored = new RecommendationMapper().toEntity(result);

		assertEquals(batchId, stored.getBatchId());
		assertFalse(stored.isProcessed());
		var account = stored.getAccounts().getFirst();
		assertSame(stored, account.getBatch());
		assertEquals("A1", account.getAccountNumber());
		assertEquals(AccountStatus.OK, account.getStatus());
		assertEquals("PLAN1", account.getPricingPlanCode());
		assertEquals(List.of(41L, 42L, 43L), account.getFees().stream()
				.map(FeeRequestRecommendationEntity::getFeeRequestId).toList());
		assertEquals(new BigDecimal("12.50"), account.getFees().getFirst().getAmount());
		assertEquals(Decision.CHARGED, account.getFees().getFirst().getDecision());
		assertEquals(Decision.WAIVED, account.getFees().get(1).getDecision());
		assertEquals(List.of("VIP", "LOYAL"), account.getFees().get(1).getReasons());
		assertEquals(FeeStatus.FEE_NOT_FOUND, account.getFees().getLast().getStatus());
		assertEquals(AccountStatus.PLAN_NOT_FOUND, stored.getAccounts().getLast().getStatus());
	}
}
