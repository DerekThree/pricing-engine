package com.pricing.engine.recommendationStore;

import java.util.List;

import org.springframework.stereotype.Component;

import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.AccountResult;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.FeeResult;

@Component
public class RecommendationMapper {

	public RecommendationOutboxEntity toEntity(RuleEngineBatchResult result) {
		RecommendationOutboxEntity batch = RecommendationOutboxEntity.builder()
				.batchId(result.batchId()).processed(false).build();
		batch.setAccounts(result.accounts().stream().map(account -> toAccount(account, batch))
				.toList());
		return batch;
	}

	private AccountRecommendationEntity toAccount(
			AccountResult result, RecommendationOutboxEntity batch) {
		AccountRecommendationEntity account = AccountRecommendationEntity.builder()
				.batch(batch).accountNumber(result.accountNumber()).status(result.status())
				.pricingPlanCode(result.pricingPlanCode()).build();
		if (result.fees() != null) {
			account.setFees(result.fees().stream().map(fee -> toFee(fee, account)).toList());
		}
		return account;
	}

	private FeeRequestRecommendationEntity toFee(
			FeeResult result, AccountRecommendationEntity account) {
		return FeeRequestRecommendationEntity.builder()
				.account(account).feeRequestId(result.feeRequestId()).status(result.status())
				.amount(result.amount())
				.reasons(result.reasons() == null ? List.of() : result.reasons()).build();
	}
}
