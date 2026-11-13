package com.pricing.engine.ruleEngine;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record RuleEngineBatchResult(UUID batchId, List<AccountResult> accounts) {

	public RuleEngineBatchResult {
		accounts = List.copyOf(accounts);
	}

	public record AccountResult(
			String accountNumber,
			AccountStatus status,
			String pricingPlanCode,
			List<FeeResult> fees) {

		public AccountResult {
			fees = fees == null ? null : List.copyOf(fees);
		}
	}

	public record FeeResult(
			Long feeRequestId,
			FeeStatus status,
			BigDecimal amount,
			List<String> reasons) {

		public FeeResult {
			reasons = reasons == null ? null : List.copyOf(reasons);
		}
	}

	public enum AccountStatus {
		OK,
		BRANCH_NOT_FOUND,
		REGION_NOT_FOUND,
		PLAN_NOT_FOUND,
		DUPLICATE_ATTRIBUTE,
		INVALID_ATTRIBUTE_TYPE,
		MISSING_ATTRIBUTE,
		ERROR
	}

	public enum FeeStatus {
		CHARGED,
		WAIVED,
		FEE_NOT_FOUND,
		MISSING_TRANSACTION,
		INVALID_TRANSACTION_AMOUNT,
		INVALID_ELIGIBILITY_CONDITION,
		ERROR
	}
}
