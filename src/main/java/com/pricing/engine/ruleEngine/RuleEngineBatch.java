package com.pricing.engine.ruleEngine;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public record RuleEngineBatch(UUID batchId, List<Account> accounts) {

	public RuleEngineBatch {
		accounts = List.copyOf(accounts);
	}

	public record Account(
			String accountNumber,
			String productCode,
			String branchCode,
			LocalDate pricingDate,
			List<AccountAttribute> attributes,
			List<FeeRequest> fees) {

		public Account {
			attributes = List.copyOf(attributes);
			fees = List.copyOf(fees);
		}
	}

	public record AccountAttribute(String code, Object value) {
	}

	public record FeeRequest(Long feeRequestId, String code, BigDecimal transactionAmount) {
	}
}
