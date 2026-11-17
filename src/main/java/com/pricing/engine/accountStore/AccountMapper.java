package com.pricing.engine.accountStore;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.pricing.engine.ruleEngine.RuleEngineAccountBatch;

@Component
public class AccountMapper {

	public RuleEngineAccountBatch toRuleEngineAccountBatch(
			UUID batchId, List<AccountEntity> accounts) {
		return new RuleEngineAccountBatch(batchId, accounts.stream().map(account ->
				new RuleEngineAccountBatch.Account(
						account.getAccountNumber(), account.getProductCode(), account.getBranchCode(),
						account.getPricingDate(),
						account.getAttributes().stream().map(attribute ->
								new RuleEngineAccountBatch.AccountAttribute(attribute.getCode(),
										attribute.getValue())).toList(),
						account.getFees().stream().map(fee ->
								new RuleEngineAccountBatch.FeeRequest(fee.getFeeRequestId(), fee.getCode(),
										fee.getTransactionAmount())).toList())).toList());
	}
}
