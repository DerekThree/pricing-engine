package com.pricing.engine.accountStore;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.pricing.engine.ruleEngine.RuleEngineBatch;

@Component
public class AccountMapper {

	public RuleEngineBatch toRuleEngineBatch(UUID batchId, List<AccountEntity> accounts) {
		return new RuleEngineBatch(batchId, accounts.stream().map(account ->
				new RuleEngineBatch.Account(
						account.getAccountNumber(), account.getProductCode(), account.getBranchCode(),
						account.getPricingDate(),
						account.getAttributes().stream().map(attribute ->
								new RuleEngineBatch.AccountAttribute(attribute.getCode(),
										attribute.getValue())).toList(),
						account.getFees().stream().map(fee ->
								new RuleEngineBatch.FeeRequest(fee.getFeeRequestId(), fee.getCode(),
										fee.getTransactionAmount())).toList())).toList());
	}
}
