package com.pricing.engine.accountStore;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import com.pricing.engine.utils.PostgresFixture;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AccountRepositoryPostgresTests extends PostgresFixture {

	@Test
	void readsCanonicalAccountStoreDatesScalarAttributesAndFeeRequests() {
		UUID batchId = UUID.randomUUID();
		long accountId = addAccount(batchId);
		accounts.update("""
				insert into accounts(batch_id, account_number, product_code, branch_code, pricing_date)
				values (?, 'A2', 'CHECKING', 'NY', '2026-02-15')
				""", batchId);
		for (var attribute : Map.of("VIP", "true", "BALANCE", "12.50", "TIER", "\"GOLD\"")
				.entrySet()) {
			accounts.update("""
					insert into account_attributes(account_id, code, value)
					values (?, ?, cast(? as jsonb))
					""", accountId, attribute.getKey(), attribute.getValue());
		}

		RuleEngineBatch batch = accountRepository.load(batchId);

		assertEquals(batchId, batch.batchId());
		assertEquals(Set.of(LocalDate.of(2026, 1, 15), LocalDate.of(2026, 2, 15)),
				batch.accounts().stream().map(RuleEngineBatch.Account::pricingDate)
						.collect(Collectors.toSet()));
		var first = batch.accounts().stream().filter(a -> a.accountNumber().equals("A1"))
				.findFirst().orElseThrow();
		var attributes = first.attributes().stream().collect(Collectors.toMap(
				RuleEngineBatch.AccountAttribute::code, RuleEngineBatch.AccountAttribute::value));
		assertEquals(true, attributes.get("VIP"));
		assertEquals("GOLD", attributes.get("TIER"));
		assertEquals(0, ((BigDecimal) attributes.get("BALANCE"))
				.compareTo(new BigDecimal("12.50")));
		assertEquals(42L, first.fees().getFirst().feeRequestId());
		assertEquals("SERVICE", first.fees().getFirst().code());
		assertEquals(0, first.fees().getFirst().transactionAmount()
				.compareTo(new BigDecimal("25.00")));
	}

}
