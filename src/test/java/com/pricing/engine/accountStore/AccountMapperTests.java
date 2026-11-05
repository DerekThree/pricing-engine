package com.pricing.engine.accountStore;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AccountMapperTests {

	@Test
	void preservesAccountDatesScalarTypesAndFeeRequestIdentifiers() {
		UUID batchId = UUID.fromString("b566f38a-0d68-4adb-8a86-84b0d945d645");
		AccountEntity first = AccountEntity.builder()
				.accountNumber("A1").productCode("CHECKING").branchCode("NY")
				.pricingDate(LocalDate.of(2026, 1, 1))
				.attributes(List.of(
						AccountAttributeEntity.builder().code("VIP").value(true).build(),
						AccountAttributeEntity.builder().code("BALANCE")
								.value(new BigDecimal("120.50")).build(),
						AccountAttributeEntity.builder().code("TIER").value("GOLD").build()))
				.fees(List.of(FeeRequestEntity.builder().feeRequestId(42L).code("SERVICE")
						.transactionAmount(new BigDecimal("25.00")).build()))
				.build();
		AccountEntity second = AccountEntity.builder()
				.accountNumber("A2").productCode("CHECKING").branchCode("NY")
				.pricingDate(LocalDate.of(2026, 2, 1)).build();

		var result = new AccountMapper().toRuleEngineBatch(batchId, List.of(first, second));

		assertEquals(batchId, result.batchId());
		assertEquals(LocalDate.of(2026, 1, 1), result.accounts().getFirst().pricingDate());
		assertEquals(LocalDate.of(2026, 2, 1), result.accounts().getLast().pricingDate());
		assertEquals(List.of(true, new BigDecimal("120.50"), "GOLD"),
				result.accounts().getFirst().attributes().stream().map(a -> a.value()).toList());
		assertEquals(42L, result.accounts().getFirst().fees().getFirst().feeRequestId());
		assertEquals(new BigDecimal("25.00"),
				result.accounts().getFirst().fees().getFirst().transactionAmount());
	}
}
