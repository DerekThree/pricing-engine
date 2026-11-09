package com.pricing.engine.utils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import com.pricing.engine.pricingStore.PricingRepository;
import com.pricing.engine.ruleEngine.Pricing;
import com.pricing.engine.ruleEngine.Pricing.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestConfiguration(proxyBeanMethods = false)
public class MockPricing {

	public static final List<Long> COMPLETED_BEFORE_EVALUATION = new CopyOnWriteArrayList<>();
	public static volatile Runnable pricingRead = () -> {};
	public static volatile String januaryAmount = "2.50";

	@Bean
	@Primary
	PricingRepository testPricingRepository(
			@Qualifier("accountDataSource") DataSource accountStore) {
		PricingRepository repository = mock(PricingRepository.class);
		when(repository.load(anySet())).thenAnswer(invocation -> {
			COMPLETED_BEFORE_EVALUATION.add(new JdbcTemplate(accountStore).queryForObject(
					"select count(*) from batch_outbox where processed = true", Long.class));
			pricingRead.run();
			return new Pricing(Map.of("NY", new Branch("NY", "NY", "10001")),
					List.of(new Region("EAST", Set.of("NY"), Set.of(), Set.of())), List.of(
							plan("JANPLAN", 1, januaryAmount), plan("FEBPLAN", 2, "3.50")));
		});
		return repository;
	}

	private Plan plan(String code, int month, String amount) {
		LocalDate start = LocalDate.of(2026, month, 1);
		return new Plan(code, "CHECKING", "EAST", start, start.withDayOfMonth(
				start.lengthOfMonth()), List.of(new PlanFee("SERVICE", FeeType.FLAT,
				new BigDecimal(amount), List.of())));
	}
}
