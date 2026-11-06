package com.pricing.engine.ruleEngine;

import java.time.LocalDate;
import java.util.Set;

public interface RuleEngineRepository {

	PriceConfig load(Set<LocalDate> pricingDates);
}
