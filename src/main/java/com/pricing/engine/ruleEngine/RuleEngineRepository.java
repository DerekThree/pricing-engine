package com.pricing.engine.ruleEngine;

import java.time.LocalDate;
import java.util.Set;

public interface RuleEngineRepository {

	Pricing load(Set<LocalDate> pricingDates);
}
