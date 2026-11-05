package com.pricing.engine.ruleEngine;

public class PricingAccessException extends RuntimeException {

	public PricingAccessException(Throwable cause) {
		super("Pricing Configuration is unavailable", cause);
	}
}
