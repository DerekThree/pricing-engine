package com.pricing.engine.pricingStore;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Embeddable
@AllArgsConstructor
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Setter
public class PricingPlanFeeId implements Serializable {

	@Column(name = "pricing_plan_id", nullable = false)
	private Long pricingPlanId;

	@Column(name = "fee_id", nullable = false)
	private Long feeId;
}
