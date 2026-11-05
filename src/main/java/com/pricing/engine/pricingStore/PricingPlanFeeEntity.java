package com.pricing.engine.pricingStore;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@Builder
@Getter
@NoArgsConstructor
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Table(name = "pricing_plan_fees")
public class PricingPlanFeeEntity {

	@EmbeddedId
	@EqualsAndHashCode.Include
	private PricingPlanFeeId id;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@MapsId("pricingPlanId")
	@JoinColumn(name = "pricing_plan_id", nullable = false)
	private PricingPlanEntity pricingPlan;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@MapsId("feeId")
	@JoinColumn(name = "fee_id", nullable = false)
	private FeeEntity fee;

	@Column(name = "amount", nullable = false, precision = 19, scale = 4)
	private BigDecimal amount;

	@Default
	@ManyToMany
	@JoinTable(
			name = "pricing_plan_fee_reasons",
			joinColumns = {
					@JoinColumn(name = "pricing_plan_id", referencedColumnName = "pricing_plan_id"),
					@JoinColumn(name = "fee_id", referencedColumnName = "fee_id")
			},
			inverseJoinColumns = @JoinColumn(name = "reason_id", referencedColumnName = "id")
	)
	private List<EligibilityReasonEntity> reasons = new ArrayList<>();
}
