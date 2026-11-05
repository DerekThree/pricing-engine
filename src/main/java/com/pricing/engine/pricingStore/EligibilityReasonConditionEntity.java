package com.pricing.engine.pricingStore;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@Builder
@Getter
@NoArgsConstructor
@Setter
@Table(name = "eligibility_reason_conditions")
public class EligibilityReasonConditionEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "reason_id", nullable = false)
	private EligibilityReasonEntity reason;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "attribute_id", nullable = false)
	private AccountAttributeEntity attribute;

	@Column(name = "operator", nullable = false, length = 2)
	private String operator;

	@Column(name = "attribute_value", nullable = false)
	private String attributeValue;
}
