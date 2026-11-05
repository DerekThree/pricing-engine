package com.pricing.engine.pricingStore;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
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
@Table(name = "eligibility_reasons")
public class EligibilityReasonEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@EqualsAndHashCode.Include
	private Long id;

	@Column(name = "reason_code", nullable = false, unique = true, length = 25)
	private String reasonCode;

	@Column(name = "reason_name", nullable = false, length = 100)
	private String reasonName;

	@Builder.Default
	@OneToMany(mappedBy = "reason", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<EligibilityReasonConditionEntity> conditions = new ArrayList<>();

	@Column(name = "updated_on", nullable = false)
	private OffsetDateTime updatedOn;

	@Column(name = "updated_by", nullable = false, length = 100)
	private String updatedBy;

	public List<ProductType> getProductTypes() {
		EnumSet<ProductType> productTypes = EnumSet.allOf(ProductType.class);
		conditions.forEach(condition ->
				productTypes.retainAll(condition.getAttribute().getProductTypes()));
		return productTypes.stream().toList();
	}
}
