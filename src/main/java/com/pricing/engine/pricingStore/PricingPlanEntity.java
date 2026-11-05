package com.pricing.engine.pricingStore;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
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
@Setter
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Table(name = "pricing_plans")
public class PricingPlanEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@EqualsAndHashCode.Include
	private Long id;

	@Column(name = "plan_code", nullable = false, unique = true, length = 25)
	private String planCode;

	@Column(name = "plan_name", nullable = false, length = 100)
	private String planName;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "product_id", nullable = false)
	private ProductEntity product;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "region_id", nullable = false)
	private RegionEntity region;

	@Column(name = "active_from", nullable = false)
	private LocalDate activeFrom;

	@Column(name = "active_through", nullable = false)
	private LocalDate activeThrough;

	@Default
	@OneToMany(mappedBy = "pricingPlan", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<PricingPlanFeeEntity> fees = new ArrayList<>();

	@Column(name = "updated_on", nullable = false)
	private OffsetDateTime updatedOn;

	@Column(name = "updated_by", nullable = false, length = 100)
	private String updatedBy;
}
