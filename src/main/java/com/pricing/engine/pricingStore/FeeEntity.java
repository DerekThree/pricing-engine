package com.pricing.engine.pricingStore;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import com.pricing.engine.ruleEngine.PriceConfig.FeeType;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@Builder
@Getter
@Setter
@NoArgsConstructor
@Table(name = "fees")
public class FeeEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "fee_code", nullable = false, unique = true, length = 25)
	private String feeCode;

	@Column(name = "fee_name", nullable = false, length = 100)
	private String feeName;

	@Enumerated(EnumType.STRING)
	@Column(name = "fee_type", nullable = false, length = 20)
	private FeeType feeType;

	@ElementCollection(fetch = FetchType.EAGER)
	@CollectionTable(name = "fee_product_types", joinColumns = @JoinColumn(name = "fee_id"))
	@Enumerated(EnumType.STRING)
	@Column(name = "product_type", nullable = false, length = 20)
	@Default
	private List<ProductType> productTypes = new ArrayList<>();

	@Column(name = "updated_on", nullable = false)
	private OffsetDateTime updatedOn;

	@Column(name = "updated_by", nullable = false, length = 100)
	private String updatedBy;
}
