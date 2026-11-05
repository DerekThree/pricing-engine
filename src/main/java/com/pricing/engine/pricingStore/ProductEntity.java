package com.pricing.engine.pricingStore;

import java.time.OffsetDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
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
@Setter
@NoArgsConstructor
@Table(name = "products")
public class ProductEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "product_code", nullable = false, unique = true, length = 25)
	private String productCode;

	@Column(name = "product_name", nullable = false, length = 100)
	private String productName;

	@Enumerated(EnumType.STRING)
	@Column(name = "product_type", nullable = false, length = 20)
	private ProductType productType;

	@Column(name = "updated_on", nullable = false)
	private OffsetDateTime updatedOn;

	@Column(name = "updated_by", nullable = false, length = 100)
	private String updatedBy;
}
