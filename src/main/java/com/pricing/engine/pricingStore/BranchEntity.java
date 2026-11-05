package com.pricing.engine.pricingStore;

import java.time.OffsetDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
@Table(name = "branches")
public class BranchEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "branch_code", nullable = false, unique = true, length = 25)
	private String branchCode;

	@Column(name = "branch_name", nullable = false, length = 100)
	private String branchName;

	@Column(nullable = false, length = 2)
	private String state;

	@Column(name = "zip_code", nullable = false, length = 5)
	private String zipCode;

	@Column(name = "updated_on", nullable = false)
	private OffsetDateTime updatedOn;

	@Column(name = "updated_by", nullable = false, length = 100)
	private String updatedBy;
}
