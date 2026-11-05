package com.pricing.engine.pricingStore;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OrderBy;
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
@Table(name = "regions")
public class RegionEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "region_code", nullable = false, unique = true, length = 25)
	private String regionCode;

	@Column(name = "region_name", nullable = false, length = 100)
	private String regionName;

	@ElementCollection(fetch = FetchType.LAZY)
	@CollectionTable(name = "region_states", joinColumns = @JoinColumn(name = "region_id"))
	@Column(name = "state_code", nullable = false, length = 2)
	@OrderBy("value asc")
	@Default
	private List<String> states = new ArrayList<>();

	@ElementCollection(fetch = FetchType.LAZY)
	@CollectionTable(name = "region_zip_codes", joinColumns = @JoinColumn(name = "region_id"))
	@Column(name = "zip_code", nullable = false, length = 5)
	@OrderBy("value asc")
	@Default
	private List<String> zipCodes = new ArrayList<>();

	@ElementCollection(fetch = FetchType.LAZY)
	@CollectionTable(name = "region_branches", joinColumns = @JoinColumn(name = "region_id"))
	@Column(name = "branch_id", nullable = false)
	@OrderBy("value asc")
	@Default
	private List<Long> branches = new ArrayList<>();

	@Column(name = "updated_on", nullable = false)
	private OffsetDateTime updatedOn;

	@Column(name = "updated_by", nullable = false, length = 100)
	private String updatedBy;
}
