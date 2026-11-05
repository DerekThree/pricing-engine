package com.pricing.engine.recommendationStore;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
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
@Table(name = "recommendation_batches")
public class RecommendationBatchEntity {

	@Id
	@Column(name = "batch_id", nullable = false)
	private UUID batchId;

	@Default
	@OneToMany(mappedBy = "batch", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<AccountRecommendationEntity> accounts = new ArrayList<>();

	@OneToOne(mappedBy = "batch", cascade = CascadeType.ALL, orphanRemoval = true)
	private RecommendationOutboxEntity outbox;
}
