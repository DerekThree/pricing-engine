package com.pricing.engine.recommendationStore;

import java.util.ArrayList;
import java.util.List;

import com.pricing.engine.ruleEngine.RuleEngineBatchResult.AccountStatus;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@Builder
@Getter
@Setter
@NoArgsConstructor
@Table(name = "account_recommendations")
public class AccountRecommendationEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "batch_id", nullable = false)
	private RecommendationOutboxEntity batch;

	@Column(name = "account_number", nullable = false, length = 25)
	private String accountNumber;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 30)
	private AccountStatus status;

	@Column(name = "pricing_plan_code", length = 25)
	private String pricingPlanCode;

	@Default
	@OneToMany(mappedBy = "account", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<FeeRequestRecommendationEntity> fees = new ArrayList<>();
}
