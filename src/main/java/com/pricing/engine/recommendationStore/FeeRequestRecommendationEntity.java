package com.pricing.engine.recommendationStore;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.pricing.engine.ruleEngine.RuleEngineBatchResult.Decision;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.FeeStatus;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
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
@Table(name = "fee_request_recommendations")
public class FeeRequestRecommendationEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "account_id", nullable = false)
	private AccountRecommendationEntity account;

	@Column(name = "fee_request_id", nullable = false)
	private Long feeRequestId;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 35)
	private FeeStatus status;

	@Enumerated(EnumType.STRING)
	@Column(length = 10)
	private Decision decision;

	@Column(columnDefinition = "numeric")
	private BigDecimal amount;

	@Default
	@ElementCollection
	@CollectionTable(name = "recommendation_eligibility_reasons",
			joinColumns = @JoinColumn(name = "fee_recommendation_id"))
	@Column(name = "reason_code", nullable = false, length = 25)
	private List<String> reasons = new ArrayList<>();
}
