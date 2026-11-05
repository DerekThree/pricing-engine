package com.pricing.engine.recommendationStore;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.pricing.engine.ruleEngine.RuleEngineBatchResult;

@Repository
public class RecommendationRepository {

	@PersistenceContext(unitName = "recommendations")
	private EntityManager entityManager;

	private final RecommendationMapper mapper;

	public RecommendationRepository(RecommendationMapper mapper) {
		this.mapper = mapper;
	}

	@Transactional(transactionManager = "recommendationTransactionManager", readOnly = true)
	public boolean existsByProcessedFalse() {
		return !entityManager.createQuery("""
				select 1 from RecommendationOutboxEntity batch where batch.processed = false
				""", Integer.class)
				.setMaxResults(1)
				.getResultList().isEmpty();
	}

	@Transactional(transactionManager = "recommendationTransactionManager")
	public void save(RuleEngineBatchResult result) {
		entityManager.persist(mapper.toEntity(result));
	}
}
