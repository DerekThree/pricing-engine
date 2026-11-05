package com.pricing.engine.pricingStore;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.pricing.engine.pricingStore.BranchEntity;
import com.pricing.engine.pricingStore.EligibilityReasonEntity;
import com.pricing.engine.pricingStore.PricingPlanEntity;
import com.pricing.engine.pricingStore.PricingPlanFeeEntity;
import com.pricing.engine.pricingStore.RegionEntity;
import com.pricing.engine.ruleEngine.PriceConfig;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class PricingRepository {

	private final EntityManager entityManager;
	private final PriceConfigMapper mapper;

	public PricingRepository(EntityManager entityManager, PriceConfigMapper mapper) {
		this.entityManager = entityManager;
		this.mapper = mapper;
	}

	@Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
	public PriceConfig load(Set<LocalDate> pricingDates) {
		List<BranchEntity> branches = entityManager
				.createQuery("select branch from BranchEntity branch", BranchEntity.class)
				.getResultList();
		List<RegionEntity> regions = entityManager
				.createQuery("select region from RegionEntity region", RegionEntity.class)
				.getResultList();
		loadRegionMemberships(regions);
		if (pricingDates.isEmpty()) {
			return mapper.toPriceConfig(branches, regions, List.of());
		}

		List<LocalDate> submittedPricingDates = pricingDates.stream().toList();
		String pricingDatePredicate = IntStream.range(0, submittedPricingDates.size())
				.mapToObj(index -> "(pricingPlan.activeFrom <= :pricingDate" + index
						+ " and pricingPlan.activeThrough >= :pricingDate" + index + ")")
				.collect(Collectors.joining(" or "));
		TypedQuery<PricingPlanEntity> pricingPlanQuery = entityManager.createQuery("""
				select distinct pricingPlan
				from PricingPlanEntity pricingPlan
				join fetch pricingPlan.product
				join fetch pricingPlan.region
				left join fetch pricingPlan.fees pricingPlanFee
				left join fetch pricingPlanFee.fee
				where %s
				""".formatted(pricingDatePredicate), PricingPlanEntity.class);
		IntStream.range(0, submittedPricingDates.size())
				.forEach(index -> pricingPlanQuery.setParameter(
						"pricingDate" + index, submittedPricingDates.get(index)));
		List<PricingPlanEntity> pricingPlans = pricingPlanQuery.getResultList();
		loadReasons(pricingPlans);
		return mapper.toPriceConfig(branches, regions, pricingPlans);
	}

	private void loadRegionMemberships(List<RegionEntity> regions) {
		if (regions.isEmpty()) {
			return;
		}
		entityManager.createQuery("""
				select distinct region
				from RegionEntity region
				left join fetch region.states
				where region in :regions
				""", RegionEntity.class)
				.setParameter("regions", regions)
				.getResultList();
		entityManager.createQuery("""
				select distinct region
				from RegionEntity region
				left join fetch region.zipCodes
				where region in :regions
				""", RegionEntity.class)
				.setParameter("regions", regions)
				.getResultList();
		entityManager.createQuery("""
				select distinct region
				from RegionEntity region
				left join fetch region.branches
				where region in :regions
				""", RegionEntity.class)
				.setParameter("regions", regions)
				.getResultList();
	}

	private void loadReasons(List<PricingPlanEntity> pricingPlans) {
		List<PricingPlanFeeEntity> pricingPlanFees = pricingPlans.stream()
				.flatMap(pricingPlan -> pricingPlan.getFees().stream())
				.toList();
		if (pricingPlanFees.isEmpty()) {
			return;
		}
		entityManager.createQuery("""
				select distinct pricingPlanFee
				from PricingPlanFeeEntity pricingPlanFee
				left join fetch pricingPlanFee.reasons
				where pricingPlanFee in :pricingPlanFees
				""", PricingPlanFeeEntity.class)
				.setParameter("pricingPlanFees", pricingPlanFees)
				.getResultList();
		List<EligibilityReasonEntity> reasons = pricingPlanFees.stream()
				.flatMap(pricingPlanFee -> pricingPlanFee.getReasons().stream())
				.distinct()
				.toList();
		if (reasons.isEmpty()) {
			return;
		}
		entityManager.createQuery("""
				select distinct reason
				from EligibilityReasonEntity reason
				left join fetch reason.conditions condition
				left join fetch condition.attribute
				where reason in :reasons
				""", EligibilityReasonEntity.class)
				.setParameter("reasons", reasons)
				.getResultList();
	}
}
