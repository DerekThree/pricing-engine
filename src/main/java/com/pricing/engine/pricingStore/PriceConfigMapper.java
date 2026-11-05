package com.pricing.engine.pricingStore;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.pricing.engine.pricingStore.BranchEntity;
import com.pricing.engine.pricingStore.EligibilityReasonConditionEntity;
import com.pricing.engine.pricingStore.EligibilityReasonEntity;
import com.pricing.engine.pricingStore.PricingPlanEntity;
import com.pricing.engine.pricingStore.PricingPlanFeeEntity;
import com.pricing.engine.pricingStore.RegionEntity;
import com.pricing.engine.ruleEngine.PriceConfig;
import com.pricing.engine.ruleEngine.PriceConfig.AttributeDefinition;
import com.pricing.engine.ruleEngine.PriceConfig.AttributeType;
import com.pricing.engine.ruleEngine.PriceConfig.Branch;
import com.pricing.engine.ruleEngine.PriceConfig.EligibilityCondition;
import com.pricing.engine.ruleEngine.PriceConfig.EligibilityReason;
import com.pricing.engine.ruleEngine.PriceConfig.FeeType;
import com.pricing.engine.ruleEngine.PriceConfig.Plan;
import com.pricing.engine.ruleEngine.PriceConfig.PlanFee;
import com.pricing.engine.ruleEngine.PriceConfig.Region;

import org.springframework.stereotype.Component;

@Component
class PriceConfigMapper {

	PriceConfig toPriceConfig(
			List<BranchEntity> branches,
			List<RegionEntity> regions,
			List<PricingPlanEntity> pricingPlans) {
		Map<Long, String> branchCodesById = branches.stream()
				.collect(Collectors.toMap(BranchEntity::getId, BranchEntity::getBranchCode));
		return new PriceConfig(
				branches.stream().collect(Collectors.toMap(
						BranchEntity::getBranchCode,
						this::toBranch)),
				regions.stream().map(region -> toRegion(region, branchCodesById)).toList(),
				pricingPlans.stream().map(this::toPlan).toList());
	}

	private Branch toBranch(BranchEntity branch) {
		return new Branch(branch.getBranchCode(), branch.getState(), branch.getZipCode());
	}

	private Region toRegion(RegionEntity region, Map<Long, String> branchCodesById) {
		return new Region(
				region.getRegionCode(),
				region.getBranches().stream().map(branchCodesById::get).collect(Collectors.toSet()),
				region.getZipCodes().stream().collect(Collectors.toSet()),
				region.getStates().stream().collect(Collectors.toSet()));
	}

	private Plan toPlan(PricingPlanEntity pricingPlan) {
		return new Plan(
				pricingPlan.getPlanCode(),
				pricingPlan.getProduct().getProductCode(),
				pricingPlan.getRegion().getRegionCode(),
				pricingPlan.getActiveFrom(),
				pricingPlan.getActiveThrough(),
				pricingPlan.getFees().stream().map(this::toPlanFee).toList());
	}

	private PlanFee toPlanFee(PricingPlanFeeEntity pricingPlanFee) {
		return new PlanFee(
				pricingPlanFee.getFee().getFeeCode(),
				FeeType.valueOf(pricingPlanFee.getFee().getFeeType().name()),
				pricingPlanFee.getAmount(),
				pricingPlanFee.getReasons().stream().map(this::toEligibilityReason).toList());
	}

	private EligibilityReason toEligibilityReason(EligibilityReasonEntity reason) {
		return new EligibilityReason(
				reason.getReasonCode(),
				reason.getConditions().stream()
				.map(this::toEligibilityCondition)
				.toList());
	}

	private EligibilityCondition toEligibilityCondition(EligibilityReasonConditionEntity condition) {
		return new EligibilityCondition(
				toAttributeDefinition(condition),
				condition.getOperator(),
				condition.getAttributeValue());
	}

	private AttributeDefinition toAttributeDefinition(EligibilityReasonConditionEntity condition) {
		return new AttributeDefinition(
				condition.getAttribute().getAttributeCode(),
				AttributeType.valueOf(condition.getAttribute().getAttributeType().name()));
	}
}
