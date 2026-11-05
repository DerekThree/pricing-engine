package com.pricing.engine.ruleEngine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.pricing.engine.pricingStore.PricingRepository;
import com.pricing.engine.ruleEngine.PriceConfig.AttributeDefinition;
import com.pricing.engine.ruleEngine.PriceConfig.Branch;
import com.pricing.engine.ruleEngine.PriceConfig.EligibilityCondition;
import com.pricing.engine.ruleEngine.PriceConfig.EligibilityReason;
import com.pricing.engine.ruleEngine.PriceConfig.FeeType;
import com.pricing.engine.ruleEngine.PriceConfig.Plan;
import com.pricing.engine.ruleEngine.PriceConfig.PlanFee;
import com.pricing.engine.ruleEngine.PriceConfig.Region;
import com.pricing.engine.ruleEngine.RuleEngineBatch.*;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;

import org.springframework.stereotype.Component;

@Component
public class RuleEngine {

	private final PricingRepository configRepository;

	public RuleEngine(PricingRepository configRepository) {
		this.configRepository = configRepository;
	}

	public RuleEngineBatchResult price(RuleEngineBatch batch) {
		Set<LocalDate> pricingDates = batch.accounts().stream()
				.map(Account::pricingDate)
				.collect(Collectors.toUnmodifiableSet());
		PriceConfig config;
		try {
			config = configRepository.load(pricingDates);
		} catch (RuntimeException exception) {
			throw new PricingAccessException(exception);
		}
		return new RuleEngineBatchResult(batch.batchId(), batch.accounts().stream()
				.map(account -> price(account, config))
				.toList());
	}

	private AccountResult price(Account account, PriceConfig config) {
		try {
			Branch branch = config.branches().get(account.branchCode());
			if (branch == null) {
				return failed(account, AccountStatus.BRANCH_NOT_FOUND);
			}

			List<Region> regionMatches = config.regions().stream()
					.filter(candidate -> candidate.branchCodes().contains(branch.code()))
					.toList();
			if (regionMatches.isEmpty()) {
				regionMatches = config.regions().stream()
						.filter(candidate -> candidate.zipCodes().contains(branch.zipCode()))
						.toList();
			}
			if (regionMatches.isEmpty()) {
				regionMatches = config.regions().stream()
						.filter(candidate -> candidate.states().contains(branch.state()))
						.toList();
			}
			if (regionMatches.isEmpty()) {
				return failed(account, AccountStatus.REGION_NOT_FOUND);
			}
			if (regionMatches.size() > 1) {
				return failed(account, AccountStatus.ERROR);
			}
			Region region = regionMatches.getFirst();
			List<Plan> planMatches = config.plans().stream()
					.filter(candidate -> candidate.productCode().equals(account.productCode()))
					.filter(candidate -> candidate.regionCode().equals(region.code()))
					.filter(candidate -> !account.pricingDate().isBefore(candidate.activeFrom()))
					.filter(candidate -> !account.pricingDate().isAfter(candidate.activeThrough()))
					.toList();
			if (planMatches.isEmpty()) {
				return failed(account, AccountStatus.PLAN_NOT_FOUND);
			}
			if (planMatches.size() > 1) {
				return failed(account, AccountStatus.ERROR);
			}
			Plan plan = planMatches.getFirst();
			Set<String> requestedFeeCodes = account.fees().stream()
					.map(FeeRequest::code)
					.collect(Collectors.toSet());
			Set<AttributeDefinition> requiredAttributes = plan.fees().stream()
					.filter(fee -> requestedFeeCodes.contains(fee.code()))
					.flatMap(fee -> fee.reasons().stream())
					.flatMap(reason -> reason.conditions().stream())
					.map(EligibilityCondition::attribute)
					.collect(Collectors.toSet());
			Set<String> requiredAttributeCodes = requiredAttributes.stream()
					.map(AttributeDefinition::code)
					.collect(Collectors.toSet());
			Set<String> suppliedAttributeCodes = account.attributes().stream()
					.map(RuleEngineBatch.AccountAttribute::code)
					.collect(Collectors.toSet());
			if (requiredAttributeCodes.stream().anyMatch(code -> account.attributes().stream()
							.filter(attribute -> attribute.code().equals(code))
							.count() > 1)) {
				return failed(account, AccountStatus.DUPLICATE_ATTRIBUTE);
			}
			if (requiredAttributes.stream().anyMatch(required -> account.attributes().stream()
					.filter(attribute -> attribute.code().equals(required.code()))
					.findFirst()
					.map(attribute -> !matchesConfiguredType(required, attribute.value()))
					.orElse(false))) {
				return failed(account, AccountStatus.INVALID_ATTRIBUTE_TYPE);
			}
			if (requiredAttributeCodes.stream().anyMatch(
					code -> !suppliedAttributeCodes.contains(code))) {
				return failed(account, AccountStatus.MISSING_ATTRIBUTE);
			}
			List<FeeResult> fees = account.fees().stream()
					.map(request -> price(request, plan, account.attributes()))
					.toList();
			return new AccountResult(account.accountNumber(), AccountStatus.OK, plan.code(), fees);
		} catch (RuntimeException exception) {
			return failed(account, AccountStatus.ERROR);
		}
	}

	private AccountResult failed(Account account, AccountStatus status) {
		return new AccountResult(account.accountNumber(), status, null, null);
	}

	private boolean matchesConfiguredType(AttributeDefinition attribute, Object value) {
		return switch (attribute.type()) {
			case TEXT -> value instanceof String;
			case BOOLEAN -> value instanceof Boolean;
			case DECIMAL -> value instanceof BigDecimal;
			case INTEGER -> value instanceof BigDecimal decimal && decimal.stripTrailingZeros().scale() <= 0;
			case DATE -> value instanceof String date && isIsoLocalDate(date);
		};
	}

	private boolean isIsoLocalDate(String value) {
		try {
			LocalDate.parse(value);
			return true;
		} catch (DateTimeParseException exception) {
			return false;
		}
	}

	private FeeResult price(FeeRequest request, Plan plan, List<AccountAttribute> attributes) {
		try {
			List<PlanFee> matches = plan.fees().stream()
					.filter(candidate -> candidate.code().equals(request.code()))
					.toList();
			if (matches.isEmpty()) {
				return new FeeResult(
						request.feeRequestId(),
						FeeStatus.FEE_NOT_FOUND,
						null,
						null,
						null);
			}
			PlanFee fee = requireOne(matches, "Pricing Plan Fee");
			if (fee.type() == FeeType.PERCENT && request.transactionAmount() == null) {
				return new FeeResult(
						request.feeRequestId(),
						FeeStatus.MISSING_TRANSACTION,
						null,
						null,
						null);
			}
			if (fee.type() == FeeType.PERCENT
					&& (request.transactionAmount().signum() < 0
							|| request.transactionAmount().scale() > 2)) {
				return new FeeResult(
						request.feeRequestId(),
						FeeStatus.INVALID_TRANSACTION_AMOUNT,
						null,
						null,
						null);
			}
			if (fee.reasons().stream()
					.flatMap(reason -> reason.conditions().stream())
					.anyMatch(condition -> !isValid(condition))) {
				return new FeeResult(
						request.feeRequestId(),
						FeeStatus.INVALID_ELIGIBILITY_CONDITION,
						null,
						null,
						null);
			}

			List<String> reasons = fee.reasons().stream()
					.filter(reason -> reason.conditions().stream()
							.allMatch(condition -> matches(condition, attributes)))
					.map(EligibilityReason::code)
					.toList();
			if (!reasons.isEmpty()) {
				return new FeeResult(
						request.feeRequestId(),
						FeeStatus.OK,
						Decision.WAIVED,
						null,
						reasons);
			}

			BigDecimal amount = switch (fee.type()) {
				case FLAT -> fee.amount();
				case PERCENT -> request.transactionAmount().multiply(fee.amount()).movePointLeft(2);
			};
			return new FeeResult(
					request.feeRequestId(),
					FeeStatus.OK,
					Decision.CHARGED,
					amount.setScale(2, RoundingMode.HALF_UP),
					null);
		} catch (RuntimeException exception) {
			return new FeeResult(request.feeRequestId(), FeeStatus.ERROR, null, null, null);
		}
	}

	private boolean isValid(EligibilityCondition condition) {
		boolean operatorIsValid = switch (condition.attribute().type()) {
			case TEXT, BOOLEAN -> condition.operator().equals("=")
					|| condition.operator().equals("<>");
			case DECIMAL, INTEGER, DATE -> switch (condition.operator()) {
				case "=", "<>", ">", "<", ">=", "<=" -> true;
				default -> false;
			};
		};
		if (!operatorIsValid) {
			return false;
		}

		try {
			switch (condition.attribute().type()) {
				case TEXT -> {}
				case BOOLEAN -> {
					return (condition.value().equals("true") || condition.value().equals("false"));
				}
				case DECIMAL -> new BigDecimal(condition.value());
				case INTEGER -> new BigDecimal(condition.value()).toBigIntegerExact();
				case DATE -> LocalDate.parse(condition.value());
			}
			return true;
		} catch (NumberFormatException | ArithmeticException | DateTimeParseException exception) {
			return false;
		}
	}

	private boolean matches(EligibilityCondition condition, List<AccountAttribute> attributes) {
		Object attributeValue = attributes.stream()
				.filter(attribute -> attribute.code().equals(condition.attribute().code()))
				.findFirst()
				.orElseThrow()
				.value();
		int comparison = switch (condition.attribute().type()) {
			case TEXT -> ((String) attributeValue).trim().compareToIgnoreCase(condition.value().trim());
			case BOOLEAN -> Boolean.compare(
					(Boolean) attributeValue,
					Boolean.parseBoolean(condition.value()));
			case DECIMAL, INTEGER -> ((BigDecimal) attributeValue)
					.compareTo(new BigDecimal(condition.value()));
			case DATE -> LocalDate.parse((String) attributeValue)
					.compareTo(LocalDate.parse(condition.value()));
		};
		return switch (condition.operator()) {
			case "=" -> comparison == 0;
			case "<>" -> comparison != 0;
			case ">" -> comparison > 0;
			case "<" -> comparison < 0;
			case ">=" -> comparison >= 0;
			case "<=" -> comparison <= 0;
			default -> throw new IllegalStateException("Unsupported Eligibility Reason operator");
		};
	}

	private <T> T requireOne(List<T> matches, String configurationType) {
		if (matches.size() != 1) {
			throw new IllegalStateException(configurationType + " is outside the flat-Fee tracer");
		}

		return matches.getFirst();
	}
}
