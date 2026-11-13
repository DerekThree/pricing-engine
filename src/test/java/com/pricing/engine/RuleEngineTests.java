package com.pricing.engine;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.pricing.engine.ruleEngine.Pricing;
import com.pricing.engine.ruleEngine.RuleEngine;
import com.pricing.engine.ruleEngine.RuleEngineRepository;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static com.pricing.engine.ruleEngine.Pricing.AttributeDefinition;
import static com.pricing.engine.ruleEngine.Pricing.AttributeType;
import static com.pricing.engine.ruleEngine.Pricing.Branch;
import static com.pricing.engine.ruleEngine.Pricing.EligibilityCondition;
import static com.pricing.engine.ruleEngine.Pricing.EligibilityReason;
import static com.pricing.engine.ruleEngine.Pricing.FeeType;
import static com.pricing.engine.ruleEngine.Pricing.Plan;
import static com.pricing.engine.ruleEngine.Pricing.PlanFee;
import static com.pricing.engine.ruleEngine.Pricing.Region;
import static com.pricing.engine.ruleEngine.RuleEngineBatch.*;
import static com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RuleEngineTests {

	private static final UUID BATCH_ID = UUID.fromString("5fd2879b-7f17-4a05-8fbe-7ebce6958f3b");
	private static final LocalDate AUGUST_1 = LocalDate.of(2026, 8, 1);
	private static final LocalDate AUGUST_15 = LocalDate.of(2026, 8, 15);
	private static final LocalDate AUGUST_31 = LocalDate.of(2026, 8, 31);
	private static final Branch BRANCH = new Branch("BRANCH001", "IL", "60459");

	@Test
	void pricesOneFlatFeeAtInclusiveActiveThroughBoundary() {
		Pricing pricing = pricing(
				plan("PLAN0001", "PROD0001", "REGION001", AUGUST_1, AUGUST_31));
		RuleEngineRepository repository = repository(pricing);
		RuleEngine engine = new RuleEngine(repository);

		RuleEngineBatchResult result = engine.price(batch("PROD0001", "BRANCH001", AUGUST_31));

		assertEquals(chargedResult("PLAN0001"), result);
		verify(repository).load(Set.of(AUGUST_31));
		verifyNoMoreInteractions(repository);
	}

	@Test
	void usesOnePricingReadForEveryAccountAndFeeRequest() {
		Pricing pricing = pricing(plan(List.of(
				flatFee("FEE00001"),
				flatFee("FEE00002"))));
		RuleEngineRepository repository = repository(pricing);
		RuleEngine engine = new RuleEngine(repository);
		RuleEngineBatch batch = new RuleEngineBatch(BATCH_ID, List.of(
				new Account(
						"ACCOUNT001",
						"PROD0001",
						"BRANCH001",
						AUGUST_15,
						List.of(),
						List.of(
								new FeeRequest(1L, "FEE00001", null),
								new FeeRequest(2L, "FEE00002", null))),
				new Account(
						"ACCOUNT002",
						"PROD0001",
						"BRANCH001",
						AUGUST_31,
						List.of(),
						List.of(
								new FeeRequest(3L, "FEE00001", null),
								new FeeRequest(4L, "FEE00002", null)))));

		RuleEngineBatchResult result = engine.price(batch);

		assertEquals(Map.of(
				"ACCOUNT001", AccountStatus.OK,
				"ACCOUNT002", AccountStatus.OK), result.accounts().stream()
				.collect(Collectors.toMap(AccountResult::accountNumber, AccountResult::status)));
		assertEquals(Map.of(
				"ACCOUNT001", "PLAN0001",
				"ACCOUNT002", "PLAN0001"), result.accounts().stream()
				.collect(Collectors.toMap(
						AccountResult::accountNumber,
						AccountResult::pricingPlanCode)));
		assertEquals(Map.of(
				1L, chargedFee(1L, "7.50"),
				2L, chargedFee(2L, "7.50"),
				3L, chargedFee(3L, "7.50"),
				4L, chargedFee(4L, "7.50")), result.accounts().stream()
				.flatMap(account -> account.fees().stream())
				.collect(Collectors.toMap(FeeResult::feeRequestId, fee -> fee)));
		verify(repository).load(Set.of(AUGUST_15, AUGUST_31));
		verifyNoMoreInteractions(repository);
	}

	@ParameterizedTest
	@MethodSource("unmatchedPlans")
	void returnsPlanNotFoundForAnUnmatchedProductOrDate(String productCode, LocalDate pricingDate) {
		Pricing pricing = pricing(
				plan("PLAN0001", "PROD0001", "REGION001", AUGUST_1, AUGUST_31));

		RuleEngineBatchResult result = price(pricing, productCode, "BRANCH001", pricingDate);

		assertEquals(failedResult(AccountStatus.PLAN_NOT_FOUND), result);
	}

	@Test
	void returnsBranchNotFoundForAnInexactBranchCode() {
		Pricing pricing = pricing(List.of(), List.of());

		RuleEngineBatchResult result = price(pricing, "PROD0001", "branch001", AUGUST_15);

		assertEquals(failedResult(AccountStatus.BRANCH_NOT_FOUND), result);
	}

	@ParameterizedTest
	@MethodSource("regionResolutionCases")
	void resolvesRegionThroughZipCodeOrStateWhenNoHigherTierMatches(
			Set<String> zipCodes, Set<String> states) {
		Pricing pricing = pricing(
				List.of(region("REGION001", Set.of(), zipCodes, states)),
				List.of(plan("PLAN0001", "PROD0001", "REGION001", AUGUST_1, AUGUST_31)));

		RuleEngineBatchResult result = price(pricing, "PROD0001", "BRANCH001", AUGUST_15);

		assertEquals(chargedResult("PLAN0001"), result);
	}

	@Test
	void stopsAfterTheFirstRegionTierContainingAMatch() {
		Pricing pricing = pricing(
				List.of(
						region("REGION001", Set.of("BRANCH001"), Set.of(), Set.of()),
						region("REGION002", Set.of(), Set.of("60459"), Set.of()),
						region("REGION003", Set.of(), Set.of("60459"), Set.of("IL"))),
				List.of(plan("PLAN0001", "PROD0001", "REGION001", AUGUST_1, AUGUST_31)));

		RuleEngineBatchResult result = price(pricing, "PROD0001", "BRANCH001", AUGUST_15);

		assertEquals(chargedResult("PLAN0001"), result);
	}

	@Test
	void returnsRegionNotFoundWhenNoRegionTierMatches() {
		RuleEngineBatchResult result = price(pricing(List.of(), List.of()),
				"PROD0001", "BRANCH001", AUGUST_15);

		assertEquals(failedResult(AccountStatus.REGION_NOT_FOUND), result);
	}

	@Test
	void returnsErrorWhenTheWinningRegionTierIsAmbiguous() {
		Pricing pricing = pricing(
				List.of(
						region("REGION001", Set.of("BRANCH001"), Set.of(), Set.of()),
						region("REGION002", Set.of("BRANCH001"), Set.of(), Set.of())),
				List.of());

		RuleEngineBatchResult result = price(pricing, "PROD0001", "BRANCH001", AUGUST_15);

		assertEquals(failedResult(AccountStatus.ERROR), result);
	}

	@Test
	void selectsPlanByExactProductRegionAndInclusiveActiveFromBoundary() {
		Pricing pricing = pricing(
				plan("PLAN0001", "PROD0001", "REGION001", AUGUST_1, AUGUST_31),
				plan("PLAN0002", "PROD0002", "REGION001", AUGUST_1, AUGUST_31),
				plan("PLAN0003", "PROD0001", "REGION002", AUGUST_1, AUGUST_31),
				plan("PLAN0004", "PROD0001", "REGION001", AUGUST_15, AUGUST_31));

		RuleEngineBatchResult result = price(pricing, "PROD0001", "BRANCH001", AUGUST_1);

		assertEquals(chargedResult("PLAN0001"), result);
	}

	@Test
	void returnsErrorWhenMultiplePricingPlansApply() {
		Pricing pricing = pricing(
				plan("PLAN0001", "PROD0001", "REGION001", AUGUST_1, AUGUST_31),
				plan("PLAN0002", "PROD0001", "REGION001", AUGUST_15,
						LocalDate.of(2026, 9, 15)));

		RuleEngineBatchResult result = price(pricing, "PROD0001", "BRANCH001", AUGUST_15);

		assertEquals(failedResult(AccountStatus.ERROR), result);
	}

	@ParameterizedTest
	@MethodSource("invalidRequestedFeeAttributes")
	void returnsExpectedStatusForInvalidRequestedFeeAttributes(
			List<AccountAttribute> attributes, AccountStatus expectedStatus) {
		RuleEngineBatchResult result = price(
				fee("FEE00001", new AttributeDefinition("ATTR0001", AttributeType.TEXT)),
				attributes);

		assertEquals(failedResult(expectedStatus), result);
	}

	@ParameterizedTest
	@MethodSource("validAttributeValues")
	void acceptsEveryConfiguredAttributeRepresentation(AttributeType type, Object value) {
		RuleEngineBatchResult result = price(
				fee("FEE00001", new AttributeDefinition("ATTR0001", type)),
				List.of(new AccountAttribute("ATTR0001", value)));

		assertEquals(chargedResult("PLAN0001"), result);
	}

	@Test
	void doesNotRequireAttributesForAnUnrequestedPlanFee() {
		Pricing pricing = pricing(
				fee("FEE00001", new AttributeDefinition("ATTR0001", AttributeType.TEXT)),
				flatFee("FEE00002"));

		RuleEngineBatchResult result = price(pricing, batch(
				List.of(),
				List.of(new FeeRequest(1L, "FEE00002", null))));

		assertEquals(okResult(chargedFee("7.50")), result);
	}

	@Test
	void returnsFeeNotFoundWithoutCreatingAnAttributeRequirementForAnAbsentPlanFee() {
		Pricing pricing = pricing(
				fee("FEE00001", new AttributeDefinition("ATTR0001", AttributeType.TEXT)));

		RuleEngineBatchResult result = price(pricing, batch(
				List.of(),
				List.of(new FeeRequest(1L, "FEE99999", null))));

		assertEquals(okResult(failedFee(FeeStatus.FEE_NOT_FOUND)), result);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("percentageFees")
	void pricesPercentageFees(
			String scenario, BigDecimal amount, boolean waived, FeeResult expected) {
		PlanFee fee = waived
				? percentageFee("FEE00001", "5.0000", reason("ALWAYS"))
				: percentageFee("FEE00001", "5.0000");
		assertEquals(expected, priceFee(fee, amount));
	}

	@ParameterizedTest
	@MethodSource("satisfyingTypedComparisons")
	void supportsEveryTypedEligibilityOperator(
			AttributeType type, Object accountValue, String operator, String conditionValue) {
		RuleEngineBatchResult result = price(
				feeWithReasons("FEE00001", reason("REASON001",
						condition("ATTR0001", type, operator, conditionValue))),
				List.of(new AccountAttribute("ATTR0001", accountValue)));

		assertEquals(waivedResult("REASON001"), result);
	}

	@Test
	void requiresEveryConditionWithinOneEligibilityReason() {
		RuleEngineBatchResult result = price(
				feeWithReasons("FEE00001", reason("REASON001",
						condition("ATTR0001", AttributeType.TEXT, "=", "PREMIER"),
						condition("ATTR0002", AttributeType.DECIMAL, ">", "100"))),
				List.of(
						new AccountAttribute("ATTR0001", "PREMIER"),
						new AccountAttribute("ATTR0002", new BigDecimal("50"))));

		assertEquals(chargedResult("PLAN0001"), result);
	}

	@Test
	void waivesForAnySatisfiedReasonAndReturnsEverySatisfiedReasonCode() {
		RuleEngineBatchResult result = price(
				feeWithReasons(
						"FEE00001",
						reason("TEXT_REASON",
								condition("ATTR0001", AttributeType.TEXT, "=", "PREMIER")),
						reason("AMOUNT_REASON",
								condition("ATTR0002", AttributeType.DECIMAL, ">=", "100")),
						reason("BOOLEAN_REASON",
								condition("ATTR0003", AttributeType.BOOLEAN, "=", "true"))),
				List.of(
						new AccountAttribute("ATTR0001", " premier "),
						new AccountAttribute("ATTR0002", new BigDecimal("100.00")),
						new AccountAttribute("ATTR0003", false)));

		FeeResult fee = result.accounts().getFirst().fees().getFirst();
		assertEquals(FeeStatus.WAIVED, fee.status());
		assertEquals(null, fee.amount());
		assertEquals(Set.of("TEXT_REASON", "AMOUNT_REASON"), Set.copyOf(fee.reasons()));
	}

	@Test
	void treatsAnEligibilityReasonWithoutConditionsAsSatisfied() {
		RuleEngineBatchResult result = price(
				feeWithReasons("FEE00001", reason("ALWAYS")),
				List.of());

		assertEquals(waivedResult("ALWAYS"), result);
	}

	@ParameterizedTest
	@MethodSource("invalidConditions")
	void rejectsInvalidEligibilityConditionsWithoutAffectingOtherFees(
			AttributeType type, Object accountValue, String operator, String conditionValue) {
		Pricing pricing = pricing(
				feeWithReasons("FEE00001", reason("INVALID",
						condition("ATTR0001", type, operator, conditionValue))),
				flatFee("FEE00002"));

		RuleEngineBatchResult result = price(pricing, batch(
				List.of(new AccountAttribute("ATTR0001", accountValue)),
				List.of(
						new FeeRequest(1L, "FEE00001", null),
						new FeeRequest(2L, "FEE00002", null))));

		assertEquals(
				okResult(
						failedFee(FeeStatus.INVALID_ELIGIBILITY_CONDITION),
						chargedFee(2L, "7.50")),
				result);
	}

	private RuleEngineBatchResult price(
			Pricing pricing,
			String productCode,
			String branchCode,
			LocalDate pricingDate) {
		return new RuleEngine(repository(pricing))
				.price(batch(productCode, branchCode, pricingDate));
	}

	private RuleEngineBatchResult price(Pricing pricing, RuleEngineBatch batch) {
		return new RuleEngine(repository(pricing)).price(batch);
	}

	private RuleEngineBatchResult price(PlanFee fee, List<AccountAttribute> attributes) {
		return price(pricing(fee), batch(
				attributes,
				List.of(new FeeRequest(1L, fee.code(), null))));
	}

	private FeeResult priceFee(PlanFee fee, BigDecimal transactionAmount) {
		return price(pricing(fee), batch(
				List.of(),
				List.of(new FeeRequest(1L, fee.code(), transactionAmount))))
				.accounts().getFirst().fees().getFirst();
	}

	private RuleEngineBatch batch(String productCode, String branchCode, LocalDate pricingDate) {
		return new RuleEngineBatch(BATCH_ID, List.of(new Account(
				"ACCOUNT001",
				productCode,
				branchCode,
				pricingDate,
				List.of(),
				List.of(new FeeRequest(1L, "FEE00001", null)))));
	}

	private RuleEngineBatch batch(List<AccountAttribute> attributes, List<FeeRequest> fees) {
		return new RuleEngineBatch(BATCH_ID, List.of(new Account(
				"ACCOUNT001",
				"PROD0001",
				"BRANCH001",
				AUGUST_15,
				attributes,
				fees)));
	}

	private Pricing pricing(List<Region> regions, List<Plan> plans) {
		return new Pricing(
				Map.of("BRANCH001", BRANCH),
				regions,
				plans);
	}

	private Pricing pricing(PlanFee... fees) {
		return pricing(
				List.of(region("REGION001", Set.of("BRANCH001"), Set.of(), Set.of())),
				List.of(plan(List.of(fees))));
	}

	private Pricing pricing(Plan... plans) {
		return pricing(
				List.of(region("REGION001", Set.of("BRANCH001"), Set.of(), Set.of())),
				List.of(plans));
	}

	private Region region(
			String code,
			Set<String> branchCodes,
			Set<String> zipCodes,
			Set<String> states) {
		return new Region(code, branchCodes, zipCodes, states);
	}

	private Plan plan(
			String code,
			String productCode,
			String regionCode,
			LocalDate activeFrom,
			LocalDate activeThrough) {
		return new Plan(
				code,
				productCode,
				regionCode,
				activeFrom,
				activeThrough,
				List.of(flatFee("FEE00001")));
	}

	private Plan plan(List<PlanFee> fees) {
		return new Plan("PLAN0001", "PROD0001", "REGION001", AUGUST_1, AUGUST_31, fees);
	}

	private PlanFee fee(String code, AttributeDefinition... attributes) {
		return feeWithReasons(code, new EligibilityReason(
				"REASON001",
				Stream.of(attributes)
						.map(attribute -> new EligibilityCondition(
								attribute,
								"=",
								unmatchedValue(attribute.type())))
						.toList()));
	}

	private PlanFee flatFee(String code) {
		return feeWithReasons(code);
	}

	private PlanFee percentageFee(
			String code, String percentage, EligibilityReason... reasons) {
		return new PlanFee(
				code,
				FeeType.PERCENT,
				new BigDecimal(percentage),
				List.of(reasons));
	}

	private PlanFee feeWithReasons(String code, EligibilityReason... reasons) {
		return new PlanFee(
				code,
				FeeType.FLAT,
				new BigDecimal("7.5000"),
				List.of(reasons));
	}

	private EligibilityReason reason(String code, EligibilityCondition... conditions) {
		return new EligibilityReason(code, List.of(conditions));
	}

	private EligibilityCondition condition(
			String code, AttributeType type, String operator, String value) {
		return new EligibilityCondition(new AttributeDefinition(code, type), operator, value);
	}

	private String unmatchedValue(AttributeType type) {
		return switch (type) {
			case TEXT -> "DIFFERENT";
			case BOOLEAN -> "false";
			case DECIMAL, INTEGER -> "0";
			case DATE -> "2025-01-01";
		};
	}

	private RuleEngineBatchResult chargedResult(String planCode) {
		return okResult(planCode, chargedFee("7.50"));
	}

	private RuleEngineBatchResult waivedResult(String... reasons) {
		return okResult(waivedFee(reasons));
	}

	private RuleEngineBatchResult okResult(FeeResult... fees) {
		return okResult("PLAN0001", fees);
	}

	private RuleEngineBatchResult okResult(String planCode, FeeResult... fees) {
		return new RuleEngineBatchResult(BATCH_ID, List.of(new AccountResult(
				"ACCOUNT001",
				AccountStatus.OK,
				planCode,
				List.of(fees))));
	}

	private FeeResult chargedFee(String amount) {
		return chargedFee(1L, amount);
	}

	private FeeResult chargedFee(long feeRequestId, String amount) {
		return new FeeResult(
				feeRequestId,
				FeeStatus.CHARGED,
				new BigDecimal(amount),
				null);
	}

	private FeeResult waivedFee(String... reasons) {
		return new FeeResult(1L, FeeStatus.WAIVED, null, List.of(reasons));
	}

	private FeeResult failedFee(FeeStatus status) {
		return new FeeResult(1L, status, null, null);
	}

	private RuleEngineBatchResult failedResult(AccountStatus status) {
		return new RuleEngineBatchResult(BATCH_ID, List.of(new AccountResult(
				"ACCOUNT001", status, null, null)));
	}

	private static Stream<Arguments> regionResolutionCases() {
		return Stream.of(
				arguments(Set.of("60459"), Set.of()),
				arguments(Set.of(), Set.of("IL")));
	}

	private static Stream<Arguments> invalidRequestedFeeAttributes() {
		return Stream.of(
				arguments(List.of(), AccountStatus.MISSING_ATTRIBUTE),
				arguments(List.of(
						new AccountAttribute("ATTR0001", "FIRST"),
						new AccountAttribute("ATTR0001", "SECOND")), AccountStatus.DUPLICATE_ATTRIBUTE),
				arguments(List.of(new AccountAttribute("ATTR0001", true)),
						AccountStatus.INVALID_ATTRIBUTE_TYPE));
	}

	private static Stream<Arguments> validAttributeValues() {
		return Stream.of(
				arguments(AttributeType.TEXT, " Value "),
				arguments(AttributeType.BOOLEAN, true),
				arguments(AttributeType.DECIMAL, new BigDecimal("1.50")),
				arguments(AttributeType.INTEGER, new BigDecimal("1.00")),
				arguments(AttributeType.DATE, "2026-08-15"));
	}

	private static Stream<Arguments> satisfyingTypedComparisons() {
		return Stream.of(
				arguments(AttributeType.TEXT, " Premium ", "=", "premium"),
				arguments(AttributeType.TEXT, "STANDARD", "<>", "premium"),
				arguments(AttributeType.BOOLEAN, true, "=", "true"),
				arguments(AttributeType.BOOLEAN, false, "<>", "true"),
				arguments(AttributeType.DECIMAL, new BigDecimal("1.0"), "=", "1.00"),
				arguments(AttributeType.DECIMAL, new BigDecimal("2"), "<>", "1"),
				arguments(AttributeType.DECIMAL, new BigDecimal("2"), ">", "1"),
				arguments(AttributeType.DECIMAL, new BigDecimal("1"), "<", "2"),
				arguments(AttributeType.DECIMAL, new BigDecimal("1"), ">=", "1.00"),
				arguments(AttributeType.DECIMAL, new BigDecimal("1"), "<=", "1.0"),
				arguments(AttributeType.INTEGER, new BigDecimal("1"), "=", "1.00"),
				arguments(AttributeType.INTEGER, new BigDecimal("2"), "<>", "1"),
				arguments(AttributeType.INTEGER, new BigDecimal("2"), ">", "1"),
				arguments(AttributeType.INTEGER, new BigDecimal("1"), "<", "2"),
				arguments(AttributeType.INTEGER, new BigDecimal("1"), ">=", "1"),
				arguments(AttributeType.INTEGER, new BigDecimal("1"), "<=", "1"),
				arguments(AttributeType.DATE, "2026-08-15", "=", "2026-08-15"),
				arguments(AttributeType.DATE, "2026-08-16", "<>", "2026-08-15"),
				arguments(AttributeType.DATE, "2026-08-16", ">", "2026-08-15"),
				arguments(AttributeType.DATE, "2026-08-14", "<", "2026-08-15"),
				arguments(AttributeType.DATE, "2026-08-15", ">=", "2026-08-15"),
				arguments(AttributeType.DATE, "2026-08-15", "<=", "2026-08-15"));
	}

	private static Stream<Arguments> unmatchedPlans() {
		return Stream.of(
				arguments("PROD0001 ", AUGUST_15),
				arguments("PROD0001", LocalDate.of(2026, 9, 1)));
	}

	private static Stream<Arguments> percentageFees() {
		return Stream.of(
				arguments("half-up rounding", new BigDecimal("123.45"), false,
						new FeeResult(1L, FeeStatus.CHARGED, new BigDecimal("6.17"), null)),
				arguments("missing transaction", null, false,
						new FeeResult(1L, FeeStatus.MISSING_TRANSACTION, null, null)),
				arguments("zero amount", new BigDecimal("0.00"), false,
						new FeeResult(1L, FeeStatus.CHARGED, new BigDecimal("0.00"), null)),
				arguments("waived", new BigDecimal("10.00"), true,
						new FeeResult(1L, FeeStatus.WAIVED, null, List.of("ALWAYS"))));
	}

	private static Stream<Arguments> invalidConditions() {
		return Stream.of(
				arguments(AttributeType.BOOLEAN, true, ">", "false"),
				arguments(AttributeType.BOOLEAN, true, "=", "not-a-boolean"),
				arguments(AttributeType.DECIMAL, new BigDecimal("1"), "=", "not-a-number"),
				arguments(AttributeType.INTEGER, new BigDecimal("1"), "=", "1.5"),
				arguments(AttributeType.DATE, "2026-08-15", "=", "not-a-date"));
	}

	private static RuleEngineRepository repository(Pricing pricing) {
		RuleEngineRepository repository = mock(RuleEngineRepository.class);
		when(repository.load(anySet())).thenReturn(pricing);
		return repository;
	}
}
