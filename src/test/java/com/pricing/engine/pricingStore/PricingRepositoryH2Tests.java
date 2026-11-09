package com.pricing.engine.pricingStore;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.pricing.engine.ruleEngine.Pricing;
import com.pricing.engine.ruleEngine.RuleEngine;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import com.pricing.engine.ruleEngine.RuleEngineBatch.*;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;

import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.hibernate.resource.jdbc.spi.StatementInspector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
		"spring.jpa.properties.hibernate.generate_statistics=true",
		"spring.jpa.properties.hibernate.session_factory.statement_inspector="
				+ "com.pricing.engine.pricingStore.PricingRepositoryH2Tests$RegionReadPause"
})
class PricingRepositoryH2Tests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private RuleEngine ruleEngine;

	@Autowired
	private PricingRepository configRepository;

	@Autowired
	private EntityManagerFactory entityManagerFactory;

	@Autowired
	private DataSource dataSource;

	@BeforeEach
	void setUp() {
		cleanUp();
	}

	@AfterEach
	void cleanUp() {
		jdbcTemplate.update("delete from pricing_plans");
		jdbcTemplate.update("delete from eligibility_reason_conditions");
		jdbcTemplate.update("delete from eligibility_reasons");
		jdbcTemplate.update("delete from account_attribute_product_types");
		jdbcTemplate.update("delete from account_attributes");
		jdbcTemplate.update("delete from fee_product_types");
		jdbcTemplate.update("delete from fees");
		jdbcTemplate.update("delete from region_branches");
		jdbcTemplate.update("delete from region_zip_codes");
		jdbcTemplate.update("delete from region_states");
		jdbcTemplate.update("delete from branches");
		jdbcTemplate.update("delete from regions");
		jdbcTemplate.update("delete from products");
	}

	@Test
	void loadsFlatFeeAndPlanDetailsFromPersistedPricing() {
		Long productId = insertProduct();
		Long branchId = insertBranch();
		Long regionId = insertRegion(branchId);
		Long feeId = insertFee("FEE00001");
		Long pricingPlanId = insertPricingPlan(
				"PLAN0001",
				productId,
				regionId,
				LocalDate.of(2026, 8, 1),
				LocalDate.of(2026, 8, 31));
		jdbcTemplate.update("insert into pricing_plan_fees values (?, ?, ?)",
				pricingPlanId, feeId, new BigDecimal("7.5000"));

		Pricing pricing = configRepository.load(Set.of(LocalDate.of(2026, 8, 31)));

		assertEquals(Map.of("BRANCH001", new Pricing.Branch("BRANCH001", "IL", "60459")),
				pricing.branches());
		assertEquals(List.of(new Pricing.Region("REGION001", Set.of("BRANCH001"), Set.of(), Set.of())),
				pricing.regions());
		assertEquals(1, pricing.plans().size());
		Pricing.Plan plan = pricing.plans().getFirst();
		assertEquals("PLAN0001", plan.code());
		assertEquals("PROD0001", plan.productCode());
		assertEquals("REGION001", plan.regionCode());
		assertEquals(LocalDate.of(2026, 8, 1), plan.activeFrom());
		assertEquals(LocalDate.of(2026, 8, 31), plan.activeThrough());
		assertEquals(1, plan.fees().size());
		Pricing.PlanFee fee = plan.fees().getFirst();
		assertEquals("FEE00001", fee.code());
		assertEquals(Pricing.FeeType.FLAT, fee.type());
		assertEquals(0, fee.amount().compareTo(new BigDecimal("7.5000")));
		assertEquals(List.of(), fee.reasons());
	}

	@Test
	void loadsFeeEligibilityConditionsAndAssociations() {
		Long productId = insertProduct();
		Long branchId = insertBranch();
		Long regionId = insertRegion(branchId);
		Long waivedFeeId = insertFee("FEE00001");
		Long chargedFeeId = insertFee("FEE00002");
		Long pricingPlanId = insertPricingPlan(
				"PLAN0001",
				productId,
				regionId,
				LocalDate.of(2026, 8, 1),
				LocalDate.of(2026, 8, 31));
		jdbcTemplate.update("insert into pricing_plan_fees values (?, ?, ?)",
				pricingPlanId, waivedFeeId, new BigDecimal("7.5000"));
		jdbcTemplate.update("insert into pricing_plan_fees values (?, ?, ?)",
				pricingPlanId, chargedFeeId, new BigDecimal("8.5000"));
		Long attributeId = insertTextAttribute();
		Long waivedReasonId = insertReason("ELIG0001");
		Long chargedReasonId = insertReason("ELIG0002");
		insertCondition(waivedReasonId, attributeId, "PREMIER");
		insertCondition(chargedReasonId, attributeId, "STANDARD");
		jdbcTemplate.update("insert into pricing_plan_fee_reasons values (?, ?, ?)",
				pricingPlanId, waivedFeeId, waivedReasonId);
		jdbcTemplate.update("insert into pricing_plan_fee_reasons values (?, ?, ?)",
				pricingPlanId, chargedFeeId, chargedReasonId);

		Pricing pricing = configRepository.load(Set.of(LocalDate.of(2026, 8, 31)));

		assertEquals(1, pricing.plans().size());
		assertEquals("PLAN0001", pricing.plans().getFirst().code());
		Map<String, Pricing.PlanFee> fees = pricing.plans().getFirst().fees().stream()
				.collect(Collectors.toMap(Pricing.PlanFee::code, fee -> fee));
		assertEquals(Set.of("FEE00001", "FEE00002"), fees.keySet());
		assertEquals(Pricing.FeeType.FLAT, fees.get("FEE00001").type());
		assertEquals(Pricing.FeeType.FLAT, fees.get("FEE00002").type());
		assertEquals(0, fees.get("FEE00001").amount().compareTo(new BigDecimal("7.5000")));
		assertEquals(0, fees.get("FEE00002").amount().compareTo(new BigDecimal("8.5000")));
		assertEquals(List.of(new Pricing.EligibilityReason("ELIG0001",
				List.of(new Pricing.EligibilityCondition(
						new Pricing.AttributeDefinition("ATTR0001", Pricing.AttributeType.TEXT),
						"=", "PREMIER")))), fees.get("FEE00001").reasons());
		assertEquals(List.of(new Pricing.EligibilityReason("ELIG0002",
				List.of(new Pricing.EligibilityCondition(
						new Pricing.AttributeDefinition("ATTR0001", Pricing.AttributeType.TEXT),
						"=", "STANDARD")))), fees.get("FEE00002").reasons());
	}

	@Test
	void loadsOnlyPricingPlansActiveOnSubmittedPricingDates() {
		Long productId = insertProduct();
		Long branchId = insertBranch();
		Long regionId = insertRegion(branchId);
		insertPricingPlan(
				"PLAN0001",
				productId,
				regionId,
				LocalDate.of(2026, 8, 1),
				LocalDate.of(2026, 8, 31));
		insertPricingPlan(
				"PLAN0002",
				productId,
				regionId,
				LocalDate.of(2026, 9, 1),
				LocalDate.of(2026, 9, 30));
		insertPricingPlan(
				"PLAN0003",
				productId,
				regionId,
				LocalDate.of(2026, 10, 1),
				LocalDate.of(2026, 10, 31));

		Pricing config = configRepository.load(Set.of(
				LocalDate.of(2026, 8, 15),
				LocalDate.of(2026, 10, 15)));

		assertEquals(Set.of("PLAN0001", "PLAN0003"), config.plans().stream()
				.map(Pricing.Plan::code)
				.collect(Collectors.toSet()));
	}

	@Test
	void loadsZipCodeRegionAndFiltersPlansByPricingDate() {
		Long productId = insertProduct();
		insertBranch();
		Long regionId = insertRegionByZipCode();
		Long feeId = insertFee("FEE00001");
		Long pricingPlanId = insertPricingPlan(
				"PLAN0001",
				productId,
				regionId,
				LocalDate.of(2026, 8, 1),
				LocalDate.of(2026, 8, 31));
		jdbcTemplate.update("insert into pricing_plan_fees values (?, ?, ?)",
				pricingPlanId, feeId, new BigDecimal("7.5000"));

		Pricing pricing = configRepository.load(Set.of(
				LocalDate.of(2026, 8, 31), LocalDate.of(2026, 9, 1)));

		assertEquals(Map.of("BRANCH001", new Pricing.Branch("BRANCH001", "IL", "60459")),
				pricing.branches());
		assertEquals(List.of(new Pricing.Region("REGION001", Set.of(), Set.of("60459"), Set.of())),
				pricing.regions());
		assertEquals(1, pricing.plans().size());
		Pricing.Plan plan = pricing.plans().getFirst();
		assertEquals("PLAN0001", plan.code());
		assertEquals("PROD0001", plan.productCode());
		assertEquals("REGION001", plan.regionCode());
		assertEquals(LocalDate.of(2026, 8, 1), plan.activeFrom());
		assertEquals(LocalDate.of(2026, 8, 31), plan.activeThrough());
		assertEquals(1, plan.fees().size());
		assertEquals("FEE00001", plan.fees().getFirst().code());
		assertEquals(0, plan.fees().getFirst().amount().compareTo(new BigDecimal("7.5000")));
		assertTrue(configRepository.load(Set.of(LocalDate.of(2026, 9, 1))).plans().isEmpty());
	}

	@Test
	void additionalRegionMembershipsDoNotAddPricingQueries() {
		Long branchId = insertBranch();
		insertRegion(branchId);
		RuleEngineBatch batch = new RuleEngineBatch(
				UUID.fromString("5fd2879b-7f17-4a05-8fbe-7ebce6958f3b"),
				List.of(new Account(
						"ACCOUNT001",
						"PROD0001",
						"BRANCH001",
						LocalDate.of(2026, 8, 31),
						List.of(),
						List.of(new FeeRequest(1L, "FEE00001", null)))));
		Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
		statistics.clear();

		RuleEngineBatchResult oneRegionResult = ruleEngine.price(batch);
		long oneRegionQueryCount = statistics.getPrepareStatementCount();
		Long secondRegionId = insert("""
				insert into regions (region_code, region_name, updated_on, updated_by)
				values ('REGION002', 'Southwest', ?, 'test')
				""", OffsetDateTime.now());
		jdbcTemplate.update(
				"insert into region_states (region_id, state_code) values (?, 'NM')",
				secondRegionId);
		jdbcTemplate.update(
				"insert into region_zip_codes (region_id, zip_code) values (?, '75001')",
				secondRegionId);
		statistics.clear();

		RuleEngineBatchResult twoRegionResult = ruleEngine.price(batch);

		assertEquals(AccountStatus.PLAN_NOT_FOUND,
				oneRegionResult.accounts().getFirst().status());
		assertEquals(AccountStatus.PLAN_NOT_FOUND,
				twoRegionResult.accounts().getFirst().status());
		assertEquals(oneRegionQueryCount, statistics.getPrepareStatementCount());
	}

	@Test
	void usesOneRepeatableReadSnapshotAcrossPricingLoadPhases() throws Exception {
		UUID batchId = UUID.fromString("5fd2879b-7f17-4a05-8fbe-7ebce6958f3b");
		Long productId = insertProduct();
		Long branchId = insertBranch();
		Long regionId = insertRegion(branchId);
		Long feeId = insertFee("FEE00001");
		Long pricingPlanId = insertPricingPlan(
				"PLAN0001",
				productId,
				regionId,
				LocalDate.of(2026, 8, 1),
				LocalDate.of(2026, 8, 31));
		jdbcTemplate.update("insert into pricing_plan_fees values (?, ?, ?)",
				pricingPlanId, feeId, new BigDecimal("7.5000"));
		RuleEngineBatch batch = new RuleEngineBatch(batchId, List.of(new Account(
				"ACCOUNT001",
				"PROD0001",
				"BRANCH001",
				LocalDate.of(2026, 8, 31),
				List.of(),
				List.of(new FeeRequest(1L, "FEE00001", null)))));

		try (ExecutorService executor = Executors.newSingleThreadExecutor();
				Connection writer = dataSource.getConnection()) {
			writer.setAutoCommit(false);
			RegionReadPause pause = new RegionReadPause();
			RegionReadPause.active = pause;
			try {
				Future<RuleEngineBatchResult> resultFuture = executor.submit(
						() -> ruleEngine.price(batch));
				assertTrue(pause.reached.await(10, TimeUnit.SECONDS),
						"Pricing data load did not reach the Region phase");
				try (PreparedStatement deleteMembership = writer.prepareStatement(
						"delete from region_branches where region_id = ?")) {
					deleteMembership.setLong(1, regionId);
					assertEquals(1, deleteMembership.executeUpdate());
				}
				writer.commit();
				assertEquals(0, jdbcTemplate.queryForObject(
						"select count(*) from region_branches where region_id = ?",
						Integer.class, regionId));
				pause.resume.countDown();

				RuleEngineBatchResult result = resultFuture.get(10, TimeUnit.SECONDS);

				assertEquals(AccountStatus.OK, result.accounts().getFirst().status());
			} finally {
				RegionReadPause.active = null;
				pause.resume.countDown();
			}
		}
	}

	public static class RegionReadPause implements StatementInspector {
		private static volatile RegionReadPause active;
		private final CountDownLatch reached = new CountDownLatch(1);
		private final CountDownLatch resume = new CountDownLatch(1);

		@Override
		public String inspect(String sql) {
			RegionReadPause pause = active;
			if (pause != null && sql.contains(" from regions ")) {
				active = null;
				pause.reached.countDown();
				try {
					assertTrue(pause.resume.await(10, TimeUnit.SECONDS),
							"Concurrent membership change did not complete");
				} catch (InterruptedException exception) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(exception);
				}
			}
			return sql;
		}
	}

	private Long insertProduct() {
		return insert("""
				insert into products (product_code, product_name, product_type, updated_on, updated_by)
				values ('PROD0001', 'Premier Checking', 'DEPOSIT', ?, 'test')
				""", OffsetDateTime.now());
	}

	private Long insertBranch() {
		return insert("""
				insert into branches (branch_code, branch_name, state, zip_code, updated_on, updated_by)
				values ('BRANCH001', 'Chicago 104th St.', 'IL', '60459', ?, 'test')
				""", OffsetDateTime.now());
	}

	private Long insertRegion(Long branchId) {
		Long regionId = insert("""
				insert into regions (region_code, region_name, updated_on, updated_by)
				values ('REGION001', 'Midwest', ?, 'test')
				""", OffsetDateTime.now());
		jdbcTemplate.update("insert into region_branches (region_id, branch_id) values (?, ?)",
				regionId, branchId);
		return regionId;
	}

	private Long insertRegionByZipCode() {
		Long regionId = insert("""
				insert into regions (region_code, region_name, updated_on, updated_by)
				values ('REGION001', 'Midwest', ?, 'test')
				""", OffsetDateTime.now());
		jdbcTemplate.update("insert into region_zip_codes (region_id, zip_code) values (?, '60459')",
				regionId);
		return regionId;
	}

	private Long insertFee(String feeCode) {
		Long feeId = insert("""
				insert into fees (fee_code, fee_name, fee_type, updated_on, updated_by)
				values (?, 'Monthly Maintenance Fee', 'FLAT', ?, 'test')
				""", feeCode, OffsetDateTime.now());
		jdbcTemplate.update("insert into fee_product_types (fee_id, product_type) values (?, 'DEPOSIT')", feeId);
		return feeId;
	}

	private Long insertTextAttribute() {
		return insert("""
				insert into account_attributes (
						attribute_code, attribute_name, attribute_type, updated_on, updated_by)
				values ('ATTR0001', 'Account Tier', 'TEXT', ?, 'test')
				""", OffsetDateTime.now());
	}

	private Long insertReason(String reasonCode) {
		return insert("""
				insert into eligibility_reasons (
						reason_code, reason_name, updated_on, updated_by)
				values (?, 'Premier Account', ?, 'test')
				""", reasonCode, OffsetDateTime.now());
	}

	private void insertCondition(Long reasonId, Long attributeId, String value) {
		jdbcTemplate.update("""
				insert into eligibility_reason_conditions (
						reason_id, attribute_id, operator, attribute_value)
				values (?, ?, '=', ?)
				""", reasonId, attributeId, value);
	}

	private Long insertPricingPlan(
			String planCode,
			Long productId,
			Long regionId,
			LocalDate activeFrom,
			LocalDate activeThrough) {
		return insert("""
				insert into pricing_plans (plan_code, plan_name, product_id, region_id, active_from,
						active_through, updated_on, updated_by)
				values (?, 'Premier Midwest', ?, ?, ?, ?, ?, 'test')
				""", planCode, productId, regionId, activeFrom, activeThrough,
				OffsetDateTime.now());
	}

	private Long insert(String sql, Object... values) {
		GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
		jdbcTemplate.update(connection -> {
			PreparedStatement statement = connection.prepareStatement(sql, new String[] { "id" });
			for (int index = 0; index < values.length; index++) {
				statement.setObject(index + 1, values[index]);
			}
			return statement;
		}, keyHolder);
		return keyHolder.getKey().longValue();
	}
}
