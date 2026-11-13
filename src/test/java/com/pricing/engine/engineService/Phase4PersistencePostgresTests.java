package com.pricing.engine.engineService;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import com.pricing.engine.utils.MockPricing;
import com.pricing.engine.utils.PostgresFixture;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;

class Phase4PersistencePostgresTests extends PostgresFixture {

	@Test
	void retriesAtomicStorageWithoutReloadingChangedPricing() throws Exception {
		addAccount(UUID.randomUUID());
		failRecommendationWrites(2);
		AtomicInteger connections = new AtomicInteger();
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			if (!MockPricing.COMPLETED_BEFORE_EVALUATION.isEmpty()
					&& connections.getAndIncrement() < 3) {
				MockPricing.januaryAmount = "9.99";
				try (var statement = connection.createStatement();
						var rows = statement.executeQuery(
								"select count(*) from recommendation_outbox")) {
					rows.next();
					assertEquals(0, rows.getInt(1));
				}
			}
			return connection;
		}).when(recommendationDataSource).getConnection();
		try {
			service.processPendingAccounts();

			assertEquals(3L, recommendations.queryForObject(
					"select last_value from recommendation_write_attempts", Long.class));
			assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
			assertEquals("9.99", MockPricing.januaryAmount);
			assertEquals(new BigDecimal("2.50"), recommendations.queryForObject(
					"select amount from fee_request_recommendations", BigDecimal.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		} finally {
			removeRecommendationWriteFailure();
		}
	}

	@Test
	void lostCommitAcknowledgementPreservesCommittedOutput() throws Exception {
		assertLostCommitAcknowledgementPreservesCommittedOutput("recommendation");
	}

	void failRecommendationWrites(int failures) {
		recommendations.execute("create sequence recommendation_write_attempts");
		recommendations.execute("""
				create function fail_recommendation_write() returns trigger
				language plpgsql as $function$
				begin
					if nextval('recommendation_write_attempts') <= %d then
						raise exception 'Temporary write failure' using errcode = '40001';
					end if;
					return NEW;
				end;
				$function$
				""".formatted(failures));
		recommendations.execute("""
				create trigger fail_recommendation_write before insert on recommendation_outbox
				for each row execute function fail_recommendation_write()
				""");
	}

	void removeRecommendationWriteFailure() {
		recommendations.execute(
				"drop trigger fail_recommendation_write on recommendation_outbox");
		recommendations.execute("drop function fail_recommendation_write()");
		recommendations.execute("drop sequence recommendation_write_attempts");
	}

	@Test
	void rollsBackCompleteOutputWhenTheRecommendationOutboxInsertFails() {
		UUID batchId = UUID.fromString("b566f38a-0d68-4adb-8a86-84b0d945d645");
		addAccount(batchId);
		recommendations.execute("""
				alter table recommendation_outbox add constraint reject_test_output
				check (batch_id <> 'b566f38a-0d68-4adb-8a86-84b0d945d645')
				""");
		try {
			RuleEngineBatchResult result = ruleEngine.price(accountRepository.load(batchId));
			assertThrows(DataIntegrityViolationException.class,
					() -> recommendationRepository.save(result));
			for (String table : List.of("account_recommendations",
					"fee_request_recommendations", "recommendation_eligibility_reasons",
					"recommendation_outbox")) {
				assertEquals(0, recommendations.queryForObject(
						"select count(*) from " + table, Integer.class), table);
			}
			assertFalse(accounts.queryForObject("select processed from batch_outbox",
					Boolean.class));
			await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
					.untilAsserted(() -> assertTrue(WAKE_UPS.isEmpty()));
		} finally {
			recommendations.execute(
					"alter table recommendation_outbox drop constraint reject_test_output");
		}
		MockPricing.januaryAmount = "9.99";
		try (var application = startApplication()) {
			assertAttempts(application.getBean(WakeUpReceiver.class).wakeUps(), 1);
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox", Boolean.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertEquals(new BigDecimal("9.99"), recommendations.queryForObject(
					"select amount from fee_request_recommendations", BigDecimal.class));
			assertEquals(2, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
		}
	}

	@Test
	void storesAllOutcomeFieldsAndEnforcesTheUniqueRecommendationOutboxKey() {
		UUID batchId = UUID.randomUUID();
		var result = new RuleEngineBatchResult(batchId, List.of(
				new AccountResult("A1", AccountStatus.OK, "JANPLAN", List.of(
						new FeeResult(41L, FeeStatus.CHARGED,
								new BigDecimal("12.50"), null),
						new FeeResult(42L, FeeStatus.WAIVED, null,
								List.of("VIP", "LOYAL")),
						new FeeResult(43L, FeeStatus.FEE_NOT_FOUND, null, null))),
				new AccountResult("A2", AccountStatus.PLAN_NOT_FOUND, null, null)));

		recommendationRepository.save(result);

		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
		assertEquals("JANPLAN", recommendations.queryForObject("""
				select pricing_plan_code from account_recommendations where account_number = 'A1'
				""", String.class));
		assertEquals("OK", recommendations.queryForObject("""
				select status from account_recommendations where account_number = 'A1'
				""", String.class));
		assertEquals("PLAN_NOT_FOUND", recommendations.queryForObject("""
				select status from account_recommendations where account_number = 'A2'
				""", String.class));
		assertNull(recommendations.queryForObject("""
				select pricing_plan_code from account_recommendations where account_number = 'A2'
				""", String.class));
		assertEquals(List.of(41L, 42L, 43L), recommendations.queryForList("""
				select fee_request_id from fee_request_recommendations order by fee_request_id
				""", Long.class));
		assertEquals(List.of("CHARGED", "WAIVED", "FEE_NOT_FOUND"), recommendations.queryForList("""
				select status from fee_request_recommendations order by fee_request_id
				""", String.class));
		assertEquals(0, recommendations.queryForObject("""
				select amount from fee_request_recommendations where fee_request_id = 41
				""", BigDecimal.class).compareTo(new BigDecimal("12.50")));
		assertNull(recommendations.queryForObject("""
				select amount from fee_request_recommendations where fee_request_id = 42
				""", BigDecimal.class));
		assertEquals(List.of("LOYAL", "VIP"), recommendations.queryForList("""
				select reason_code from recommendation_eligibility_reasons order by reason_code
				""", String.class));
		assertFalse(recommendations.queryForObject(
				"select processed from recommendation_outbox", Boolean.class));
		assertThrows(DataIntegrityViolationException.class, () -> recommendations.update("""
				insert into recommendation_outbox(batch_id, processed) values (?, false)
				""", batchId));
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
	}

}
