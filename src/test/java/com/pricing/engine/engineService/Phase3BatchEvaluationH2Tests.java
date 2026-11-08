package com.pricing.engine.engineService;

import com.pricing.engine.EngineService;
import com.pricing.engine.H2TestSupport;
import com.pricing.engine.MockPricing;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;
import static com.pricing.engine.JdbcTestSupport.intercept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.time.LocalDate;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Phase3BatchEvaluationH2Tests extends H2TestSupport {

	@ParameterizedTest
	@ValueSource(ints = {0, 1})
	void retriesATemporaryAccountStoreFailure(int failedConnection) throws Exception {
		UUID batchId = UUID.randomUUID();
		addAccount(batchId);
		AtomicInteger connections = new AtomicInteger();
		doAnswer(invocation -> {
			if (connections.getAndIncrement() == failedConnection) {
				throw new SQLTransientConnectionException("Temporary outage", "08001");
			}
			return invocation.callRealMethod();
		}).when(accountDataSource).getConnection();

		service.processPendingAccounts();

		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
		assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
	}

	@ParameterizedTest
	@ValueSource(strings = {"startup", "wake-up", "completion"})
	void persistsRuleEngineErrorsUntilManuallyCleared(String trigger) {
		UUID failed = UUID.randomUUID();
		addAccount(failed);
		String message = trigger.equals("completion") ? null : "Invalid \"rule\"\nvalue";
		var failure = new IllegalStateException(message, new IllegalArgumentException("Bad rule"));
		doAnswer(call -> {
			throw failure;
		}).when(ruleEngine).price(any(RuleEngineBatch.class));

		service.processPendingAccounts();

		String error = accounts.queryForObject(
				"select error from batch_outbox where batch_id = ?", String.class, failed);
		var details = JsonMapper.builder().build().readTree(error);
		assertEquals(3, details.size());
		assertEquals(IllegalStateException.class.getName(), details.get("type").asString());
		if (message == null) {
			assertTrue(details.get("message").isNull());
		} else {
			assertEquals(message, details.get("message").asString());
		}
		assertTrue(details.get("stackTrace").asString().contains(failure.getStackTrace()[0].toString()));
		assertTrue(details.get("stackTrace").asString()
				.contains("Caused by: java.lang.IllegalArgumentException: Bad rule"));
		assertFalse(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, failed));
		assertEquals(List.of(failed), accounts.queryForList("""
				select batch_id from batch_outbox for update skip locked
				""", UUID.class));
		assertEquals(0, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));

		try (var restarted = startApplication()) {
			assertEquals(error, accounts.queryForObject(
					"select error from batch_outbox where batch_id = ?", String.class, failed));
			assertEquals(0, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			if (trigger.equals("completion")) {
				addAccount(UUID.randomUUID());
				MockPricing.pricingRead = () -> {
					accounts.update("update batch_outbox set error = NULL where batch_id = ?", failed);
					MockPricing.pricingRead = () -> {};
				};
				restarted.getBean(EngineService.class).processPendingAccounts();
			} else if (trigger.equals("wake-up")) {
				accounts.update("update batch_outbox set error = NULL where batch_id = ?", failed);
				restarted.getBean(EngineService.class).wakeUp();
				await().atMost(Duration.ofSeconds(3)).until(() -> accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, failed));
			}
		}
		if (trigger.equals("startup")) {
			accounts.update("update batch_outbox set error = NULL where batch_id = ?", failed);
			try (var restarted = startApplication()) {
				assertTrue(restarted.isActive());
				await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
						accounts.queryForObject("select processed from batch_outbox where batch_id = ?",
								Boolean.class, failed)));
			}
		}
		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, failed));
		assertNull(accounts.queryForObject(
				"select error from batch_outbox where batch_id = ?", String.class, failed));
	}

	@ParameterizedTest
	@ValueSource(strings = {"valid", "reclaim", "parallel", "owner", "processed", "error",
			"recommendations", "completion"})
	void retainsRuleEngineErrorAfterAccountTransactionLoss(String outcome) throws Exception {
		boolean heldClaim = Set.of("valid", "parallel").contains(outcome);
		UUID batch = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(batch);
		var repository = application.getBean(AccountRepository.class);
		var retainedRepository = org.mockito.Mockito.mock(AccountRepository.class,
				org.mockito.AdditionalAnswers.delegatesTo(repository));
		var worker = new EngineService(
				retainedRepository, ruleEngine,
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200),
				outcome.equals("valid") ? Duration.ofSeconds(5) : Duration.ofMillis(100),
				outcome.equals("parallel") ? 2 : 1,
				application.getBean("accountTransactionManager", PlatformTransactionManager.class));
		var owner = new AtomicReference<Thread>();
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		var probes = new AtomicInteger();
		var completionFailures = new AtomicInteger();
		var evaluations = new AtomicInteger();
		var lastFailure = new AtomicReference<Long>();
		var firstProbe = new AtomicReference<Long>();
		var failure = new IllegalStateException("Invalid \"rule\"\nvalue",
				new IllegalArgumentException("Bad rule"));
		if (heldClaim) {
			doAnswer(call -> {
				if (unavailable.get()) {
					attempts.incrementAndGet();
					lastFailure.set(System.nanoTime());
					throw new DataAccessResourceFailureException("Error storage unavailable");
				}
				repository.recordError(batch, failure);
				return null;
			}).when(retainedRepository).recordError(batch, failure);
			doAnswer(call -> {
				if (unavailable.get()) {
					probes.incrementAndGet();
					firstProbe.compareAndSet(null, System.nanoTime());
					throw new DataAccessResourceFailureException("Account Store unavailable");
				}
				repository.checkConnection();
				return null;
			}).when(retainedRepository).checkConnection();
		}
		doAnswer(call -> {
			if (!((RuleEngineBatch) call.getArgument(0)).batchId().equals(batch)) {
				return call.callRealMethod();
			}
			owner.set(Thread.currentThread());
			evaluations.incrementAndGet();
			throw failure;
		}).when(ruleEngine).price(any(RuleEngineBatch.class));
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return intercept(connection, Connection.class, (method, arguments, proceed) -> {
				String sql = method.getName().equals("prepareStatement")
						? (String) arguments[0] : "";
				if (!heldClaim && Thread.currentThread() == owner.get()
						&& unavailable.get()
						&& sql.equals("select 1")) {
					probes.incrementAndGet();
					throw new SQLTransientConnectionException("Account Store outage", "08001");
				}
				if (Thread.currentThread() == owner.get() && !unavailable.get()
						&& outcome.equals("completion") && method.getName().equals("commit")
						&& completionFailures.getAndIncrement() < 3) {
					connection.rollback();
					throw new SQLTransientConnectionException("Completion outage", "08001");
				}
				if (!heldClaim && Thread.currentThread() == owner.get()
						&& unavailable.get()
						&& method.getName().equals("commit")) {
					connection.rollback();
					attempts.incrementAndGet();
					throw new SQLTransientConnectionException("Lost error transaction", "08001");
				}
				return proceed.get();
			});
		}).when(accountDataSource).getConnection();
		var available = new DriverManagerDataSource(url("accountdb"), "sa", "");
		try (var lock = available.getConnection();
				var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				await().atMost(Duration.ofSeconds(7)).until(() -> probes.get() > 0);
				assertFalse(processing.isDone());
				assertEquals(1, evaluations.get());
				assertEquals(3, attempts.get());
				if (outcome.equals("valid")) {
					assertTrue(firstProbe.get() - lastFailure.get()
							>= Duration.ofSeconds(5).toNanos());
					lock.setAutoCommit(false);
					try (var claim = lock.prepareStatement(
							"select batch_id from batch_outbox where batch_id = ? for update skip locked")) {
						claim.setObject(1, batch);
						try (var rows = claim.executeQuery()) {
							assertFalse(rows.next());
						}
					}
					lock.rollback();
					lock.setAutoCommit(true);
				}
				addAccount(next);
				worker.wakeUp();
				if (outcome.equals("parallel")) {
					await().atMost(Duration.ofSeconds(3)).until(() -> accounts.queryForObject(
							"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
					assertFalse(processing.isDone());
				} else {
					assertEquals(0, recommendations.queryForObject(
							"select count(*) from recommendation_outbox", Integer.class));
				}
				if (outcome.equals("owner")) {
					lock.setAutoCommit(false);
					try (var claim = lock.prepareStatement(
							"select batch_id from batch_outbox where batch_id = ? for update")) {
						claim.setObject(1, batch);
						try (var rows = claim.executeQuery()) {
							assertTrue(rows.next());
						}
					}
				} else if (outcome.equals("processed")) {
					accounts.update("update batch_outbox set processed = true where batch_id = ?", batch);
				} else if (outcome.equals("error")) {
					accounts.update("update batch_outbox set error = '' where batch_id = ?", batch);
				} else if (outcome.equals("recommendations") || outcome.equals("completion")) {
					recommendationRepository.save(new RuleEngineBatchResult(batch, List.of()));
				}
			} finally {
				unavailable.set(false);
				processing.get(8, TimeUnit.SECONDS);
			}
			assertEquals(1, evaluations.get());
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
			String error = accounts.queryForObject(
					"select error from batch_outbox where batch_id = ?", String.class, batch);
			if (Set.of("reclaim", "valid", "parallel").contains(outcome)) {
				var details = JsonMapper.builder().build().readTree(error);
				assertEquals(failure.getClass().getName(), details.get("type").asString());
				assertEquals(failure.getMessage(), details.get("message").asString());
				assertTrue(details.get("stackTrace").asString().contains("Bad rule"));
			} else if (outcome.equals("error")) {
				assertEquals("", error);
			} else {
				assertNull(error);
			}
			boolean completed = Set.of("processed", "recommendations", "completion").contains(outcome);
			assertEquals(completed, accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, batch));
			assertEquals(Set.of("recommendations", "completion").contains(outcome) ? 2 : 1,
					recommendations.queryForObject(
							"select count(*) from recommendation_outbox", Integer.class));
			if (outcome.equals("completion")) {
				assertTrue(completionFailures.get() >= 4);
			}
			if (outcome.equals("owner")) {
				lock.rollback();
				accounts.update("update batch_outbox set processed = true where batch_id = ?", batch);
			}
		}
	}

	@Test
	void retriesPricingAccessBeforeCalculating() {
		addAccount(UUID.randomUUID());
		AtomicInteger reads = new AtomicInteger();
		MockPricing.pricingRead = () -> {
			if (reads.getAndIncrement() == 0) {
				throw new DataAccessResourceFailureException("Temporary Pricing Store outage");
			}
		};

		service.processPendingAccounts();

		assertEquals(2, reads.get());
		assertTrue(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertNull(accounts.queryForObject("select error from batch_outbox", String.class));
		assertEquals(0, recommendations.queryForObject(
				"select amount from fee_request_recommendations", BigDecimal.class)
						.compareTo(new BigDecimal("2.50")));
	}

	@Test
	void accountStoreOutageIgnoresWakeUpsAndRecoversWithoutAnotherRequest() throws Exception {
		UUID first = UUID.randomUUID();
		UUID second = UUID.randomUUID();
		UUID missed = UUID.randomUUID();
		addAccount(first);
		addAccount(second);
		var availableAccounts = new JdbcTemplate(new DriverManagerDataSource(
				url("accountdb"), "sa", ""));
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		doAnswer(invocation -> {
			attempts.incrementAndGet();
			if (unavailable.get()) {
				throw new SQLTransientConnectionException("Account Store outage", "08001");
			}
			return invocation.callRealMethod();
		}).when(accountDataSource).getConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertFalse(processing.isDone()));
				availableAccounts.update(
						"insert into batch_outbox(batch_id, processed) values (?, false)", missed);
				availableAccounts.update("""
						insert into accounts(batch_id, account_number, product_code,
						branch_code, pricing_date)
						values (?, 'A1', 'CHECKING', 'NY', '2026-01-15')
						""", missed);
				availableAccounts.update("""
						insert into fee_requests(account_id, fee_request_id, code)
						select id, 42, 'SERVICE' from accounts where batch_id = ?
						""", missed);
				for (int i = 0; i < 3; i++) {
					mockMvc.perform(post("/api/v1/work-available"))
							.andExpect(status().isAccepted()).andExpect(content().string(""));
				}
				assertTrue(MockPricing.COMPLETED_BEFORE_EVALUATION.isEmpty());
				assertEquals(3, availableAccounts.queryForObject(
						"select count(*) from batch_outbox where processed = false", Integer.class));
				assertEquals(0, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
			} finally {
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertEquals(Set.of(first, second, missed), Set.copyOf(recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class)));
		assertEquals(List.of(0L, 1L, 2L), MockPricing.COMPLETED_BEFORE_EVALUATION);
		assertEquals(Set.of("batch_id", "processed", "error"),
				Set.copyOf(availableAccounts.queryForList("""
				select column_name from information_schema.columns
				where table_schema = 'public' and table_name = 'batch_outbox'
						""", String.class)));
		int idleConnections = attempts.get();
		await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
				.untilAsserted(() -> assertEquals(idleConnections, attempts.get()));
	}

	@Test
	void accountAndFeeRequestValidationOutcomesDoNotRetry() {
		UUID invalidAccount = UUID.randomUUID();
		addAccount(invalidAccount);
		accounts.update("update accounts set product_code = 'UNKNOWN' where batch_id = ?",
				invalidAccount);
		UUID invalidFee = UUID.randomUUID();
		long accountId = addAccount(invalidFee);
		accounts.update("update fee_requests set code = 'UNKNOWN' where account_id = ?", accountId);

		service.processPendingAccounts();

		assertEquals(2, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from account_recommendations where status = 'PLAN_NOT_FOUND'",
				Integer.class));
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from fee_request_recommendations where status = 'FEE_NOT_FOUND'",
				Integer.class));
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed = true", Integer.class));
	}

	@Test
	void pricingOutageChecksConnectivityBeforeRediscoveringBatches() throws Exception {
		addAccount(UUID.randomUUID());
		var unavailable = new AtomicBoolean(true);
		var failedReads = new AtomicInteger();
		var checks = new AtomicInteger();
		MockPricing.pricingRead = () -> {
			if (unavailable.get()) {
				failedReads.incrementAndGet();
				throw new DataAccessResourceFailureException("Pricing data outage");
			}
		};
		doAnswer(invocation -> {
			checks.incrementAndGet();
			if (unavailable.get()) {
				throw new DataAccessResourceFailureException("Pricing data outage");
			}
			return null;
		}).when(pricingRepository).checkConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> checks.get() >= 2);
				int exhaustedReads = failedReads.get();
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertEquals(exhaustedReads, failedReads.get()));
				addAccount(UUID.randomUUID());
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
				assertEquals(2, accounts.queryForObject(
						"select count(*) from batch_outbox where processed = false", Integer.class));
				assertEquals(0, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
			} finally {
				unavailable.set(false);
				try {
					processing.get(5, TimeUnit.SECONDS);
				} finally {
					doNothing().when(pricingRepository).checkConnection();
					MockPricing.pricingRead = () -> {};
				}
			}
		}
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed = true", Integer.class));
		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
	}

	@Test
	void startupStoresRecommendationsUsingEachAccountsOwnPricingDate() {
		UUID batchId = UUID.randomUUID();
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", batchId);
		addAccount(batchId, "JAN", LocalDate.of(2026, 1, 15));
		addAccount(batchId, "FEB", LocalDate.of(2026, 2, 15));

		service.afterSingletonsInstantiated();
		await().atMost(Duration.ofSeconds(5))
				.until(() -> WAKE_UPS.size() >= 1 && accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
		service.afterSingletonsInstantiated();
		await().atMost(Duration.ofSeconds(5))
				.until(() -> WAKE_UPS.size() >= 2);

		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
		assertEquals("JANPLAN", recommendations.queryForObject(
				"select pricing_plan_code from account_recommendations where account_number = 'JAN'",
				String.class));
		assertEquals("FEBPLAN", recommendations.queryForObject(
				"select pricing_plan_code from account_recommendations where account_number = 'FEB'",
				String.class));
		assertEquals(0, recommendations.queryForObject("""
				select fee.amount from fee_request_recommendations fee
				join account_recommendations account on account.id = fee.account_id
				where account.account_number = 'JAN'
				""", BigDecimal.class).compareTo(new BigDecimal("2.50")));
		assertEquals(0, recommendations.queryForObject("""
				select fee.amount from fee_request_recommendations fee
				join account_recommendations account on account.id = fee.account_id
				where account.account_number = 'FEB'
				""", BigDecimal.class).compareTo(new BigDecimal("3.50")));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
	}
}
