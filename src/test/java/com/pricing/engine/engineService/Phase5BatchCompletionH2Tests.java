package com.pricing.engine.engineService;

import com.pricing.engine.EngineService;
import com.pricing.engine.H2TestSupport;
import com.pricing.engine.MockPricing;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.ruleEngine.RuleEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;
import static com.pricing.engine.JdbcTestSupport.intercept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.time.LocalDate;
import static org.mockito.Mockito.when;

class Phase5BatchCompletionH2Tests extends H2TestSupport {

	@Test
	void recoveryFinishesKnownSourceCompletionWithoutRecalculating() throws Exception {
		addAccount(UUID.randomUUID());
		var availableAccounts = new JdbcTemplate(new DriverManagerDataSource(
				url("accountdb"), "sa", ""));
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		doAnswer(invocation -> {
			if (unavailable.get() && recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class) > 0) {
				attempts.incrementAndGet();
				throw new SQLTransientConnectionException("Source completion outage", "08001");
			}
			Connection connection = (Connection) invocation.callRealMethod();
			return intercept(connection, Connection.class, (method, arguments, proceed) -> {
				if (method.getName().equals("prepareStatement") && unavailable.get()
						&& recommendations.queryForObject(
								"select count(*) from recommendation_outbox", Integer.class) > 0) {
					attempts.incrementAndGet();
					throw new SQLTransientConnectionException("Source completion outage", "08001");
				}
				return proceed.get();
			});
		}).when(accountDataSource).getConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				assertAttempts(1);
				assertFalse(availableAccounts.queryForObject(
						"select processed from batch_outbox", Boolean.class));
				MockPricing.januaryAmount = "9.99";
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
			} finally {
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertTrue(availableAccounts.queryForObject("select processed from batch_outbox", Boolean.class));
		assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(1, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
		assertEquals(0, recommendations.queryForObject(
				"select amount from fee_request_recommendations", BigDecimal.class)
						.compareTo(new BigDecimal("2.50")));
	}

	@ParameterizedTest
	@ValueSource(strings = {"prepareStatement", "commit"})
	void completionRecoveryRetainsItsSlotAfterClaimTransactionLoss(String failedOperation)
			throws Exception {
		var defaults = PropertiesLoaderUtils.loadProperties(
				new ClassPathResource("application.properties"));
		var interval = DurationStyle.detectAndParse(application.getEnvironment().resolvePlaceholders(
				defaults.getProperty("database.recovery-check-interval")));
		assertEquals(Duration.ofSeconds(5), interval);
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), interval, 1,
				application.getBean("accountTransactionManager", PlatformTransactionManager.class));
		UUID retained = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(retained);
		var availableAccounts = new JdbcTemplate(new DriverManagerDataSource(
				url("accountdb"), "sa", ""));
		var owner = new AtomicReference<Thread>();
		var unavailable = new AtomicBoolean(true);
		var lost = new AtomicBoolean();
		var failures = new LinkedBlockingQueue<Long>();
		var probes = new LinkedBlockingQueue<Long>();
		MockPricing.pricingRead = () -> owner.compareAndSet(null, Thread.currentThread());
		doAnswer(invocation -> {
			assertFalse(lost.get() && Thread.currentThread() == owner.get(),
					"Completion recovery must not access recommendations again");
			return invocation.callRealMethod();
		}).when(recommendationDataSource).getConnection();
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return intercept(connection, Connection.class, (method, arguments, proceed) -> {
				if (Thread.currentThread() == owner.get()) {
					String sql = method.getName().equals("prepareStatement")
							? (String) arguments[0] : "";
					assertFalse(lost.get() && sql.contains("skip locked"),
							"Completion recovery must not reclaim the batch");
					if (unavailable.get() && sql.equals("select 1")) {
						probes.add(System.nanoTime());
						throw new SQLTransientConnectionException("Account Store outage", "08001");
					}
					if (unavailable.get() && method.getName().equals(failedOperation)) {
						connection.rollback();
						lost.set(true);
						failures.add(System.nanoTime());
						throw new SQLTransientConnectionException("Lost Account transaction", "08001");
					}
				}
				return proceed.get();
			});
		}).when(accountDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				Long first = failures.poll(3, TimeUnit.SECONDS);
				Long second = failures.poll(3, TimeUnit.SECONDS);
				Long third = failures.poll(3, TimeUnit.SECONDS);
				assertTrue(first != null && second != null && third != null);
				assertTrue(second - first >= Duration.ofMillis(200).toNanos());
				assertTrue(third - second >= Duration.ofMillis(200).toNanos());
				Long probe = probes.poll(7, TimeUnit.SECONDS);
				assertTrue(probe != null);
				assertTrue(probe - third >= interval.toNanos());
				assertTrue(failures.isEmpty());
				assertFalse(processing.isDone());
				assertFalse(availableAccounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, retained));
				assertEquals(List.of(retained), availableAccounts.queryForList(
						"select batch_id from batch_outbox for update skip locked", UUID.class));
				var committed = storedRecommendations(recommendations);
				MockPricing.januaryAmount = "9.99";
				addAccount(next);
				worker.wakeUp();
				assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
				assertEquals(committed, storedRecommendations(recommendations));
			} finally {
				unavailable.set(false);
				processing.get(12, TimeUnit.SECONDS);
			}
		}
		assertEquals(2, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(2, availableAccounts.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
	}

	@Test
	void completionRecoveryDoesNotPauseOtherWorkersOrReleaseItsSlot() throws Exception {
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), Duration.ofMillis(100), 2,
				application.getBean("accountTransactionManager", PlatformTransactionManager.class));
		UUID retained = UUID.randomUUID();
		addAccount(retained);
		var owner = new AtomicReference<Thread>();
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		var entered = new LinkedBlockingQueue<Boolean>();
		var permits = new Semaphore(0);
		MockPricing.pricingRead = () -> {
			if (!owner.compareAndSet(null, Thread.currentThread())) {
				entered.add(true);
				try {
					assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(ex);
				}
			}
		};
		doAnswer(invocation -> {
			if (Thread.currentThread() == owner.get() && unavailable.get()) {
				attempts.incrementAndGet();
				throw new SQLTransientConnectionException("Worker-local Account outage", "08001");
			}
			Connection connection = (Connection) invocation.callRealMethod();
			return intercept(connection, Connection.class, (method, arguments, proceed) -> {
				if (Thread.currentThread() == owner.get() && unavailable.get()
						&& method.getName().equals("commit")) {
					connection.rollback();
					attempts.incrementAndGet();
					throw new SQLTransientConnectionException("Lost Account transaction", "08001");
				}
				return proceed.get();
			});
		}).when(accountDataSource).getConnection();
		var availableAccounts = new DriverManagerDataSource(url("accountdb"), "sa", "");
		try (var lock = availableAccounts.getConnection();
				var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 4);
				// Another instance may own the row after the original Account transaction is lost.
				lock.setAutoCommit(false);
				try (var claim = lock.prepareStatement(
						"select batch_id from batch_outbox where batch_id = ? for update")) {
					claim.setObject(1, retained);
					try (var rows = claim.executeQuery()) {
						assertTrue(rows.next());
					}
				}
				MockPricing.januaryAmount = "9.99";
				addAccount(UUID.randomUUID());
				addAccount(UUID.randomUUID());
				worker.wakeUp();
				assertEquals(Boolean.TRUE, entered.poll(3, TimeUnit.SECONDS));
				worker.wakeUp();
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				assertEquals(Boolean.TRUE, entered.poll(3, TimeUnit.SECONDS));
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertEquals(2,
						accounts.queryForObject(
								"select count(*) from batch_outbox where processed", Integer.class)));
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, retained));
				assertFalse(processing.isDone());
			} finally {
				permits.release(3);
				lock.rollback();
				unavailable.set(false);
				processing.get(5, TimeUnit.SECONDS);
			}
		}
		assertEquals(3, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(3, accounts.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
	}

	@Test
	void completesEachAccountBatchBeforeEvaluatingTheNext() {
		for (String accountNumber : List.of("FIRST", "SECOND")) {
			UUID batchId = UUID.randomUUID();
			accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)",
					batchId);
			addAccount(batchId, accountNumber, LocalDate.of(2026, 1, 15));
		}

		service.processPendingAccounts();

		assertEquals(List.of(0L, 1L), MockPricing.COMPLETED_BEFORE_EVALUATION);
		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
	}
}
