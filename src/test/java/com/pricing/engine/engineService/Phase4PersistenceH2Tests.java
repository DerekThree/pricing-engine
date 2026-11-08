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
import java.util.concurrent.CopyOnWriteArrayList;
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

class Phase4PersistenceH2Tests extends H2TestSupport {

	@Test
	void recoveryStoresRetainedResultsBeforeFreshAndSubsequentScans() throws Exception {
		UUID retained = UUID.randomUUID();
		UUID missed = UUID.randomUUID();
		UUID subsequent = UUID.randomUUID();
		addAccount(retained);
		var availableRecommendations = new JdbcTemplate(new DriverManagerDataSource(
				url("recommendationdb"), "sa", ""));
		var unavailable = new AtomicBoolean(true);
		var attempts = new AtomicInteger();
		doAnswer(invocation -> {
			if (!MockPricing.COMPLETED_BEFORE_EVALUATION.isEmpty()) {
				attempts.incrementAndGet();
				if (unavailable.get()) {
					throw new SQLTransientConnectionException("Recommendation Store outage", "08001");
				}
			}
			return invocation.callRealMethod();
		}).when(recommendationDataSource).getConnection();

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 3);
				await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertFalse(processing.isDone()));
				assertEquals(1, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
				assertEquals(0, availableRecommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
				assertFalse(accounts.queryForObject("select processed from batch_outbox", Boolean.class));
				MockPricing.januaryAmount = "9.99";
				addAccount(missed);
				var addSubsequent = new AtomicBoolean(true);
				MockPricing.pricingRead = () -> {
					if (addSubsequent.getAndSet(false)) {
						assertEquals(0, recommendations.queryForObject(
								"select amount from fee_request_recommendations", BigDecimal.class)
										.compareTo(new BigDecimal("2.50")));
						assertTrue(accounts.queryForObject(
								"select processed from batch_outbox where batch_id = ?",
								Boolean.class, retained));
						addAccount(subsequent);
					}
				};
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
			} finally {
				unavailable.set(false);
				try {
					processing.get(5, TimeUnit.SECONDS);
				} finally {
					MockPricing.pricingRead = () -> {};
				}
			}
		}
		assertEquals(Set.of(retained, missed, subsequent), Set.copyOf(recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class)));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
		assertEquals(List.of(0L, 1L, 2L), MockPricing.COMPLETED_BEFORE_EVALUATION);
	}

	@Test
	void storageRecoveryUsesFiveSecondChecksAndKeepsItsResultClaimAndSlot() throws Exception {
		var defaults = PropertiesLoaderUtils.loadProperties(
				new ClassPathResource("application.properties"));
		var interval = DurationStyle.detectAndParse(application.getEnvironment().resolvePlaceholders(
				defaults.getProperty("database.recovery-check-interval")));
		assertEquals(Duration.ofSeconds(5), interval);
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), interval, 1,
				application.getBean("accountTransactionManager",
						PlatformTransactionManager.class));
		UUID retained = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(retained);
		var calculated = new AtomicBoolean();
		var unavailable = new AtomicBoolean(true);
		var attempts = new LinkedBlockingQueue<Long>();
		var queries = new CopyOnWriteArrayList<String>();
		MockPricing.pricingRead = () -> calculated.set(true);
		doAnswer(invocation -> {
			if (calculated.get()) {
				attempts.add(System.nanoTime());
				if (unavailable.get()) {
					throw new SQLTransientConnectionException("Recommendation Store outage", "08001");
				}
			}
			Connection connection = (Connection) invocation.callRealMethod();
			return intercept(connection, Connection.class, (method, arguments, proceed) -> {
				if (calculated.get() && method.getName().equals("prepareStatement")) {
					queries.add((String) arguments[0]);
				}
				return proceed.get();
			});
		}).when(recommendationDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				Long first = attempts.poll(3, TimeUnit.SECONDS);
				Long second = attempts.poll(3, TimeUnit.SECONDS);
				Long third = attempts.poll(3, TimeUnit.SECONDS);
				assertTrue(first != null && second != null && third != null);
				assertTrue(second - first >= Duration.ofMillis(200).toNanos());
				assertTrue(third - second >= Duration.ofMillis(200).toNanos());
				Long probe = attempts.poll(7, TimeUnit.SECONDS);
				assertTrue(probe != null);
				assertTrue(probe - third >= interval.toNanos());
				assertFalse(processing.isDone());
				assertTrue(queries.isEmpty());
				assertTrue(accounts.queryForList("""
						select batch_id from batch_outbox where batch_id = ?
						for update skip locked
						""", UUID.class, retained).isEmpty());
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, retained));
				MockPricing.januaryAmount = "9.99";
				addAccount(next);
				worker.wakeUp();
				await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
						.untilAsserted(() -> assertEquals(1,
								MockPricing.COMPLETED_BEFORE_EVALUATION.size()));
				unavailable.set(false);
				Long restored = attempts.poll(7, TimeUnit.SECONDS);
				assertTrue(restored != null);
				assertTrue(restored - probe >= interval.toNanos());
				processing.get(5, TimeUnit.SECONDS);
			} finally {
				unavailable.set(false);
				processing.get(8, TimeUnit.SECONDS);
			}
		}
		assertEquals("select 1", queries.getFirst());
		assertTrue(queries.get(1).startsWith("insert into recommendation_outbox"));
		assertEquals(2, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
		assertEquals(2, accounts.queryForObject(
				"select count(*) from batch_outbox where processed", Integer.class));
		assertEquals(List.of(new BigDecimal("2.50"), new BigDecimal("9.99")),
				recommendations.queryForList(
						"select amount from fee_request_recommendations order by amount", BigDecimal.class)
						.stream().map(amount -> amount.setScale(2)).toList());
	}

	@Test
	void recoveringStorageDoesNotPauseOtherWorkersOrReleaseItsSlot() throws Exception {
		var worker = new EngineService(
				application.getBean(AccountRepository.class), application.getBean(RuleEngine.class),
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), Duration.ofMillis(100), 2,
				application.getBean("accountTransactionManager",
						PlatformTransactionManager.class));
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
				throw new SQLTransientConnectionException("Worker-local storage outage", "08001");
			}
			return invocation.callRealMethod();
		}).when(recommendationDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				await().atMost(Duration.ofSeconds(3)).until(() -> attempts.get() >= 4);
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
				assertTrue(accounts.queryForList("""
						select batch_id from batch_outbox where batch_id = ?
						for update skip locked
						""", UUID.class, retained).isEmpty());
				assertFalse(processing.isDone());
			} finally {
				permits.release(3);
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
}
