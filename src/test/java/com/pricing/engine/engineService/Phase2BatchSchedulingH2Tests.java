package com.pricing.engine.engineService;

import com.pricing.engine.EngineService;
import com.pricing.engine.utils.H2Fixture;
import com.pricing.engine.utils.MockPricing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;
import static com.pricing.engine.utils.JdbcUtils.intercept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.time.LocalDate;
import static org.mockito.Mockito.when;

class Phase2BatchSchedulingH2Tests extends H2Fixture {

	@Test
	void workersDrainBatchesAndRespectCapAcrossWakeUps() throws Exception {
		try (var application = startApplication("--engine.max-workers=3")) {
			EngineService service = application.getBean(EngineService.class);
			var entered = new LinkedBlockingQueue<Boolean>();
			var permits = new Semaphore(0);
			var active = new AtomicInteger();
			var peak = new AtomicInteger();
			MockPricing.pricingRead = () -> {
				peak.accumulateAndGet(active.incrementAndGet(), Math::max);
				entered.add(true);
				try {
					assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(ex);
				} finally {
					active.decrementAndGet();
				}
			};
			for (int i = 0; i < 4; i++) {
				addAccount(UUID.randomUUID());
			}
			try {
				for (int i = 0; i < 3; i++) {
					service.wakeUp();
					assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
					assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				}
				service.wakeUp();
				assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
				permits.release();
				assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
				assertEquals(3, active.get());
				for (int i = 0; i < 12; i++) {
					addAccount(UUID.randomUUID());
				}
				try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
					for (int i = 0; i < 20; i++) {
						requests.submit(service::wakeUp);
					}
					permits.release(20);
				}
				await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertEquals(16,
						accounts.queryForObject("select count(*) from batch_outbox where processed",
								Integer.class)));
				assertEquals(3, peak.get());
				assertEquals(16, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
				assertEquals(16, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
			} finally {
				permits.release(30);
			}
		}
	}

	@Test
	void competingInstancesRespectConfiguredCapsAndKeepClaimsUntilCompletion() throws Exception {
		try (var primary = startApplication("--engine.max-workers=3");
				var other = startApplication("--engine.max-workers=2")) {
			EngineService service = primary.getBean(EngineService.class);
			var entered = new LinkedBlockingQueue<Boolean>();
			var permits = new Semaphore(0);
			MockPricing.pricingRead = () -> {
				entered.add(true);
				try {
					assertTrue(permits.tryAcquire(10, TimeUnit.SECONDS));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(ex);
				}
			};
			UUID locked = UUID.randomUUID();
			UUID errored = UUID.randomUUID();
			UUID processed = UUID.randomUUID();
			addAccount(locked);
			addAccount(errored);
			addAccount(processed);
			accounts.update("update batch_outbox set error = '' where batch_id = ?", errored);
			accounts.update("update batch_outbox set processed = true where batch_id = ?", processed);
			for (int i = 0; i < 6; i++) {
				addAccount(UUID.randomUUID());
			}
			try (var connection = new DriverManagerDataSource(url("accountdb"), "sa", "").getConnection()) {
				connection.setAutoCommit(false);
				try (var statement = connection.prepareStatement(
						"select batch_id from batch_outbox where batch_id = ? for update")) {
					statement.setObject(1, locked);
					statement.executeQuery().close();
				}
				try {
					var second = other.getBean(EngineService.class);
					for (int i = 0; i < 2; i++) {
						second.wakeUp();
						assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
					}
					second.wakeUp();
					assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
					for (int i = 0; i < 3; i++) {
						service.wakeUp();
						assertEquals(Boolean.TRUE, entered.poll(5, TimeUnit.SECONDS));
					}
					assertNull(entered.poll(200, TimeUnit.MILLISECONDS));
					assertEquals(1, accounts.queryForList("""
							select batch_id from batch_outbox
							where not processed and error is null for update skip locked
							""", UUID.class).size());
				} finally {
					permits.release(20);
				}
				await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertEquals(6,
						recommendations.queryForObject(
								"select count(*) from recommendation_outbox", Integer.class)));
				assertEquals(6, MockPricing.COMPLETED_BEFORE_EVALUATION.size());
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, locked));
				connection.rollback();
			}
			service.processPendingAccounts();
			assertTrue(accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, locked));
			assertFalse(accounts.queryForObject(
					"select processed from batch_outbox where batch_id = ?", Boolean.class, errored));
			assertEquals(7, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
		}
	}

	@Test
	void ignoresProcessedAndErroredBatches() {
		UUID eligible = UUID.randomUUID();
		UUID errored = UUID.randomUUID();
		UUID processed = UUID.randomUUID();
		addAccount(eligible);
		addAccount(errored);
		addAccount(processed);
		accounts.update("update batch_outbox set error = '' where batch_id = ?", errored);
		accounts.update("update batch_outbox set processed = true where batch_id = ?", processed);

		service.processPendingAccounts();

		assertEquals(List.of(eligible), recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class));
		assertFalse(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, errored));
	}

	@Test
	void ruleEngineFailureReleasesCapacityWhileAnotherWorkerKeepsItsClaim() throws Exception {
		var worker = new EngineService(
				application.getBean(AccountRepository.class), ruleEngine,
				recommendationRepository, pricingRepository, application.getBean(RestClient.class),
				3, Duration.ofMillis(50), 3, Duration.ofMillis(200), Duration.ofMillis(100), 2,
				application.getBean("accountTransactionManager",
						PlatformTransactionManager.class));
		UUID held = UUID.randomUUID();
		UUID failed = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(held);
		var heldEntered = new CountDownLatch(1);
		var failedEntered = new CountDownLatch(1);
		var releaseHeld = new CountDownLatch(1);
		var releaseFailure = new CountDownLatch(1);
		doAnswer(call -> {
			UUID batchId = call.getArgument(0, RuleEngineBatch.class).batchId();
			if (batchId.equals(held)) {
				heldEntered.countDown();
				assertTrue(releaseHeld.await(5, TimeUnit.SECONDS));
			} else if (batchId.equals(failed)) {
				failedEntered.countDown();
				assertTrue(releaseFailure.await(5, TimeUnit.SECONDS));
				throw new IllegalArgumentException("Invalid rule");
			}
			return call.callRealMethod();
		}).when(ruleEngine).price(any(RuleEngineBatch.class));

		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(worker::processPendingAccounts);
			try {
				assertTrue(heldEntered.await(3, TimeUnit.SECONDS));
				addAccount(failed);
				worker.wakeUp();
				assertTrue(failedEntered.await(3, TimeUnit.SECONDS));
				addAccount(next);
				releaseFailure.countDown();
				await().atMost(Duration.ofSeconds(3)).until(() -> accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
				assertEquals(List.of(failed), accounts.queryForList("""
						select batch_id from batch_outbox where error is not null
						and processed = false for update skip locked
						""", UUID.class));
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, held));
				assertTrue(accounts.queryForList("""
						select batch_id from batch_outbox where batch_id = ? for update skip locked
						""", UUID.class, held).isEmpty());
				assertFalse(processing.isDone());
			} finally {
				releaseFailure.countDown();
				releaseHeld.countDown();
				processing.get(5, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void wakeUpsDoNotStartAnotherBatchDuringTheCurrentRetryDelay() throws Exception {
		addAccount(UUID.randomUUID());
		AtomicInteger connections = new AtomicInteger();
		var retrying = new CompletableFuture<Long>();
		var failedAt = new AtomicReference<Long>();
		var retryGate = new CountDownLatch(1);
		doAnswer(invocation -> {
			int attempt = connections.getAndIncrement();
			if (attempt == 0) {
				failedAt.set(System.nanoTime());
				throw new SQLTransientConnectionException("Temporary outage", "08001");
			}
			if (attempt == 1) {
				retrying.complete(System.nanoTime() - failedAt.get());
				assertTrue(retryGate.await(5, TimeUnit.SECONDS));
			}
			return invocation.callRealMethod();
		}).when(accountDataSource).getConnection();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var run = work.submit(service::processPendingAccounts);
			try {
				assertTrue(retrying.get(5, TimeUnit.SECONDS) >= Duration.ofMillis(200).toNanos());
				addAccount(UUID.randomUUID());
				service.wakeUp();
				assertTrue(MockPricing.COMPLETED_BEFORE_EVALUATION.isEmpty());
			} finally {
				retryGate.countDown();
			}
			run.get(5, TimeUnit.SECONDS);
		}
		assertEquals(List.of(0L, 1L), MockPricing.COMPLETED_BEFORE_EVALUATION);
	}

	@ParameterizedTest(name = "parallel wake-ups={0}")
	@ValueSource(booleans = {false, true})
	void duplicateWakeUpsShareActiveRunAndDrainNewBatches(boolean parallel) throws Exception {
		UUID first = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		addAccount(first);
		var evaluating = new CompletableFuture<Void>();
		var resume = new CompletableFuture<Void>();
		MockPricing.pricingRead = () -> {
			evaluating.complete(null);
			resume.join();
		};

		try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
			try {
				mockMvc.perform(post("/api/v1/work-available"))
						.andExpect(status().isAccepted()).andExpect(content().string(""));
				evaluating.get(5, TimeUnit.SECONDS);
				addAccount(next);
				var processing = requests.submit(service::processPendingAccounts);
				var wakeUps = new ArrayList<Future<?>>();
				for (int i = 0; i < 8; i++) {
					if (parallel) {
						wakeUps.add(requests.submit(() -> {
							mockMvc.perform(post("/api/v1/work-available"))
									.andExpect(status().isAccepted());
							return null;
						}));
					} else {
						mockMvc.perform(post("/api/v1/work-available"))
								.andExpect(status().isAccepted());
					}
				}
				for (var wakeUp : wakeUps) {
					wakeUp.get(5, TimeUnit.SECONDS);
				}
				processing.get(2, TimeUnit.SECONDS);
			} finally {
				resume.complete(null);
			}
		}

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(2,
				accounts.queryForObject("select count(*) from batch_outbox where processed = true",
						Integer.class)));
		assertEquals(Set.of(first, next), Set.copyOf(recommendations.queryForList(
				"select batch_id from recommendation_outbox", UUID.class)));
		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
		assertEquals(List.of(0L, 1L), MockPricing.COMPLETED_BEFORE_EVALUATION);
	}

	@Test
	void idleEngineWaitsForAWakeUpInsteadOfPolling() throws Exception {
		service.processPendingAccounts();
		UUID batchId = UUID.randomUUID();
		addAccount(batchId);

		await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
			assertFalse(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
			assertEquals(0, recommendations.queryForObject(
						"select count(*) from recommendation_outbox", Integer.class));
		});
		mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
				accounts.queryForObject("select processed from batch_outbox where batch_id = ?",
						Boolean.class, batchId)));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
	}

	@Test
	void emptyWorkerReleasesItsSlotAfterTheAccountTransactionFinishes() throws Exception {
		var committing = new CompletableFuture<Void>();
		var finishCommit = new CountDownLatch(1);
		var firstCommit = new AtomicBoolean(true);
		doAnswer(invocation -> {
			Connection connection = (Connection) invocation.callRealMethod();
			return intercept(connection, Connection.class, (method, arguments, proceed) -> {
				if (method.getName().equals("commit") && firstCommit.getAndSet(false)) {
					committing.complete(null);
					assertTrue(finishCommit.await(5, TimeUnit.SECONDS));
				}
				return proceed.get();
			});
		}).when(accountDataSource).getConnection();
		UUID batchId = UUID.randomUUID();
		try (var work = Executors.newVirtualThreadPerTaskExecutor()) {
			var processing = work.submit(service::processPendingAccounts);
			Future<?> wakeUp = null;
			try {
				committing.get(5, TimeUnit.SECONDS);
				await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
						.untilAsserted(() -> assertFalse(processing.isDone()));
				addAccount(batchId);
				var received = new CountDownLatch(1);
				wakeUp = work.submit(() -> {
					received.countDown();
					service.wakeUp();
				});
				assertTrue(received.await(5, TimeUnit.SECONDS));
				Future<?> pendingWakeUp = wakeUp;
				await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
						.untilAsserted(() -> assertFalse(pendingWakeUp.isDone()));
				assertFalse(accounts.queryForObject(
						"select processed from batch_outbox", Boolean.class));
			} finally {
				finishCommit.countDown();
				processing.get(5, TimeUnit.SECONDS);
			}
			wakeUp.get(5, TimeUnit.SECONDS);
		}
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
				accounts.queryForObject("select processed from batch_outbox where batch_id = ?",
						Boolean.class, batchId)));
	}

	@Test
	void wakeUpOverlappingAnEmptyScanDoesNotStrandCommittedWork() throws Exception {
		var scannedEmpty = new CompletableFuture<Void>();
		var finishScan = new CompletableFuture<Void>();
		EMPTY_SCAN.set(() -> {
			scannedEmpty.complete(null);
			finishScan.join();
		});
		doAnswer(invocation -> delayEmptyScan(invocation.callRealMethod()))
				.when(accountDataSource).getConnection();
		UUID batchId = UUID.randomUUID();

		try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
			try {
				mockMvc.perform(post("/api/v1/work-available")).andExpect(status().isAccepted());
				scannedEmpty.get(5, TimeUnit.SECONDS);
				addAccount(batchId);
				var received = new CompletableFuture<Void>();
				var wakeUp = requests.submit(() -> {
					mockMvc.perform(post("/api/v1/work-available").with(request -> {
						received.complete(null);
						return request;
					})).andExpect(status().isAccepted());
					return null;
				});
				received.get(5, TimeUnit.SECONDS);
				await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1))
						.untilAsserted(() -> assertFalse(wakeUp.isDone()));
				finishScan.complete(null);
				wakeUp.get(5, TimeUnit.SECONDS);
			} finally {
				finishScan.complete(null);
			}
		}

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertTrue(
				accounts.queryForObject("select processed from batch_outbox where batch_id = ?",
						Boolean.class, batchId)));
		assertEquals(batchId, recommendations.queryForObject(
				"select batch_id from recommendation_outbox", UUID.class));
	}

	static Object delayEmptyScan(Object target) {
		// Delay the JDBC result after H2 has found no rows, before the worker exits.
		Class<?> type = target instanceof Connection ? Connection.class
				: target instanceof PreparedStatement ? PreparedStatement.class : ResultSet.class;
		return intercept(target, type, (method, arguments, proceed) -> {
			Object result = proceed.get();
			if (target instanceof Connection && result instanceof PreparedStatement
					&& arguments[0] instanceof String sql
					&& sql.contains("batch_outbox")) {
				return delayEmptyScan(result);
			}
			if (result instanceof ResultSet) {
				return delayEmptyScan(result);
			}
			if (target instanceof ResultSet && method.getName().equals("next")
					&& Boolean.FALSE.equals(result)) {
				Runnable gate = EMPTY_SCAN.getAndSet(null);
				if (gate != null) {
					gate.run();
				}
			}
			return result;
		});
	}

	@Test
	void wakeUpAcceptsNoPayloadAndStoresPendingRecommendations() throws Exception {
		UUID batchId = UUID.randomUUID();
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", batchId);
		addAccount(batchId, "WAKE", LocalDate.of(2026, 1, 15));

		mockMvc.perform(post("/api/v1/work-available"))
				.andExpect(status().isAccepted()).andExpect(content().string(""));

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
			assertTrue(accounts.queryForObject(
						"select processed from batch_outbox where batch_id = ?", Boolean.class, batchId));
			assertEquals(batchId, recommendations.queryForObject(
						"select batch_id from recommendation_outbox", UUID.class));
		});
	}

	@Test
	void drainsBatchesPersistedDuringEvaluationWithoutAnotherWakeUp() {
		UUID first = UUID.randomUUID();
		UUID next = UUID.randomUUID();
		accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", first);
		addAccount(first, "FIRST", LocalDate.of(2026, 1, 15));
		MockPricing.pricingRead = () -> {
			MockPricing.pricingRead = () -> {};
			accounts.update("insert into batch_outbox(batch_id, processed) values (?, false)", next);
			addAccount(next, "NEXT", LocalDate.of(2026, 1, 15));
		};

		service.processPendingAccounts();

		assertEquals(2, recommendations.queryForObject(
				"select count(*) from recommendation_outbox", Integer.class));
		assertEquals(List.of(0L, 1L), MockPricing.COMPLETED_BEFORE_EVALUATION);
		assertTrue(accounts.queryForObject(
				"select processed from batch_outbox where batch_id = ?", Boolean.class, next));
	}

}
