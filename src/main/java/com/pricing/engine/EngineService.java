package com.pricing.engine;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.function.BooleanSupplier;

import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.pricingStore.PricingRepository;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.PricingAccessException;
import com.pricing.engine.ruleEngine.RuleEngine;
import com.pricing.engine.ruleEngine.RuleEngineBatch;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Service
public class EngineService implements SmartInitializingSingleton {

	private static final Logger log = LoggerFactory.getLogger(EngineService.class);

	private final AccountRepository accountRepository;
	private final RuleEngine ruleEngine;

	private final PricingRepository pricingRepository;
	private final RecommendationRepository recommendationRepository;
	private final RestClient transactionProcessorClient;
	private final int maxAttempts;
	private final Duration retryDelay;
	private final int databaseMaxAttempts;
	private final Duration databaseRetryDelay;

	private final Duration databaseRecoveryCheckInterval;
	private final int maxWorkers;
	private final PlatformTransactionManager accountTransactionManager;
	private final ReentrantLock processingLock = new ReentrantLock();
	private int workers;
	private CompletableFuture<Void> completion = CompletableFuture.completedFuture(null);

	public EngineService(AccountRepository accountRepository, RuleEngine ruleEngine,
			RecommendationRepository recommendationRepository, PricingRepository pricingRepository,
			RestClient transactionProcessorClient,
			@Value("${transaction-processor.max-attempts}") int maxAttempts,
			@Value("${transaction-processor.retry-delay}") Duration retryDelay,
			@Value("${database.max-attempts}") int databaseMaxAttempts,
			@Value("${database.retry-delay}") Duration databaseRetryDelay,
			@Value("${database.recovery-check-interval}") Duration databaseRecoveryCheckInterval,
			@Value("${engine.max-workers:3}") int maxWorkers,
			@Qualifier("accountTransactionManager") PlatformTransactionManager transactionManager) {
		Assert.isTrue(maxWorkers > 0, "Worker limit must be positive");
		this.maxWorkers = maxWorkers;
		this.accountTransactionManager = transactionManager;
		Assert.isTrue(maxAttempts > 0, "Transaction Processor attempt limit must be positive");
		Assert.isTrue(!retryDelay.isNegative(),
				"Transaction Processor retry delay must not be negative");
		Assert.isTrue(databaseMaxAttempts > 0, "Database attempt limit must be positive");
		Assert.isTrue(!databaseRetryDelay.isNegative(), "Database retry delay must not be negative");
		Assert.isTrue(!databaseRecoveryCheckInterval.isNegative()
				&& !databaseRecoveryCheckInterval.isZero(),
				"Database recovery check interval must be positive");
		this.databaseRecoveryCheckInterval = databaseRecoveryCheckInterval;
		this.databaseMaxAttempts = databaseMaxAttempts;
		this.databaseRetryDelay = databaseRetryDelay;
		this.accountRepository = accountRepository;
		this.ruleEngine = ruleEngine;
		this.pricingRepository = pricingRepository;
		this.recommendationRepository = recommendationRepository;
		this.transactionProcessorClient = transactionProcessorClient;
		this.maxAttempts = maxAttempts;
		this.retryDelay = retryDelay;
	}

	@Override
	public void afterSingletonsInstantiated() {
		Thread.startVirtualThread(() -> {
			if (retryDatabaseOperation(recommendationRepository::existsByProcessedFalse,
					recommendationRepository::checkConnection)) {
				Thread.startVirtualThread(this::notifyTransactionProcessor);
			}
			processPendingAccounts();
		});
	}

	public void processPendingAccounts() {
		CompletableFuture<Void> run = startWorkers(maxWorkers);
		if (run != null) {
			try {
				run.join();
			} catch (CompletionException ex) {
				if (ex.getCause() instanceof RuntimeException failure) {
					throw failure;
				}
				throw (Error) ex.getCause();
			}
		}
	}

	public void wakeUp() {
		startWorkers(1);
	}

	public void notifyTransactionProcessor() {
		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			String failure;
			try {
				HttpStatusCode status = transactionProcessorClient.post().retrieve().toBodilessEntity()
						.getStatusCode();
				if (status.is2xxSuccessful()) {
					log.info("Transaction Processor wake-up acknowledged");
					return;
				}
				failure = "HTTP " + status.value();
			} catch (RestClientException ex) {
				failure = ex.getClass().getSimpleName() + ": " + ex.getMessage();
			}
			if (attempt == maxAttempts) {
				log.error("Final Transaction Processor wake-up attempt failed. {}. Delivery stopped",
						failure);
				return;
			}
			log.warn("Transaction Processor wake-up attempt {} of {} failed. {}.",
					attempt, maxAttempts, failure);
			try {
				Thread.sleep(retryDelay);
			} catch (InterruptedException ex) {
				log.warn("Transaction Processor wake-up retry interrupted after attempt {} of {}",
						attempt, maxAttempts);
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	private CompletableFuture<Void> startWorkers(int limit) {
		processingLock.lock();
		try {
			if (workers == maxWorkers) {
				return null;
			}
			if (workers == 0) {
				completion = new CompletableFuture<>();
			}
			int count = Math.min(limit, maxWorkers - workers);
			for (int i = 0; i < count; i++) {
				workers++;
				Thread.startVirtualThread(new Worker(completion)::run);
			}
			return completion;
		} finally {
			processingLock.unlock();
		}
	}

	private void releaseWorker() {
		processingLock.lock();
		try {
			workers--;
			if (workers == 0) {
				completion.complete(null);
			}
		} finally {
			processingLock.unlock();
		}
	}

	private class Worker {
		private final CompletableFuture<Void> runCompletion;
		private TransactionStatus transaction;
		private UUID batchId;

		Worker(CompletableFuture<Void> runCompletion) {
			this.runCompletion = runCompletion;
		}

		void run() {
			try {
				while (processNextBatch()) { }
			} catch (RuntimeException | Error ex) {
				runCompletion.completeExceptionally(ex);
				releaseWorker();
				log.error("Batch worker failed", ex);
			}
		}

		private boolean processNextBatch() {
			try {
				RuleEngineBatch batch = retryAccountOperation(this::claimAndLoad);
				if (batchId == null) {
					return false;
				}
				if (batch != null) {
					RuleEngineBatchResult result;
					try {
						result = retryDatabaseOperation(() -> ruleEngine.price(batch),
								pricingRepository::checkConnection);
					} catch (RuntimeException ex) {
						if (isTemporaryDatabaseFailure(ex)) {
							throw ex;
						}
						log.error("Rule-engine evaluation failed for batch {}", batchId, ex);
						recordEvaluationError(ex);
						return true;
					}
					storeResult(result);
				}
				completeBatch();
				return true;
			} finally {
				rollbackAccountTransaction();
			}
		}

		private RuleEngineBatch claimAndLoad() {
			processingLock.lock();
			batchId = accountRepository.claimNextBatchId();
			if (batchId == null) {
				commitAccountTransaction();
				releaseWorker();
				return null;
			}
			processingLock.unlock();
			log.info("Pricing Batch received: {}", batchId);
			if (retryDatabaseOperation(() -> recommendationRepository.existsByBatchId(batchId),
					recommendationRepository::checkConnection)) {
				return null;
			}
			return accountRepository.load(batchId);
		}

		private void recordEvaluationError(RuntimeException failure) {
			TransactionStatus claim = transaction;
			boolean recommendationsStored = retryAccountOperation(() -> {
				if (transaction != claim) {
					if (accountRepository.claimBatchId(batchId) == null) {
						commitAccountTransaction();
						return false;
					}
					if (retryDatabaseOperation(() -> recommendationRepository.existsByBatchId(batchId),
							recommendationRepository::checkConnection)) {
						return true;
					}
				}
				retryDatabaseOperation(() -> {
					accountRepository.recordError(batchId, failure);
					return null;
				}, accountRepository::checkConnection, () -> !transaction.isRollbackOnly());
				commitAccountTransaction();
				return false;
			});
			if (recommendationsStored) {
				completeBatch();
			}
		}

		private void storeResult(RuleEngineBatchResult result) {
			retryDatabaseOperation(() -> {
				try {
					recommendationRepository.save(result);
				} catch (DataIntegrityViolationException ex) {
					Throwable cause = ex;
					while (cause != null) {
						if (cause instanceof SQLException sqlException
								&& "23505".equals(sqlException.getSQLState())
								&& sqlException.getMessage() != null) {
							String message = sqlException.getMessage().toLowerCase(Locale.ROOT);
							if (message.contains("recommendation_outbox_pkey")) {
								return null;
							}
						}
						cause = cause.getCause();
					}
					throw ex;
				}
				return null;
			}, recommendationRepository::checkConnection);
			log.info("Pricing Batch stored: {}", result.batchId());
			Thread.startVirtualThread(EngineService.this::notifyTransactionProcessor);
		}

		private void completeBatch() {
			retryAccountOperation(() -> {
				accountRepository.markProcessed(batchId);
				commitAccountTransaction();
				return null;
			});
		}

		private <T> T retryAccountOperation(Supplier<T> operation) {
			return retryDatabaseOperation(() -> {
				try {
					if (transaction == null) {
						transaction = accountTransactionManager.getTransaction(
								new DefaultTransactionDefinition());
					}
					return operation.get();
				} catch (RuntimeException | Error ex) {
					rollbackAccountTransaction();
					throw ex;
				} finally {
					// Keep an empty claim locked through its commit and slot release.
					if (processingLock.isHeldByCurrentThread()) {
						processingLock.unlock();
					}
				}
			}, accountRepository::checkConnection);
		}

		private void commitAccountTransaction() {
			accountTransactionManager.commit(transaction);
			transaction = null;
		}

		private void rollbackAccountTransaction() {
			if (transaction != null) {
				try {
					if (!transaction.isCompleted()) {
						accountTransactionManager.rollback(transaction);
					}
				} finally {
					transaction = null;
				}
			}
		}
	}

	private boolean waitForDatabaseRecovery(Runnable connectivityCheck) {
		while (true) {
			try {
				Thread.sleep(databaseRecoveryCheckInterval);
			} catch (InterruptedException ex) {
				log.warn("Database recovery interrupted");
				Thread.currentThread().interrupt();
				return false;
			}
			try {
				connectivityCheck.run();
				log.info("Database connectivity restored. Processing resumed");
				return true;
			} catch (RuntimeException ex) {
				if (!isTemporaryDatabaseFailure(ex)) {
					throw ex;
				}
				log.warn("Database remains unavailable. {}", ex.getMessage());
			}
		}
	}

	private <T> T retryDatabaseOperation(Supplier<T> operation, Runnable connectivityCheck) {
		return retryDatabaseOperation(operation, connectivityCheck, () -> true);
	}

	private <T> T retryDatabaseOperation(Supplier<T> operation, Runnable connectivityCheck,
			BooleanSupplier retryInTransaction) {
		for (int attempt = 1; ; attempt++) {
			try {
				return operation.get();
			} catch (RuntimeException ex) {
				if (!isTemporaryDatabaseFailure(ex) || !retryInTransaction.getAsBoolean()) {
					throw ex;
				}
				if (attempt == databaseMaxAttempts) {
					log.error("Final database operation attempt failed. {}. Worker paused",
							ex.getMessage());
					if (!Thread.currentThread().isInterrupted()
							&& waitForDatabaseRecovery(connectivityCheck)) {
						if (!retryInTransaction.getAsBoolean()) {
							throw ex;
						}
						attempt = 0;
						continue;
					}
					throw ex;
				}
				log.warn("Database operation attempt {} of {} failed. {}",
						attempt, databaseMaxAttempts, ex.getMessage());
				try {
					Thread.sleep(databaseRetryDelay);
				} catch (InterruptedException interrupted) {
					log.warn("Database retry interrupted after attempt {} of {}",
							attempt, databaseMaxAttempts);
					Thread.currentThread().interrupt();
					throw ex;
				}
			}
		}
	}

	private boolean isTemporaryDatabaseFailure(Throwable failure) {
		if (failure instanceof PricingAccessException) {
			failure = failure.getCause();
		}
		return failure instanceof TransientDataAccessException
				|| failure instanceof RecoverableDataAccessException
				|| failure instanceof DataAccessResourceFailureException
				|| failure instanceof CannotCreateTransactionException;
	}
}
