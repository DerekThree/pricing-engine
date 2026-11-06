package com.pricing.engine;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Supplier;

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
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.CannotCreateTransactionException;
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
	private final Object processingLock = new Object();
	private RuleEngineBatchResult pendingResult;

	private UUID pendingCompletion;

	private Runnable databaseConnectivityCheck;
	private boolean running;

	public EngineService(AccountRepository accountRepository, RuleEngine ruleEngine,
			RecommendationRepository recommendationRepository, PricingRepository pricingRepository,
			RestClient transactionProcessorClient,
			@Value("${transaction-processor.max-attempts}") int maxAttempts,
			@Value("${transaction-processor.retry-delay}") Duration retryDelay,
			@Value("${database.max-attempts}") int databaseMaxAttempts,
			@Value("${database.retry-delay}") Duration databaseRetryDelay,
			@Value("${database.recovery-check-interval}") Duration databaseRecoveryCheckInterval) {
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
		if (startRun()) {
			drainBatches(true);
		}
	}

	public void processPendingAccounts() {
		if (startRun()) {
			drainBatches(false);
		}
	}

	public void wakeUp() {
		if (startRun()) {
			Thread.startVirtualThread(() -> drainBatches(false));
		}
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

	private boolean startRun() {
		synchronized (processingLock) {
			if (running) {
				return false;
			}
			running = true;
			return true;
		}
	}

	private void drainBatches(boolean startup) {
		while (true) {
			try {
				if (databaseConnectivityCheck != null && !waitForDatabaseRecovery()) {
					synchronized (processingLock) {
						running = false;
					}
					return;
				}
				if (startup) {
					if (retryDatabaseOperation(recommendationRepository::existsByProcessedFalse,
							recommendationRepository::checkConnection)) {
						Thread.startVirtualThread(this::notifyTransactionProcessor);
					}
					startup = false;
				}
				if (pendingResult != null) {
					storePendingResult();
				}
				if (pendingCompletion != null) {
					completePendingBatch();
				}
				while (true) {
					List<UUID> batches = retryDatabaseOperation(() -> {
						synchronized (processingLock) {
							List<UUID> pending = accountRepository.findUnprocessedBatchIds();
							if (pending.isEmpty()) {
								running = false;
							}
							return pending;
						}
					}, accountRepository::checkConnection);
					if (batches.isEmpty()) {
						return;
					}
					for (UUID batchId : batches) {
						if (retryDatabaseOperation(
								() -> recommendationRepository.existsByBatchId(batchId),
								recommendationRepository::checkConnection)) {
							pendingCompletion = batchId;
							completePendingBatch();
							continue;
						}
						RuleEngineBatch batch = retryDatabaseOperation(
								() -> accountRepository.load(batchId), accountRepository::checkConnection);
						pendingResult = retryDatabaseOperation(
								() -> ruleEngine.price(batch), pricingRepository::checkConnection);
						storePendingResult();
					}
				}
			} catch (RuntimeException | Error ex) {
				if (isTemporaryDatabaseFailure(ex) && databaseConnectivityCheck != null
						&& !Thread.currentThread().isInterrupted()) {
					continue;
				}
				if (!isTemporaryDatabaseFailure(ex)) {
					pendingResult = null;
					pendingCompletion = null;
					databaseConnectivityCheck = null;
				}
				synchronized (processingLock) {
					running = false;
				}
				throw ex;
			}
		}
	}

	private void storePendingResult() {
		RuleEngineBatchResult result = pendingResult;
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
						if (message.contains("recommendation_batches_pkey")
								|| message.contains("recommendation_outbox_pkey")) {
							return null;
						}
					}
					cause = cause.getCause();
				}
				throw ex;
			}
			return null;
		}, recommendationRepository::checkConnection);
		pendingResult = null;
		pendingCompletion = result.batchId();
		Thread.startVirtualThread(this::notifyTransactionProcessor);
		completePendingBatch();
	}

	private void completePendingBatch() {
		retryDatabaseOperation(() -> {
			accountRepository.markProcessed(pendingCompletion);
			return null;
		}, accountRepository::checkConnection);
		pendingCompletion = null;
	}

	private boolean waitForDatabaseRecovery() {
		while (true) {
			try {
				Thread.sleep(databaseRecoveryCheckInterval);
			} catch (InterruptedException ex) {
				log.warn("Database recovery interrupted");
				Thread.currentThread().interrupt();
				return false;
			}
			try {
				databaseConnectivityCheck.run();
				databaseConnectivityCheck = null;
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
		for (int attempt = 1; ; attempt++) {
			try {
				return operation.get();
			} catch (RuntimeException ex) {
				if (!isTemporaryDatabaseFailure(ex)) {
					throw ex;
				}
				if (attempt == databaseMaxAttempts) {
					databaseConnectivityCheck = connectivityCheck;
					log.error("Final database operation attempt failed. {}. Processing paused",
							ex.getMessage());
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
