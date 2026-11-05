package com.pricing.engine;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import com.pricing.engine.accountStore.AccountRepository;
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
	private final RecommendationRepository recommendationRepository;
	private final RestClient transactionProcessorClient;
	private final int maxAttempts;
	private final Duration retryDelay;
	private final int databaseMaxAttempts;
	private final Duration databaseRetryDelay;
	private final Object processingLock = new Object();
	private RuleEngineBatchResult pendingResult;
	private boolean running;

	public EngineService(AccountRepository accountRepository, RuleEngine ruleEngine,
			RecommendationRepository recommendationRepository, RestClient transactionProcessorClient,
			@Value("${transaction-processor.max-attempts}") int maxAttempts,
			@Value("${transaction-processor.retry-delay}") Duration retryDelay,
			@Value("${database.max-attempts}") int databaseMaxAttempts,
			@Value("${database.retry-delay}") Duration databaseRetryDelay) {
		Assert.isTrue(maxAttempts > 0, "Transaction Processor attempt limit must be positive");
		Assert.isTrue(!retryDelay.isNegative(),
				"Transaction Processor retry delay must not be negative");
		Assert.isTrue(databaseMaxAttempts > 0, "Database attempt limit must be positive");
		Assert.isTrue(!databaseRetryDelay.isNegative(), "Database retry delay must not be negative");
		this.databaseMaxAttempts = databaseMaxAttempts;
		this.databaseRetryDelay = databaseRetryDelay;
		this.accountRepository = accountRepository;
		this.ruleEngine = ruleEngine;
		this.recommendationRepository = recommendationRepository;
		this.transactionProcessorClient = transactionProcessorClient;
		this.maxAttempts = maxAttempts;
		this.retryDelay = retryDelay;
	}

	@Override
	public void afterSingletonsInstantiated() {
		if (retryDatabaseOperation(recommendationRepository::existsByProcessedFalse)) {
			Thread.startVirtualThread(this::notifyTransactionProcessor);
		}
		processPendingAccounts();
	}

	public void processPendingAccounts() {
		if (startRun()) {
			drainBatches();
		}
	}

	public void wakeUp() {
		if (startRun()) {
			Thread.startVirtualThread(this::drainBatches);
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

	private void drainBatches() {
		try {
			if (pendingResult != null) {
				storePendingResult();
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
				});
				if (batches.isEmpty()) {
					return;
				}
				for (UUID batchId : batches) {
					RuleEngineBatch batch = retryDatabaseOperation(() -> accountRepository.load(batchId));
					pendingResult = retryDatabaseOperation(() -> ruleEngine.price(batch));
					storePendingResult();
				}
			}
		} catch (RuntimeException | Error ex) {
			if (!isTemporaryDatabaseFailure(ex)) {
				pendingResult = null;
			}
			synchronized (processingLock) {
				running = false;
			}
			throw ex;
		}
	}

	private void storePendingResult() {
		RuleEngineBatchResult result = pendingResult;
		retryDatabaseOperation(() -> {
			recommendationRepository.save(result);
			return null;
		});
		pendingResult = null;
		Thread.startVirtualThread(this::notifyTransactionProcessor);
		retryDatabaseOperation(() -> {
			accountRepository.markProcessed(result.batchId());
			return null;
		});
	}

	private <T> T retryDatabaseOperation(Supplier<T> operation) {
		for (int attempt = 1; ; attempt++) {
			try {
				return operation.get();
			} catch (RuntimeException ex) {
				if (!isTemporaryDatabaseFailure(ex)) {
					throw ex;
				}
				if (attempt == databaseMaxAttempts) {
					log.error("Final database operation attempt failed. {}. Processing stopped",
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
