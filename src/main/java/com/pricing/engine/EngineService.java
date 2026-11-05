package com.pricing.engine;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.pricing.engine.accountStore.AccountRepository;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.RuleEngine;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Service
public class EngineService {

	private static final Logger log = LoggerFactory.getLogger(EngineService.class);

	private final AccountRepository accountRepository;
	private final RuleEngine ruleEngine;
	private final RecommendationRepository recommendationRepository;
	private final RestClient transactionProcessorClient;
	private final int maxAttempts;
	private final Duration retryDelay;
	private final Object processingLock = new Object();
	private boolean running;

	public EngineService(AccountRepository accountRepository, RuleEngine ruleEngine,
			RecommendationRepository recommendationRepository, RestClient transactionProcessorClient,
			@Value("${transaction-processor.max-attempts}") int maxAttempts,
			@Value("${transaction-processor.retry-delay}") Duration retryDelay) {
		Assert.isTrue(maxAttempts > 0, "Transaction Processor attempt limit must be positive");
		Assert.isTrue(!retryDelay.isNegative(),
				"Transaction Processor retry delay must not be negative");
		this.accountRepository = accountRepository;
		this.ruleEngine = ruleEngine;
		this.recommendationRepository = recommendationRepository;
		this.transactionProcessorClient = transactionProcessorClient;
		this.maxAttempts = maxAttempts;
		this.retryDelay = retryDelay;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void recoverPendingBatches() {
		if (recommendationRepository.existsByProcessedFalse()) {
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
			while (true) {
				List<UUID> batches;
				synchronized (processingLock) {
					batches = accountRepository.findUnprocessedBatchIds();
					if (batches.isEmpty()) {
						running = false;
						return;
					}
				}
				for (UUID batchId : batches) {
					RuleEngineBatchResult result = ruleEngine.price(accountRepository.load(batchId));
					recommendationRepository.save(result);
					Thread.startVirtualThread(this::notifyTransactionProcessor);
					accountRepository.markProcessed(batchId);
				}
			}
		} catch (RuntimeException | Error ex) {
			synchronized (processingLock) {
				running = false;
			}
			throw ex;
		}
	}
}
