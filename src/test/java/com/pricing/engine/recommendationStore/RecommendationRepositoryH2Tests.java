package com.pricing.engine.recommendationStore;

import java.math.BigDecimal;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import com.pricing.engine.utils.H2Fixture;

class RecommendationRepositoryH2Tests extends H2Fixture {

	@Test
	void competingResultsCommitOnlyOneRecommendationBatch() throws Exception {
		try (var other = startApplication();
				var work = Executors.newVirtualThreadPerTaskExecutor()) {
			UUID batchId = UUID.randomUUID();
			var ready = new CountDownLatch(2);
			var start = new CountDownLatch(1);
			var attempts = new ArrayList<java.util.concurrent.Future<String>>();
			for (var repository : List.of(recommendationRepository,
					other.getBean(RecommendationRepository.class))) {
				String plan = attempts.isEmpty() ? "FIRST" : "SECOND";
				attempts.add(work.submit(() -> {
					ready.countDown();
					assertTrue(start.await(5, TimeUnit.SECONDS));
					try {
						repository.save(new RuleEngineBatchResult(batchId, List.of(
								new AccountResult("A1", AccountStatus.OK, plan, List.of(
										new FeeResult(42L, FeeStatus.OK, Decision.CHARGED,
												new BigDecimal("2.50"), null))))));
						return plan;
					} catch (DataIntegrityViolationException ex) {
						return null;
					}
				}));
			}
			assertTrue(ready.await(5, TimeUnit.SECONDS));
			start.countDown();
			String first = attempts.getFirst().get(5, TimeUnit.SECONDS);
			String second = attempts.getLast().get(5, TimeUnit.SECONDS);
			assertTrue((first == null) != (second == null));
			assertEquals(first == null ? second : first, recommendations.queryForObject(
					"select pricing_plan_code from account_recommendations", String.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from recommendation_outbox", Integer.class));
			assertEquals(1, recommendations.queryForObject(
					"select count(*) from fee_request_recommendations", Integer.class));
		}
	}
}
