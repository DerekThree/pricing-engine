package com.pricing.engine.accountStore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.UUID;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import com.pricing.engine.ruleEngine.RuleEngineBatch;

@Repository
public class AccountRepository {

	private static final JsonMapper JSON = JsonMapper.builder().build();

	@PersistenceContext(unitName = "accounts")
	private EntityManager entityManager;

	private final AccountMapper mapper;

	public AccountRepository(AccountMapper mapper) {
		this.mapper = mapper;
	}

	@Transactional(transactionManager = "accountTransactionManager", readOnly = true)
	public void checkConnection() {
		entityManager.createNativeQuery("select 1", Integer.class).getSingleResult();
	}

	@Transactional(transactionManager = "accountTransactionManager")
	public UUID claimNextBatchId() {
		return (UUID) entityManager.createNativeQuery("""
				select batch_id from batch_outbox
				where processed = false and error is null
				limit 1 for update skip locked
				""", UUID.class).getSingleResultOrNull();
	}

	@Transactional(transactionManager = "accountTransactionManager", readOnly = true)
	public RuleEngineBatch load(UUID batchId) {
		List<AccountEntity> accounts = entityManager.createQuery("""
				select account from AccountEntity account where account.batch.batchId = :batchId
				""", AccountEntity.class).setParameter("batchId", batchId).getResultList();
		entityManager.createQuery("""
				select distinct account from AccountEntity account
				left join fetch account.attributes where account.batch.batchId = :batchId
				""", AccountEntity.class).setParameter("batchId", batchId).getResultList();
		entityManager.createQuery("""
				select distinct account from AccountEntity account
				left join fetch account.fees where account.batch.batchId = :batchId
				""", AccountEntity.class).setParameter("batchId", batchId).getResultList();
		return mapper.toRuleEngineBatch(batchId, accounts);
	}

	@Transactional(transactionManager = "accountTransactionManager")
	public UUID claimBatchId(UUID batchId) {
		return (UUID) entityManager.createNativeQuery("""
				select batch_id from batch_outbox
				where batch_id = :batchId and processed = false and error is null
				for update skip locked
				""", UUID.class).setParameter("batchId", batchId).getSingleResultOrNull();
	}

	@Transactional(transactionManager = "accountTransactionManager")
	public void recordError(UUID batchId, RuntimeException failure) {
		var trace = new StringWriter();
		failure.printStackTrace(new PrintWriter(trace));
		String error = JSON.createObjectNode()
				.put("type", failure.getClass().getName())
				.put("message", failure.getMessage())
				.put("stackTrace", trace.toString()).toString();
		entityManager.find(AccountOutboxEntity.class, batchId).setError(error);
	}

	@Transactional(transactionManager = "accountTransactionManager")
	public void markProcessed(UUID batchId) {
		entityManager.find(AccountOutboxEntity.class, batchId).setProcessed(true);
	}
}
