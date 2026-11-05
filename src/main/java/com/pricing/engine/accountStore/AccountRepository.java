package com.pricing.engine.accountStore;

import java.util.List;
import java.util.UUID;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.pricing.engine.ruleEngine.RuleEngineBatch;

@Repository
public class AccountRepository {

	@PersistenceContext(unitName = "accounts")
	private EntityManager entityManager;

	private final AccountMapper mapper;

	public AccountRepository(AccountMapper mapper) {
		this.mapper = mapper;
	}

	@Transactional(transactionManager = "accountTransactionManager", readOnly = true)
	public List<UUID> findUnprocessedBatchIds() {
		return entityManager.createQuery("""
				select accountOutbox.batchId from AccountOutboxEntity accountOutbox
				where accountOutbox.processed = false
				""", UUID.class).getResultList();
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
	public void markProcessed(UUID batchId) {
		entityManager.find(AccountOutboxEntity.class, batchId).setProcessed(true);
	}
}
