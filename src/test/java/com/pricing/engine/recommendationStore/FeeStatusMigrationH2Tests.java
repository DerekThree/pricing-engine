package com.pricing.engine.recommendationStore;

import java.util.List;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FeeStatusMigrationH2Tests {

	@Test
	void preservesExistingFeeResultsWhenDecisionIsMergedIntoStatus() {
		String url = "jdbc:h2:mem:fee_status_upgrade;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;"
				+ "DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
		var dataSource = new DriverManagerDataSource(url, "sa", "");
		Flyway.configure().dataSource(dataSource).locations("classpath:db/recommendation")
				.target("2").load().migrate();

		var db = new JdbcTemplate(dataSource);
		db.update("insert into recommendation_outbox(batch_id, processed) values (?, false)",
				UUID.randomUUID());
		db.update("insert into account_recommendations(batch_id, account_number, status) "
				+ "select batch_id, 'A1', 'OK' from recommendation_outbox");
		Long accountId = db.queryForObject("select id from account_recommendations", Long.class);
		db.update("insert into fee_request_recommendations "
				+ "(account_id, fee_request_id, status, decision, amount) values (?, 1, 'OK', 'CHARGED', 2.50)",
				accountId);
		db.update("insert into fee_request_recommendations "
				+ "(account_id, fee_request_id, status, decision) values (?, 2, 'OK', 'WAIVED')",
				accountId);
		db.update("insert into fee_request_recommendations "
				+ "(account_id, fee_request_id, status) values (?, 3, 'FEE_NOT_FOUND')", accountId);

		Flyway.configure().dataSource(dataSource).locations("classpath:db/recommendation")
				.load().migrate();

		assertEquals(List.of("CHARGED", "WAIVED", "FEE_NOT_FOUND"), db.queryForList(
				"select status from fee_request_recommendations order by fee_request_id", String.class));
		assertEquals(0, db.queryForObject(
				"select count(*) from information_schema.columns "
				+ "where table_name = 'fee_request_recommendations' and column_name = 'decision'",
				Integer.class));
		db.update("insert into fee_request_recommendations(account_id, fee_request_id, status, amount) "
				+ "values (?, 4, 'CHARGED', 1.50)", accountId);
		assertEquals("CHARGED", db.queryForObject(
				"select status from fee_request_recommendations where fee_request_id = 4", String.class));
	}
}
