package com.pricing.engine;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import com.pricing.engine.config.AccountStoreConfiguration;
import com.pricing.engine.config.ConfigurationStoreConfiguration;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers(disabledWithoutDocker = true)
class AccountStorePostgresTests {

	private static final List<String> TABLES = List.of("accounts", "account_attributes",
			"fee_requests", "batch_outbox");

	@Container
	static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
			.withDatabaseName("postgres");

	@BeforeAll
	static void createPrivilegedInitializationLogin() throws SQLException {
		try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
				POSTGRES.getUsername(), POSTGRES.getPassword());
				var statement = connection.createStatement()) {
			statement.execute("create role store_initializer login createdb createrole "
					+ "password 'initialization-password'");
		}
	}

	private final ApplicationContextRunner application = new ApplicationContextRunner()
			.withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
					DataSourceAutoConfiguration.class, FlywayAutoConfiguration.class,
					HibernateJpaAutoConfiguration.class))
			.withUserConfiguration(ConfigurationStoreConfiguration.class, AccountStoreConfiguration.class)
			.withPropertyValues(
					"database-initialization.enabled=true",
					"database-initialization.datasource.url=" + POSTGRES.getJdbcUrl(),
					"database-initialization.datasource.username=store_initializer",
					"database-initialization.datasource.password=initialization-password",
					"configuration-store.backend.username=pricingdb_backend",
					"configuration-store.backend.password=backend-password",
					"spring.datasource.url=" + databaseUrl("pricingdb"),
					"spring.datasource.username=pricingdb_engine",
					"spring.datasource.password=configuration-engine-password",
					"spring.jpa.hibernate.ddl-auto=none",
					"account-store.datasource.url=" + databaseUrl("accountdb"),
					"account-store.datasource.username=accountdb_engine",
					"account-store.datasource.password=engine'password",
					"account-store.mock-server.username=accountdb_mock_server",
					"account-store.mock-server.password=mock'password");

	@Test
	void initializesFreshAccountStoreAndKeepsAppliedMigrationsOnRestart() {
		UUID batchId = UUID.randomUUID();
		application.run(context -> {
			assertThat(context).hasNotFailed();
			DataSource engineDataSource = context.getBean("accountDataSource", DataSource.class);
			DataSource mockDataSource = new DriverManagerDataSource(databaseUrl("accountdb"),
					"accountdb_mock_server", "mock'password");
			JdbcTemplate engine = new JdbcTemplate(engineDataSource);
			JdbcTemplate mock = new JdbcTemplate(mockDataSource);
			assertThat(engine.queryForObject("select current_user", String.class))
					.isEqualTo("accountdb_engine");
			assertThat(mock.queryForObject("select current_user", String.class))
					.isEqualTo("accountdb_mock_server");
			mock.update("insert into batch_outbox (batch_id) values (?)", batchId);
			Long accountId = mock.queryForObject("""
					insert into accounts
					    (batch_id, account_number, product_code, branch_code, pricing_date)
					values (?, 'INIT_TEST', 'CHECKING', 'MAIN', date '2026-01-01') returning id
					""", Long.class, batchId);
			mock.update("""
					insert into account_attributes (account_id, code, value) values (?, 'BALANCE', '100')
					""", accountId);
			mock.update("""
					insert into fee_requests (account_id, fee_request_id, code) values (?, 1, 'MONTHLY')
					""", accountId);
			for (String table : TABLES) {
				assertThat(engine.queryForObject("select count(*) from " + table, Integer.class))
						.isEqualTo(1);
				assertThat(mock.queryForObject("select count(*) from " + table, Integer.class))
						.isEqualTo(1);
				assertDenied(mockDataSource, "update " + table + " set "
						+ (table.equals("batch_outbox") ? "processed = true" : "id = id"));
				assertDenied(mockDataSource, "delete from " + table);
				assertDenied(engineDataSource, "insert into " + table + " default values");
				assertDenied(engineDataSource, "delete from " + table);
				if (!table.equals("batch_outbox")) {
					assertDenied(engineDataSource, "update " + table + " set id = id");
				}
				for (DataSource runtime : List.of(engineDataSource, mockDataSource)) {
					assertDenied(runtime, "truncate " + table + " cascade");
				}
			}
			assertThat(engine.update("""
					update batch_outbox set processed = true, error = 'test' where batch_id = ?
					""", batchId)).isEqualTo(1);
			assertThat(mock.queryForObject("select processed from batch_outbox", Boolean.class)).isTrue();
			assertThat(mock.queryForObject("select error from batch_outbox", String.class))
					.isEqualTo("test");
			for (String table : List.of("accounts", "account_attributes", "fee_requests")) {
				assertThat(mock.queryForObject("select nextval(pg_get_serial_sequence('"
						+ table + "', 'id'))", Long.class)).isPositive();
				assertDenied(engineDataSource, "select nextval(pg_get_serial_sequence('"
						+ table + "', 'id'))");
			}
			for (DataSource privileged : List.of(
					new DriverManagerDataSource(databaseUrl("accountdb"),
							"store_initializer", "initialization-password"),
					new DriverManagerDataSource(databaseUrl("accountdb"),
							POSTGRES.getUsername(), POSTGRES.getPassword()))) {
				for (String table : TABLES) {
					assertDenied(privileged, "delete from " + table);
					assertDenied(privileged, "truncate " + table + " cascade");
					if (!table.equals("batch_outbox")) {
						assertDenied(privileged, "update " + table + " set id = id");
					}
				}
				assertDenied(privileged, "update batch_outbox set batch_id = '"
						+ UUID.randomUUID() + "'");
			}
			assertDenied(engineDataSource, "update batch_outbox set batch_id = '"
					+ UUID.randomUUID() + "'");
			assertThat(engine.update("update batch_outbox set batch_id = batch_id, error = null"))
					.isEqualTo(1);
			for (DataSource runtime : List.of(engineDataSource, mockDataSource)) {
				for (String sql : List.of("create table forbidden (id integer)",
						"create schema forbidden", "create database forbidden", "create role forbidden",
						"create temporary table forbidden (id integer)",
						"alter table accounts add column forbidden integer",
						"select * from flyway_schema_history")) {
					assertDenied(runtime, sql);
				}
				assertThrows(FlywayException.class, () -> Flyway.configure().dataSource(runtime)
						.locations("classpath:db/account").load().migrate());
			}
		});
		JdbcTemplate initialization = new JdbcTemplate(new DriverManagerDataSource(
				databaseUrl("accountdb"), "store_initializer", "initialization-password"));
		var history = initialization.queryForList(
				"select * from flyway_schema_history order by installed_rank");
		assertThat(history).hasSize(4);
		assertThat(initialization.queryForList("select distinct tableowner from pg_tables "
				+ "where schemaname = 'public'", String.class)).containsExactly("store_initializer");
		initialization.execute("alter table batch_outbox add column processing_note text");
		application.run(context -> {
			assertThat(context).hasNotFailed();
			JdbcTemplate engine = new JdbcTemplate(context.getBean("accountDataSource", DataSource.class));
			assertThat(engine.queryForObject("select processed from batch_outbox", Boolean.class)).isTrue();
			assertThat(engine.update("""
					update batch_outbox set processing_note = 'resumed' where batch_id = ?
					""", batchId)).isEqualTo(1);
		});
		assertThat(initialization.queryForList(
				"select * from flyway_schema_history order by installed_rank")).isEqualTo(history);
	}

	private static String databaseUrl(String database) {
		return POSTGRES.getJdbcUrl().replace("/postgres", "/" + database);
	}

	private static void assertDenied(DataSource dataSource, String sql) {
		SQLException failure = assertThrows(SQLException.class, () -> {
			try (var connection = dataSource.getConnection();
					var statement = connection.createStatement()) {
				statement.execute(sql);
			}
		});
		assertThat(failure.getSQLState()).as(sql).isEqualTo("42501");
	}
}
