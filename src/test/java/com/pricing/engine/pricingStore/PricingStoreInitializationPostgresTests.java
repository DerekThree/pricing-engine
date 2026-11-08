package com.pricing.engine.pricingStore;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import com.pricing.engine.config.PricingStoreConfiguration;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
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
class PricingStoreInitializationPostgresTests {

	private static final List<String> TABLES = List.of("branches", "products", "pricing_plans",
			"regions", "region_branches", "region_states", "region_zip_codes", "account_attributes",
			"account_attribute_product_types", "eligibility_reasons", "eligibility_reason_conditions",
			"fees", "fee_product_types", "pricing_plan_fees", "pricing_plan_fee_reasons");

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
					DataSourceAutoConfiguration.class, FlywayAutoConfiguration.class))
			.withUserConfiguration(PricingStoreConfiguration.class)
			.withPropertyValues(
					"database-initialization.enabled=true",
					"database-initialization.datasource.url=" + POSTGRES.getJdbcUrl(),
					"database-initialization.datasource.username=store_initializer",
					"database-initialization.datasource.password=initialization-password",
					"pricing-store.backend.username=pricingdb_backend",
					"pricing-store.backend.password=backend'password",
					"spring.datasource.url=" + POSTGRES.getJdbcUrl().replace("/postgres", "/pricingdb"),
					"spring.datasource.username=pricingdb_engine",
					"spring.datasource.password=engine'password");

	@Test
	void initializesFreshPricingStoreAndLeavesAppliedMigrationsOnRestart() {
		application.run(context -> {
			assertThat(context).hasNotFailed();
			DataSource engineDataSource = context.getBean(DataSource.class);
			JdbcTemplate engine = new JdbcTemplate(engineDataSource);
			assertThat(engine.queryForObject("select current_user", String.class))
					.isEqualTo("pricingdb_engine");
			assertThat(engine.queryForObject("select count(*) from branches", Integer.class))
					.isPositive();
			DataSource backendDataSource = new DriverManagerDataSource(databaseUrl(),
					"pricingdb_backend", "backend'password");
			JdbcTemplate backend = new JdbcTemplate(backendDataSource);
			for (String table : TABLES) {
				assertThat(engine.queryForObject("select count(*) from " + table, Integer.class))
						.isNotNull();
				assertThat(backend.queryForObject("select count(*) from " + table, Integer.class))
						.isNotNull();
			}
			Long id = backend.queryForObject("""
					insert into branches
					    (branch_code, branch_name, state, zip_code, updated_on, updated_by)
					values ('INIT_TEST', 'Initialization', 'NY', '10001', current_timestamp, 'test')
					returning id
					""", Long.class);
			assertThat(backend.update("update branches set branch_name = 'Updated' where id = ?", id))
					.isEqualTo(1);
			assertThat(engine.queryForObject("select branch_name from branches where id = ?",
					String.class, id)).isEqualTo("Updated");
			assertThat(backend.queryForObject(
					"select nextval(pg_get_serial_sequence('branches', 'id'))", Long.class)).isPositive();
			assertThat(backend.update("delete from branches where id = ?", id)).isEqualTo(1);
			for (DataSource runtime : List.of(engineDataSource, backendDataSource)) {
				for (String sql : List.of("create table forbidden (id integer)",
						"create schema forbidden", "create database forbidden", "create role forbidden",
						"create temporary table forbidden (id integer)", "truncate branches",
						"alter table branches add column forbidden integer",
						"select * from flyway_schema_history")) {
					assertDenied(runtime, sql);
				}
				assertThrows(FlywayException.class, () -> Flyway.configure().dataSource(runtime)
						.locations("classpath:db/migration").load().migrate());
			}
			for (String sql : List.of("insert into branches default values",
					"update branches set branch_name = branch_name", "delete from branches",
					"select nextval(pg_get_serial_sequence('branches', 'id'))")) {
				assertDenied(engineDataSource, sql);
			}
		});
		JdbcTemplate initialization = new JdbcTemplate(new DriverManagerDataSource(databaseUrl(),
				"store_initializer", "initialization-password"));
		var history = initialization.queryForList(
				"select * from flyway_schema_history order by installed_rank");
		assertThat(history).hasSize(26);
		application.run(context -> {
			assertThat(context).hasNotFailed();
			JdbcTemplate engine = new JdbcTemplate(context.getBean(DataSource.class));
			assertThat(engine.queryForObject("select count(*) from branches", Integer.class))
					.isPositive();
		});
		assertThat(initialization.queryForList(
				"select * from flyway_schema_history order by installed_rank")).isEqualTo(history);
	}

	private static String databaseUrl() {
		return POSTGRES.getJdbcUrl().replace("/postgres", "/pricingdb");
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
