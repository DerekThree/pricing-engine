package com.pricing.engine;

import java.net.ServerSocket;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import com.pricing.engine.config.ConfigurationStoreConfiguration;
import com.pricing.engine.config.RecommendationStoreConfiguration;
import com.pricing.engine.recommendationStore.RecommendationMapper;
import com.pricing.engine.recommendationStore.RecommendationRepository;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult;
import com.pricing.engine.ruleEngine.RuleEngineBatchResult.*;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers(disabledWithoutDocker = true)
class RecommendationStorePostgresTests {

	private static final List<String> TABLES = List.of("recommendation_outbox",
			"account_recommendations", "fee_request_recommendations",
			"recommendation_eligibility_reasons");

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
					HibernateJpaAutoConfiguration.class, TransactionAutoConfiguration.class))
			.withUserConfiguration(ConfigurationStoreConfiguration.class,
					RecommendationStoreConfiguration.class, RecommendationMapper.class,
					RecommendationRepository.class)
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
					"recommendation-store.datasource.url=" + databaseUrl("recommendationdb"),
					"recommendation-store.datasource.username=recommendationdb_engine",
					"recommendation-store.datasource.password=engine'password",
					"recommendation-store.txn-processor.username=recommendationdb_txn_processor",
					"recommendation-store.txn-processor.password=processor'password");

	@Test
	void initializesFreshStoreAndKeepsAppliedMigrationsOnRestart() {
		UUID batchId = UUID.randomUUID();
		application.run(context -> {
			assertThat(context).hasNotFailed();
			DataSource engineDataSource = context.getBean("recommendationDataSource", DataSource.class);
			DataSource processorDataSource = new DriverManagerDataSource(databaseUrl("recommendationdb"),
					"recommendationdb_txn_processor", "processor'password");
			JdbcTemplate engine = new JdbcTemplate(engineDataSource);
			JdbcTemplate processor = new JdbcTemplate(processorDataSource);
			RecommendationRepository repository = context.getBean(RecommendationRepository.class);
			repository.save(new RuleEngineBatchResult(batchId, List.of(new AccountResult(
					"A1", AccountStatus.OK, "PLAN", List.of(new FeeResult(42L, FeeStatus.OK,
							Decision.WAIVED, null, List.of("ELIGIBLE")))))));
			assertThat(repository.existsByBatchId(batchId)).isTrue();
			assertThat(repository.existsByProcessedFalse()).isTrue();
			assertThat(engine.queryForObject("select current_user", String.class))
					.isEqualTo("recommendationdb_engine");
			for (String table : TABLES) {
				assertThat(engine.queryForObject("select count(*) from " + table, Integer.class))
						.isEqualTo(1);
				assertThat(processor.queryForObject("select count(*) from " + table, Integer.class))
						.isEqualTo(1);
				assertDenied(processorDataSource, "insert into " + table + " default values");
				assertDenied(engineDataSource, "update " + table + " set "
						+ (table.equals("recommendation_outbox") ? "processed = true"
								: table.equals("recommendation_eligibility_reasons")
										? "reason_code = 'OTHER'" : "status = 'ERROR'"));
				for (DataSource runtime : List.of(engineDataSource, processorDataSource)) {
					assertDenied(runtime, "delete from " + table);
					assertDenied(runtime, "truncate " + table + " cascade");
				}
				if (!table.equals("recommendation_outbox")) {
					assertDenied(processorDataSource, "update " + table + " set "
							+ (table.equals("recommendation_eligibility_reasons")
									? "reason_code = 'OTHER'" : "status = 'ERROR'"));
				}
			}
			assertThat(processor.update("update recommendation_outbox set processed = true"))
					.isEqualTo(1);
			assertThat(repository.existsByProcessedFalse()).isFalse();
			for (DataSource runtime : List.of(engineDataSource, processorDataSource)) {
				for (String sql : List.of("create table forbidden (id integer)",
						"create schema forbidden", "create database forbidden", "create role forbidden",
						"create temporary table forbidden (id integer)",
						"alter table recommendation_outbox add column forbidden integer",
						"select * from flyway_schema_history")) {
					assertDenied(runtime, sql);
				}
				assertThrows(FlywayException.class, () -> Flyway.configure().dataSource(runtime)
						.locations("classpath:db/recommendation").load().migrate());
			}
		});
		JdbcTemplate initialization = new JdbcTemplate(new DriverManagerDataSource(
				databaseUrl("recommendationdb"), "store_initializer", "initialization-password"));
		var history = initialization.queryForList(
				"select * from flyway_schema_history order by installed_rank");
		assertThat(history).hasSize(3);
		assertThat(initialization.queryForList("select distinct tableowner from pg_tables "
				+ "where schemaname = 'public'", String.class)).containsExactly("store_initializer");
		initialization.execute("alter table recommendation_outbox add column processing_note text");
		application.run(context -> {
			assertThat(context).hasNotFailed();
			JdbcTemplate processor = new JdbcTemplate(new DriverManagerDataSource(
					databaseUrl("recommendationdb"), "recommendationdb_txn_processor", "processor'password"));
			assertThat(processor.queryForObject("select processed from recommendation_outbox",
					Boolean.class)).isTrue();
			assertThat(processor.update("update recommendation_outbox set processing_note = 'resumed'"))
					.isEqualTo(1);
		});
		assertThat(initialization.queryForList(
				"select * from flyway_schema_history order by installed_rank")).isEqualTo(history);
	}

	private static String databaseUrl(String database) {
		return POSTGRES.getJdbcUrl().replace("/postgres", "/" + database);
	}

	@Test
	void readinessRequiresSuccessfulInitializationOfEveryStore() throws Exception {
		for (String failedLogin : List.of("", "spring.datasource.username",
				"account-store.datasource.username", "recommendation-store.datasource.username")) {
			int port;
			try (var available = new ServerSocket(0)) {
				port = available.getLocalPort();
			}
			var arguments = new ArrayList<>(List.of(
					"--server.port=" + port,
					"--database-initialization.enabled=true",
					"--database-initialization.datasource.url=" + POSTGRES.getJdbcUrl(),
					"--database-initialization.datasource.username=store_initializer",
					"--database-initialization.datasource.password=initialization-password",
					"--configuration-store.backend.username=pricingdb_backend",
					"--configuration-store.backend.password=backend-password",
					"--spring.datasource.url=" + databaseUrl("pricingdb_ready"),
					"--spring.datasource.username=pricingdb_engine",
					"--spring.datasource.password=configuration-engine-password",
					"--spring.datasource.driver-class-name=org.postgresql.Driver",
					"--account-store.datasource.url=" + databaseUrl("accountdb_ready"),
					"--account-store.datasource.username=accountdb_engine",
					"--account-store.datasource.password=account-engine-password",
					"--account-store.datasource.driver-class-name=org.postgresql.Driver",
					"--account-store.jpa.ddl-auto=validate",
					"--account-store.mock-server.username=accountdb_mock_server",
					"--account-store.mock-server.password=mock-password",
					"--recommendation-store.datasource.url=" + databaseUrl("recommendationdb_ready"),
					"--recommendation-store.datasource.username=recommendationdb_engine",
					"--recommendation-store.datasource.password=engine'password",
					"--recommendation-store.datasource.driver-class-name=org.postgresql.Driver",
					"--recommendation-store.txn-processor.username=recommendationdb_txn_processor",
					"--recommendation-store.txn-processor.password=processor'password"));
			String readinessUrl = "http://127.0.0.1:" + port + "/openapi.yaml";
			if (failedLogin.isEmpty()) {
				try (var context = new SpringApplicationBuilder(EngineApplication.class)
						.run(arguments.toArray(String[]::new))) {
					assertThat(RestClient.create().get().uri(readinessUrl).retrieve()
							.toBodilessEntity().getStatusCode().value()).isEqualTo(200);
				}
			} else {
				arguments.removeIf(argument -> argument.startsWith("--" + failedLogin + "="));
				arguments.add("--" + failedLogin + "=store_initializer");
				var failure = assertThrows(RuntimeException.class, () -> new SpringApplicationBuilder(
						EngineApplication.class).run(arguments.toArray(String[]::new)));
				assertThat(failure).hasRootCauseMessage(
						"Initialization and runtime logins must be separate");
				assertThrows(RestClientException.class, () -> RestClient.create().get()
						.uri(readinessUrl).retrieve().toBodilessEntity());
			}
		}
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
