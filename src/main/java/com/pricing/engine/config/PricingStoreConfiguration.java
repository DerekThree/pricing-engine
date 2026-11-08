package com.pricing.engine.config;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class PricingStoreConfiguration {

	@Bean(defaultCandidate = false)
	@ConfigurationProperties("database-initialization.datasource")
	public DataSourceProperties initializationDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean(initMethod = "migrate")
	@ConditionalOnProperty(name = "database-initialization.enabled", matchIfMissing = true)
	public Flyway flyway(DataSourceProperties runtime,
			@Qualifier("initializationDataSourceProperties") DataSourceProperties initialization,
			@Value("${pricing-store.backend.username}") String backendUser,
			@Value("${pricing-store.backend.password}") String backendPassword)
			throws SQLException {
		String database = initializeDatabase(runtime, initialization, backendUser, backendPassword);
		return Flyway.configure().dataSource(runtime.getUrl(), initialization.getUsername(),
				initialization.getPassword()).locations("classpath:db/migration", "classpath:db/postgresql")
				.placeholders(Map.of("backendLogin", backendUser.replace("\"", "\"\""),
						"engineLogin", runtime.getUsername().replace("\"", "\"\""),
						"configurationDatabase", database.replace("\"", "\"\""))).load();
	}

	static String initializeDatabase(DataSourceProperties runtime,
			DataSourceProperties initialization, String applicationUser, String applicationPassword)
			throws SQLException {
		if (initialization.getUsername().equals(runtime.getUsername())
				|| initialization.getUsername().equals(applicationUser)
				|| applicationUser.equals(runtime.getUsername())) {
			throw new IllegalArgumentException("Initialization and runtime logins must be separate");
		}
		String database = URI.create(runtime.getUrl().substring(5)).getPath().substring(1);
		try (var connection = DriverManager.getConnection(initialization.getUrl(),
				initialization.getUsername(), initialization.getPassword())) {
			connection.setAutoCommit(true);
			ensureLogin(connection, applicationUser, applicationPassword);
			ensureLogin(connection, runtime.getUsername(), runtime.getPassword());
			try (var query = connection.prepareStatement(
					"select 1 from pg_database where datname = ?")) {
				query.setString(1, database);
				try (var result = query.executeQuery(); var statement = connection.createStatement()) {
					if (!result.next()) {
						statement.execute("create database "
								+ statement.enquoteIdentifier(database, true));
					}
				}
			}
		}
		return database;
	}

	private static void ensureLogin(Connection connection, String username, String password)
			throws SQLException {
		try (var query = connection.prepareStatement("""
				select rolsuper or rolcreatedb or rolcreaterole or rolreplication or rolbypassrls
				from pg_roles where rolname = ?
				""")) {
			query.setString(1, username);
			try (var result = query.executeQuery(); var statement = connection.createStatement()) {
				boolean exists = result.next();
				if (exists && result.getBoolean(1)) {
					throw new IllegalArgumentException(
							"Runtime logins must not have administrative privileges");
				}
				String command = exists ? "alter role " : "create role ";
				statement.execute(command + statement.enquoteIdentifier(username, true)
						+ " login password "
						+ statement.enquoteLiteral(password));
			}
		}
	}
}
