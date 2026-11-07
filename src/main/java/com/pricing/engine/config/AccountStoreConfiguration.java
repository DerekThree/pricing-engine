package com.pricing.engine.config;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import com.pricing.engine.accountStore.AccountOutboxEntity;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.boot.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

@Configuration(proxyBeanMethods = false)
public class AccountStoreConfiguration {

	@Bean(defaultCandidate = false)
	@ConfigurationProperties("account-store.datasource")
	public DataSourceProperties accountDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean(defaultCandidate = false)
	public HikariDataSource accountDataSource(
			@Qualifier("accountDataSourceProperties") DataSourceProperties properties) {
		return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
	}

	@Bean(defaultCandidate = false, initMethod = "migrate")
	@ConditionalOnProperty(name = "database-initialization.enabled", matchIfMissing = true)
	public Flyway accountFlyway(
			@Qualifier("accountDataSourceProperties") DataSourceProperties runtime,
			@Qualifier("initializationDataSourceProperties") DataSourceProperties initialization,
			@Value("${account-store.mock-server.username}") String mockUser,
			@Value("${account-store.mock-server.password}") String mockPassword) throws SQLException {
		String database = ConfigurationStoreConfiguration.initializeDatabase(
				runtime, initialization, mockUser, mockPassword);
		return Flyway.configure().dataSource(runtime.getUrl(), initialization.getUsername(),
				initialization.getPassword())
				.locations("classpath:db/account", "classpath:db/account-postgresql")
				.placeholders(Map.of("mockServerLogin", mockUser.replace("\"", "\"\""),
						"engineLogin", runtime.getUsername().replace("\"", "\"\""),
						"accountDatabase", database.replace("\"", "\"\""))).load();
	}

	@Bean(defaultCandidate = false)
	public LocalContainerEntityManagerFactoryBean accountEntityManagerFactory(
			EntityManagerFactoryBuilder builder,
			@Qualifier("accountDataSource") DataSource dataSource,
			@Qualifier("accountFlyway") ObjectProvider<Flyway> migrations,
			@Value("${account-store.jpa.ddl-auto:validate}") String ddlMode) {
		migrations.getIfAvailable();
		return builder.dataSource(dataSource).packages(AccountOutboxEntity.class)
				.persistenceUnit("accounts")
				.properties(Map.of("hibernate.hbm2ddl.auto", ddlMode)).build();
	}

	@Bean(defaultCandidate = false)
	public JpaTransactionManager accountTransactionManager(
			@Qualifier("accountEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
		return new JpaTransactionManager(entityManagerFactory);
	}
}
