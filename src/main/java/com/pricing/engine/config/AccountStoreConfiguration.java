package com.pricing.engine.config;

import java.util.Map;

import javax.sql.DataSource;

import com.pricing.engine.accountStore.AccountOutboxEntity;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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

	@Bean(defaultCandidate = false)
	public LocalContainerEntityManagerFactoryBean accountEntityManagerFactory(
			EntityManagerFactoryBuilder builder,
			@Qualifier("accountDataSource") DataSource dataSource,
			@Value("${account-store.jpa.ddl-auto:validate}") String ddlMode) {
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
