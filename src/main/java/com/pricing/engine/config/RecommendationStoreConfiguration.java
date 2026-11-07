package com.pricing.engine.config;

import java.util.Map;

import javax.sql.DataSource;

import com.pricing.engine.recommendationStore.RecommendationOutboxEntity;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.boot.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

@Configuration(proxyBeanMethods = false)
public class RecommendationStoreConfiguration {

	@Bean(defaultCandidate = false)
	@ConfigurationProperties("recommendation-store.datasource")
	public DataSourceProperties recommendationDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean(defaultCandidate = false)
	public HikariDataSource recommendationDataSource(
			@Qualifier("recommendationDataSourceProperties") DataSourceProperties properties) {
		return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
	}

	@Bean(defaultCandidate = false, initMethod = "migrate")
	public Flyway recommendationFlyway(
			@Qualifier("recommendationDataSource") DataSource dataSource) {
		return Flyway.configure().dataSource(dataSource)
				.locations("classpath:db/recommendation").load();
	}

	@Bean(defaultCandidate = false)
	@DependsOn("recommendationFlyway")
	public LocalContainerEntityManagerFactoryBean recommendationEntityManagerFactory(
			EntityManagerFactoryBuilder builder,
			@Qualifier("recommendationDataSource") DataSource dataSource,
			@Value("${recommendation-store.jpa.ddl-auto:validate}") String ddlMode) {
		return builder.dataSource(dataSource).packages(RecommendationOutboxEntity.class)
				.persistenceUnit("recommendations")
				.properties(Map.of("hibernate.hbm2ddl.auto", ddlMode)).build();
	}

	@Bean(defaultCandidate = false)
	public JpaTransactionManager recommendationTransactionManager(
			@Qualifier("recommendationEntityManagerFactory") EntityManagerFactory factory) {
		return new JpaTransactionManager(factory);
	}
}
