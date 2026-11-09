package com.pricing.engine.config;

import java.sql.SQLException;
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
			@Qualifier("recommendationDataSourceProperties") DataSourceProperties properties,
			@Value("${engine.max-workers:3}") int maxWorkers) {
		HikariDataSource dataSource = properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
		dataSource.setMaximumPoolSize(Math.max(3, maxWorkers));
		return dataSource;
	}

	@Bean(defaultCandidate = false, initMethod = "migrate")
	public Flyway recommendationFlyway(
			@Qualifier("recommendationDataSourceProperties") DataSourceProperties runtime,
			@Qualifier("initializationDataSourceProperties") DataSourceProperties initialization,
			@Value("${database-initialization.enabled:true}") boolean initialize,
			@Value("${recommendation-store.txn-processor.username:recommendationdb_txn_processor}")
			String processorUser,
			@Value("${recommendation-store.txn-processor.password:recommendationpass}")
			String processorPassword) throws SQLException {
		DataSourceProperties credentials = initialize ? initialization : runtime;
		String database = initialize ? PricingStoreConfiguration.initializeDatabase(
				runtime, initialization, processorUser, processorPassword) : null;
		var migrations = Flyway.configure().dataSource(runtime.getUrl(), credentials.getUsername(),
				credentials.getPassword())
				.locations("classpath:db/recommendation");
		if (initialize) {
			migrations.locations("classpath:db/recommendation", "classpath:db/recommendation-postgresql")
					.placeholders(Map.of("processorLogin", processorUser.replace("\"", "\"\""),
							"engineLogin", runtime.getUsername().replace("\"", "\"\""),
							"recommendationDatabase", database.replace("\"", "\"\"")));
		}
		return migrations.load();
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
