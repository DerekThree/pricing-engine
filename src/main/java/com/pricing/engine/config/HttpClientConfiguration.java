package com.pricing.engine.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
public class HttpClientConfiguration {

	@Bean
	public RestClient transactionProcessorClient(
			@Value("${transaction-processor.wake-up-url}") String wakeUpUrl,
			@Value("${transaction-processor.http-timeout}") Duration timeout) {
		Assert.isTrue(timeout.toMillis() > 0, "Transaction Processor HTTP timeout must be positive");
		SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
		factory.setConnectTimeout(timeout);
		factory.setReadTimeout(timeout);
		return RestClient.builder().baseUrl(wakeUpUrl).requestFactory(factory).build();
	}
}
