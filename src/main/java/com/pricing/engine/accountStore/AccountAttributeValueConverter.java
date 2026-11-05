package com.pricing.engine.accountStore;

import java.math.BigDecimal;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

@Converter
public class AccountAttributeValueConverter implements AttributeConverter<Object, String> {

	private static final JsonMapper JSON = JsonMapper.builder()
			.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
			.enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
			.build();

	@Override
	public String convertToDatabaseColumn(Object value) {
		return JSON.writeValueAsString(value);
	}

	@Override
	public Object convertToEntityAttribute(String value) {
		Object scalar = JSON.readValue(value, Object.class);
		return scalar instanceof Number number ? new BigDecimal(number.toString()) : scalar;
	}
}
