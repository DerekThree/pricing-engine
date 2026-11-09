package com.pricing.engine.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.junit.jupiter.api.function.ThrowingSupplier;

public final class JdbcUtils {

	@FunctionalInterface
	public interface Interceptor {
		Object invoke(Method method, Object[] arguments, ThrowingSupplier<Object> proceed)
				throws Throwable;
	}

	public static Object intercept(Object target, Class<?> type, Interceptor interceptor) {
		return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
				(proxy, method, arguments) -> interceptor.invoke(method, arguments, () -> {
					try {
						return method.invoke(target, arguments);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				}));
	}
}
