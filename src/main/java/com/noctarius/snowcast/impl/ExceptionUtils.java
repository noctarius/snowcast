package com.noctarius.snowcast.impl;

import com.noctarius.snowcast.SnowcastException;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

class ExceptionUtils {

    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    static Supplier<Object[]> exceptionParameters(Object... args) {
        return () -> args;
    }

    static <V> V execute(Callable<V> callable, ExceptionMessages exceptionMessage) {
        return execute(callable, exceptionMessage, () -> EMPTY_OBJECT_ARRAY);
    }

    static <V> V execute(Callable<V> callable, ExceptionMessages exceptionMessage,
                         Supplier<Object[]> exceptionParameterSupplier) {

        try {
            return callable.call();
        } catch (Throwable throwable) {
            Object[] parameters = exceptionParameterSupplier.get();
            String message = exceptionMessage.buildMessage(parameters);
            throw new SnowcastException(message);
        }
    }
}
