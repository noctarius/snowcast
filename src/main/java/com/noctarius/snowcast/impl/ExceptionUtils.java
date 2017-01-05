/*
 * Copyright (c) 2015-2017, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.noctarius.snowcast.impl;

import com.noctarius.snowcast.SnowcastException;

import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

public class ExceptionUtils {

    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    public static SnowcastException exception(ExceptionMessages exceptionMessage, Object... exceptionParameters) {
        return exception(SnowcastException::new, exceptionMessage, exceptionParameters);
    }

    public static <E extends Exception> E exception(Function<String, E> constructor, ExceptionMessages exceptionMessage,
                                                    Object... exceptionParameters) {

        String message = exceptionMessage.buildMessage(exceptionParameters);
        return constructor.apply(message);
    }

    public static Supplier<Object[]> exceptionParameters(Object... args) {
        return () -> args;
    }

    public static <V> V execute(ThrowingCallable<V> callable) {
        try {
            return callable.call();
        } catch (Throwable throwable) {
            if (throwable instanceof SnowcastException) {
                throw (SnowcastException) throwable;
            } else if (throwable instanceof Error) {
                throw (Error) throwable;
            }
            throw new SnowcastException(throwable);
        }
    }

    public static <V> V execute(Callable<V> callable, ExceptionMessages exceptionMessage) {
        return execute(callable, exceptionMessage, () -> EMPTY_OBJECT_ARRAY);
    }

    public static <V> V execute(Callable<V> callable, ExceptionMessages exceptionMessage,
                                Supplier<Object[]> exceptionParameterSupplier) {

        try {
            return callable.call();
        } catch (Throwable throwable) {
            Object[] parameters = exceptionParameterSupplier.get();
            String message = exceptionMessage.buildMessage(parameters);
            throw new SnowcastException(message);
        }
    }

    public interface ThrowingCallable<V> {
        V call()
                throws Throwable;
    }
}
