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
