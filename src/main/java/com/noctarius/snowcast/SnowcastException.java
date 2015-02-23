/*
 * Copyright (c) 2014, Christoph Engelbert (aka noctarius) and
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
package com.noctarius.snowcast;

import javax.annotation.Nonnull;

/**
 * The SnowcastException is the base class for all other snowcast exception
 * classes. It is also often used to throw an exception for general problematic
 * situations where there is no special subclass provided.
 */
public class SnowcastException
        extends RuntimeException {

    /**
     * Basic exception constructor
     */
    SnowcastException() {
        super();
    }

    /**
     * Basic exception constructor with a given message
     *
     * @param message message to be used
     */
    public SnowcastException(@Nonnull String message) {
        super(message);
    }

    /**
     * Basic exception constructor with a given message and {@link java.lang.Throwable} cause.
     *
     * @param message message to be used
     * @param cause   causing throwable for this exception
     */
    public SnowcastException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    /**
     * Basic exception constructor with a given {@link java.lang.Throwable} cause.
     *
     * @param cause causing throwable for this exception
     */
    public SnowcastException(@Nonnull Throwable cause) {
        super(cause);
    }
}
