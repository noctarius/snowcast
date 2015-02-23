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
 * The SnowcastIllegalStateException is thrown whenever an illegal state change
 * is tried to execute or when an operation is tried to execute that is not
 * legal in the current state of the {@link com.noctarius.snowcast.SnowcastSequencer},
 * for example {@link SnowcastSequencer#next()} in states
 * {@link com.noctarius.snowcast.SnowcastSequenceState#Detached} or
 * {@link com.noctarius.snowcast.SnowcastSequenceState#Destroyed}.
 */
public class SnowcastIllegalStateException
        extends SnowcastException {

    /**
     * Basic exception constructor
     *
     * @param message message to be used
     */
    public SnowcastIllegalStateException(@Nonnull String message) {
        super(message);
    }
}
