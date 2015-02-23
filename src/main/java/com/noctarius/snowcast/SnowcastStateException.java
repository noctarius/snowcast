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
 * The SnowcastStateException is used whenever there is an exception in the internal
 * state machine of a {@link com.noctarius.snowcast.SnowcastSequencer}. This can happen
 * for example if {@link SnowcastSequencer#next()} is called on a <tt>detached</tt>
 * sequencer instance.
 */
public class SnowcastStateException
        extends SnowcastException {

    /**
     * Basic exception constructor
     *
     * @param message message to be used
     */
    public SnowcastStateException(@Nonnull String message) {
        super(message);
    }
}
