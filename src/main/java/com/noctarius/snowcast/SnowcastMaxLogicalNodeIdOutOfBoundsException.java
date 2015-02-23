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
 * The SnowcastMaxLogicalNodeIdOutOfBoundsException is thrown whenever the given
 * maximal logical node ID is outside the legal bound between 128 and 8192.
 */
public class SnowcastMaxLogicalNodeIdOutOfBoundsException
        extends SnowcastException {

    /**
     * Basic exception constructor
     *
     * @param message message to be used
     */
    public SnowcastMaxLogicalNodeIdOutOfBoundsException(@Nonnull String message) {
        super(message);
    }
}
