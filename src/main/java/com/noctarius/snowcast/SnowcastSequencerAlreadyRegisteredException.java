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

/**
 * The SnowcastSequencerAlreadyRegisteredException is thrown whenever there is requested
 * sequencer is already registered with the snowcast system in the cluster but the
 * configurations do not match. snowcast follows the fail-fast principle to prevent
 * conflicting IDs to be generated.
 */
public class SnowcastSequencerAlreadyRegisteredException
        extends SnowcastException {

    /**
     * Basic exception constructor
     *
     * @param message message to be used
     */
    public SnowcastSequencerAlreadyRegisteredException(String message) {
        super(message);
    }
}
