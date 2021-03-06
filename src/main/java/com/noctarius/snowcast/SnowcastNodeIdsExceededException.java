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
package com.noctarius.snowcast;

/**
 * The SnowcastNodeIdsExceededException is thrown whenever no the configured number
 * of logical node IDs is exceeded.<br>
 * If more logical node IDs are required the {@link com.noctarius.snowcast.SnowcastSequencer}
 * must be destroyed and the configuration can be changed by recreating it.
 */
public class SnowcastNodeIdsExceededException
        extends SnowcastException {

    /**
     * Basic exception constructor
     */
    public SnowcastNodeIdsExceededException() {
    }
}
