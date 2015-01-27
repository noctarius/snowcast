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
package com.noctarius.snowcast.impl;

import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.noctarius.snowcast.Snowcast;

public final class ClientSnowcastFactory {

    public static Snowcast snowcast(HazelcastInstance hazelcastInstance, short backupCount) {
        if (hazelcastInstance instanceof HazelcastClientProxy) {
            return new ClientSnowcast(hazelcastInstance, backupCount);
        }

        // Client not yet supported
        String className = hazelcastInstance.getClass().getCanonicalName();
        String message = ExceptionMessages.PARAMETER_IS_NOT_SUPPORTED.buildMessage(className);
        throw new IllegalArgumentException(message);
    }
}
