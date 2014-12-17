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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.noctarius.snowcast.Snowcast;

public final class SnowcastFactory {

    private SnowcastFactory() {
    }

    public static Snowcast snowcast(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance instanceof HazelcastInstanceProxy) {
            return new NodeSnowcast(hazelcastInstance);
        }
        /*try {
            String className = SnowcastFactory.class.getPackage().getName() + ".ClientSnowcast";
            Class<? extends Snowcast> clazz = (Class<? extends Snowcast>) ClassLoaderUtil.loadClass(null, className);
            Constructor<? extends Snowcast> constructor = clazz.getDeclaredConstructor(HazelcastInstance.class);
            return constructor.newInstance(hazelcastInstance);
        } catch (Exception e) {
            throw new SnowcastException(e);
        }*/

        // Client not yet supported
        String className = hazelcastInstance.getClass().getCanonicalName();
        String message = ExceptionMessages.PARAMETER_IS_NOT_SUPPORTED.buildMessage(className);
        throw new IllegalArgumentException(message);
    }
}
