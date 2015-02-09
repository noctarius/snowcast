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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.noctarius.snowcast.impl.ExceptionMessages;
import com.noctarius.snowcast.impl.NodeSnowcastFactory;
import com.noctarius.snowcast.impl.SequencerService;

import java.lang.reflect.Method;
import java.util.Map;

import static com.noctarius.snowcast.impl.SnowcastConstants.USER_CONTEXT_LOOKUP_NAME;

/**
 * <p>The <ii>SnowcastSystem</ii> class is the entrance point into using snowcast. By providing the passed in
 * {@link com.hazelcast.core.HazelcastInstance} instance (no matter if it is a Hazelcast embedded node or Hazelcast client)
 * the internal factory will create the appropriate {@link com.noctarius.snowcast.Snowcast} instance.</p>
 * <p>Creation of the {@link com.noctarius.snowcast.Snowcast} instance is fully thread-safe, the creation result is cached and
 * only one instance will be created for the same {@link com.hazelcast.core.HazelcastInstance}.</p>
 * <p>
 * A basic example on how to use the snowcast API to generate IDs will show the following snippet:
 * <pre>
 * // Create a HazelcastInstance
 * HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
 *
 * // Create or retrieve the snowcast instance
 * Snowcast snowcast = SnowcastSystem.snowcast( hazelcastInstance );
 *
 * // Define the custom epoch of the following sequencer
 * Calendar calendar = GregorianCalendar.getInstance();
 * calendar.set( 2014, 1, 1, 0, 0, 0 );
 * SnowcastEpoch epoch = SnowcastEpoch.byCalendar( calendar );
 *
 * // Create the sequencer
 * SnowcastSequencer sequencer = snowcast.createSequencer( "name", epoch );
 *
 * // Generate IDs
 * while ( true ) {
 *     long id = sequencer.next();
 *     System.out.println( id );
 * }
 * </pre>
 * </p>
 */
public final class SnowcastSystem {

    private static final Object CREATION_LOCK = new Object();

    private SnowcastSystem() {
    }

    /**
     * Creates or retrieves (a cached) {@link com.noctarius.snowcast.Snowcast} instance assigned to the passed in
     * {@link com.hazelcast.core.HazelcastInstance}. An implicit value of 1 (ONE) will be configured as the backup count
     * for all {@link com.noctarius.snowcast.SnowcastSequencer} instances created by the returned
     * {@link com.noctarius.snowcast.Snowcast} instance.
     *
     * @param hazelcastInstance the HazelcastInstance with link the Snowcast instance to
     * @return a created or cached Snowcast instance, linked to the given HazelcastInstance
     * @throws com.noctarius.snowcast.SnowcastException if creation fails for various reasons
     */
    public static Snowcast snowcast(HazelcastInstance hazelcastInstance) {
        return snowcast(hazelcastInstance, 1);
    }

    /**
     * Creates or retrieves (a cached) {@link com.noctarius.snowcast.Snowcast} instance assigned to the passed in
     * {@link com.hazelcast.core.HazelcastInstance}. The defined <tt>backupCount</tt> will be set as the backup count
     * for all {@link com.noctarius.snowcast.SnowcastSequencer} instances created by the returned
     * {@link com.noctarius.snowcast.Snowcast} instance.
     *
     * @param hazelcastInstance the HazelcastInstance with link the Snowcast instance to
     * @param backupCount
     * @return a created or cached Snowcast instance, linked to the given HazelcastInstance
     * @throws com.noctarius.snowcast.SnowcastException if creation fails for various reasons
     */
    public static Snowcast snowcast(HazelcastInstance hazelcastInstance, int backupCount) {
        // Test for an already created instance first
        Map<String, Object> userContext = hazelcastInstance.getUserContext();
        Snowcast snowcast = (Snowcast) userContext.get(USER_CONTEXT_LOOKUP_NAME);
        if (snowcast != null) {
            return snowcast;
        }

        if (backupCount < 0) {
            throw new IllegalArgumentException(ExceptionMessages.BACKUP_COUNT_TOO_LOW.buildMessage());
        }
        if (backupCount > Short.MAX_VALUE) {
            String message = ExceptionMessages.BACKUP_COUNT_TOO_HIGH.buildMessage(Short.MAX_VALUE);
            throw new IllegalArgumentException(message);
        }

        synchronized (CREATION_LOCK) {
            snowcast = (Snowcast) userContext.get(USER_CONTEXT_LOOKUP_NAME);
            if (snowcast != null) {
                return snowcast;
            }

            // Node setup
            if (hazelcastInstance instanceof HazelcastInstanceProxy) {
                snowcast = NodeSnowcastFactory.snowcast(hazelcastInstance, (short) backupCount);
            }

            if (snowcast == null) {
                try {
                    String className = SequencerService.class.getPackage().getName() + ".ClientSnowcastFactory";
                    Class<?> clazz = ClassLoaderUtil.loadClass(null, className);
                    Method snowcastMethod = clazz.getMethod("snowcast", HazelcastInstance.class, short.class);
                    snowcast = (Snowcast) snowcastMethod.invoke(clazz, hazelcastInstance, (short) backupCount);

                } catch (Exception e) {
                    if (e instanceof SnowcastException) {
                        throw (SnowcastException) e;
                    }
                    throw new SnowcastException(e);
                }
            }

            userContext.put(USER_CONTEXT_LOOKUP_NAME, snowcast);
        }

        return snowcast;
    }
}
