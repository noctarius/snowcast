package com.noctarius.snowcast.impl;

import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.noctarius.snowcast.Snowcast;

public final class ClientSnowcastFactory {

    public static Snowcast snowcast(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance instanceof HazelcastClientProxy) {
            return new ClientSnowcast(hazelcastInstance);
        }

        // Client not yet supported
        String className = hazelcastInstance.getClass().getCanonicalName();
        String message = ExceptionMessages.PARAMETER_IS_NOT_SUPPORTED.buildMessage(className);
        throw new IllegalArgumentException(message);
    }
}
