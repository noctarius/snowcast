package com.noctarius.snowcast.impl;

import com.hazelcast.core.HazelcastInstance;
import com.noctarius.snowcast.Snowcast;

public final class NodeSnowcastFactory {

    public static Snowcast snowcast(HazelcastInstance hazelcastInstance, short backupCount) {
        return new NodeSnowcast(hazelcastInstance, backupCount);
    }
}
