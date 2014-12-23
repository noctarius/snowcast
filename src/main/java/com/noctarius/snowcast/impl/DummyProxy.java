package com.noctarius.snowcast.impl;

import com.hazelcast.core.DistributedObject;

public class DummyProxy implements DistributedObject {

    private final String sequencerName;

    public DummyProxy(String sequencerName) {
        this.sequencerName = sequencerName;
    }

    @Override
    public Object getId() {
        return sequencerName;
    }

    @Override
    public String getPartitionKey() {
        return sequencerName;
    }

    @Override
    public String getName() {
        return sequencerName;
    }

    @Override
    public String getServiceName() {
        return SnowcastConstants.SERVICE_NAME;
    }

    @Override
    public void destroy() {
    }
}
