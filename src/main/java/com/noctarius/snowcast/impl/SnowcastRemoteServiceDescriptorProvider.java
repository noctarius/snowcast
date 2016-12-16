package com.noctarius.snowcast.impl;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptor;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptorProvider;

public final class SnowcastRemoteServiceDescriptorProvider
        implements RemoteServiceDescriptorProvider {

    @Override
    public RemoteServiceDescriptor[] createRemoteServiceDescriptors() {
        return new RemoteServiceDescriptor[] {
                new RemoteServiceDescriptor() {
                    @Override
                    public String getServiceName() {
                        return SnowcastConstants.SERVICE_NAME;
                    }

                    @Override
                    public RemoteService getService(NodeEngine nodeEngine) {
                        return new NodeSequencerService();
                    }
                }
        };
    }
}
