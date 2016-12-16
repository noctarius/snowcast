package com.noctarius.snowcast.impl;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyDescriptor;
import com.hazelcast.client.spi.ClientProxyDescriptorProvider;

public final class SnowcastClientProxyDescriptorProvider
        implements ClientProxyDescriptorProvider {

    @Override
    public ClientProxyDescriptor[] createClientProxyDescriptors() {
        return new ClientProxyDescriptor[] {
                new ClientProxyDescriptor() {
                    @Override
                    public String getServiceName() {
                        return SnowcastConstants.SERVICE_NAME;
                    }

                    @Override
                    public Class<? extends ClientProxy> getClientProxyClass() {
                        return ClientSequencer.class;
                    }
                }
        };
    }
}
