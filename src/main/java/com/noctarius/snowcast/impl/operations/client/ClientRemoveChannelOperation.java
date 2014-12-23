package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerPortableHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

import java.security.Permission;

public class ClientRemoveChannelOperation
        extends BaseClientRemoveListenerRequest {

    public ClientRemoveChannelOperation() {
    }

    public ClientRemoveChannelOperation(String sequencerName, String registrationId) {
        super(sequencerName, registrationId);
    }

    @Override
    public Object call()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        sequencerService.unregisterClientChannel(getName(), registrationId);
        return Boolean.TRUE;
    }

    @Override
    public String getServiceName() {
        return SnowcastConstants.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_REMOVE_CHANNEL;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
