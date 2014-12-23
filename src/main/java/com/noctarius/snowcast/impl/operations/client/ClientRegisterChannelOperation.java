package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.spi.EventRegistration;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerPortableHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

public class ClientRegisterChannelOperation
        extends AbstractClientSequencerOperation {

    public ClientRegisterChannelOperation() {
    }

    public ClientRegisterChannelOperation(String sequencerName) {
        super(sequencerName);
    }

    @Override
    protected void invoke() {
        NodeSequencerService sequencerService = getService();
        EventRegistration registration = sequencerService.registerClientChannel(getSequencerName(), getEndpoint(), getCallId());

        ClientEndpoint endpoint = getEndpoint();
        String registrationId = registration.getId();

        endpoint.setListenerRegistration(SnowcastConstants.SERVICE_NAME, endpoint.getUuid(), registrationId);
        endpoint.sendResponse(registrationId, getCallId());
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_REGISTER_CHANNEL;
    }
}
