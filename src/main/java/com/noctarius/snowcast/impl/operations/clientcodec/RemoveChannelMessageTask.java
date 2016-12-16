package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SnowcastRemoveChannelCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

class RemoveChannelMessageTask
        extends AbstractSnowcastMessageTask<SnowcastRemoveChannelCodec.RequestParameters> {

    RemoveChannelMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation createOperation() {
        return new ClientRemoveChannelOperation(parameters.sequencerName, this, parameters.registrationId);
    }

    @Override
    protected SnowcastRemoveChannelCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SnowcastRemoveChannelCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SnowcastRemoveChannelCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.sequencerName;
    }

    @Override
    public String getMethodName() {
        return "execute";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
