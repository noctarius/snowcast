package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SnowcastRegisterChannelCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

class RegisterChannelMessageTask
        extends AbstractSnowcastMessageTask<SnowcastRegisterChannelCodec.RequestParameters> {

    RegisterChannelMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation createOperation() {
        return new ClientRegisterChannelOperation(parameters.sequencerName, this);
    }

    @Override
    protected SnowcastRegisterChannelCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SnowcastRegisterChannelCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SnowcastRegisterChannelCodec.encodeResponse((String) response);
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
