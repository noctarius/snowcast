package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SnowcastDestroySequencerDefinitionCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

class DestroySequencerDefinitionMessageTask
        extends AbstractSnowcastMessageTask<SnowcastDestroySequencerDefinitionCodec.RequestParameters> {

    DestroySequencerDefinitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation createOperation() {
        return new ClientDestroySequencerDefinitionOperation(parameters.sequencerName, this);
    }

    @Override
    protected SnowcastDestroySequencerDefinitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SnowcastDestroySequencerDefinitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SnowcastDestroySequencerDefinitionCodec.encodeResponse((Boolean) response);
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
