package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SnowcastDetachLogicalNodeCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.impl.SequencerDefinition;

public class DetachLogicalNodeMessageTask
        extends AbstractSnowcastMessageTask<SnowcastDetachLogicalNodeCodec.RequestParameters> {

    DetachLogicalNodeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation createOperation() {
        String sequencerName = parameters.sequencerName;
        int maxLogicalNodeCount = parameters.maxLogicalNodeCount;
        long epochOffset = parameters.epochOffset;
        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        short backupCount = (short) parameters.backupCount;
        SequencerDefinition sequencerDefinition = new SequencerDefinition(sequencerName, epoch, maxLogicalNodeCount, backupCount);

        int logicalNodeId = parameters.logicalNodeId;

        return new ClientDetachLogicalNodeOperation(sequencerName, sequencerDefinition, this, logicalNodeId);
    }

    @Override
    protected SnowcastDetachLogicalNodeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SnowcastDetachLogicalNodeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SnowcastDetachLogicalNodeCodec.encodeResponse((Boolean) response);
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
