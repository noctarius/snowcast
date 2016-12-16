package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SnowcastCreateSequencerDefinitionCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.impl.SequencerDefinition;

class CreateSequencerDefinitionMessageTask
        extends AbstractSnowcastMessageTask<SnowcastCreateSequencerDefinitionCodec.RequestParameters> {

    CreateSequencerDefinitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected SnowcastCreateSequencerDefinitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SnowcastCreateSequencerDefinitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        SequencerDefinition definition = (SequencerDefinition) response;

        String sequencerName = definition.getSequencerName();
        long epochOffset = definition.getEpoch().getEpochOffset();
        int maxLogicalNodeCount = definition.getMaxLogicalNodeCount();
        short backupCount = definition.getBackupCount();

        return SnowcastCreateSequencerDefinitionCodec
                .encodeResponse(sequencerName, epochOffset, maxLogicalNodeCount, backupCount);
    }

    @Override
    protected Operation createOperation() {
        String sequencerName = parameters.sequencerName;
        int maxLogicalNodeCount = parameters.maxLogicalNodeCount;
        long epochOffset = parameters.epochOffset;
        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        short backupCount = (short) parameters.backupCount;
        SequencerDefinition sequencerDefinition = new SequencerDefinition(sequencerName, epoch, maxLogicalNodeCount, backupCount);

        return new ClientCreateSequencerDefinitionOperation(sequencerName, this, sequencerDefinition);
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
