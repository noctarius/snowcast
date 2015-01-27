package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.SequencerPortableHook;

import java.io.IOException;

public class ClientDetachLogicalNodeRequest
        extends AbstractClientSequencerPartitionRequest {

    private int logicalNodeId;
    private SequencerDefinition definition;

    public ClientDetachLogicalNodeRequest() {
    }

    public ClientDetachLogicalNodeRequest(SequencerDefinition definition, int partitionId, int logicalNodeId) {
        super(definition.getSequencerName(), partitionId);
        this.definition = definition;
        this.logicalNodeId = logicalNodeId;
    }

    @Override
    protected Operation prepareOperation() {
        return new ClientDetachLogicalNodeOperation(getSequencerName(), definition, getEndpoint(), logicalNodeId);
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_DETACH_LOGICAL_NODE;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {

        super.write(writer);
        writer.writeInt("lni", logicalNodeId);
        writer.writeLong("epoch", definition.getEpoch().getEpochOffset());
        writer.writeInt("mnc", definition.getMaxLogicalNodeCount());
        writer.writeShort("bc", definition.getBackupCount());
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);
        this.logicalNodeId = reader.readInt("lni");

        long epochOffset = reader.readLong("epoch");
        int maxLogicalNodeCount = reader.readInt("mnc");
        short backupCount = reader.readShort("bc");

        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        definition = new SequencerDefinition(getSequencerName(), epoch, maxLogicalNodeCount, backupCount);
    }
}
