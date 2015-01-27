package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.SequencerPortableHook;

import java.io.IOException;

public class ClientAttachLogicalNodeRequest
        extends AbstractClientSequencerPartitionRequest {

    private SequencerDefinition definition;

    public ClientAttachLogicalNodeRequest() {
    }

    public ClientAttachLogicalNodeRequest(String sequencerName, int partitionId, SequencerDefinition definition) {
        super(sequencerName, partitionId);
        this.definition = definition;
    }

    @Override
    protected Operation prepareOperation() {
        return new ClientAttachLogicalNodeOperation(getSequencerName(), getEndpoint(), definition);
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_ATTACH_LOGICAL_NODE;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {

        super.write(writer);
        writer.writeLong("epoch", definition.getEpoch().getEpochOffset());
        writer.writeInt("mnc", definition.getMaxLogicalNodeCount());
        writer.writeShort("bc", definition.getBackupCount());
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);

        long epochOffset = reader.readLong("epoch");
        int maxLogicalNodeCount = reader.readInt("mnc");
        short backupCount = reader.readShort("bc");

        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        definition = new SequencerDefinition(getSequencerName(), epoch, maxLogicalNodeCount, backupCount);
    }
}
