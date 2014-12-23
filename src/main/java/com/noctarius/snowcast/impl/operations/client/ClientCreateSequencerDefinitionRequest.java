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

public class ClientCreateSequencerDefinitionRequest
        extends AbstractClientSequencerPartitionRequest {

    private SequencerDefinition definition;

    public ClientCreateSequencerDefinitionRequest() {
    }

    public ClientCreateSequencerDefinitionRequest(String sequencerName, int partitionId, SequencerDefinition definition) {
        super(sequencerName, partitionId);
        this.definition = definition;
    }

    @Override
    protected Operation prepareOperation() {
        return new ClientCreateSequencerDefinitionOperation(getSequencerName(), getEndpoint(), definition);
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_CREATE_SEQUENCER_DEFINITION;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {

        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeLong(definition.getEpoch().getEpochOffset());
        out.writeInt(definition.getMaxLogicalNodeCount());
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);

        ObjectDataInput in = reader.getRawDataInput();
        long epochOffset = in.readLong();
        int maxLogicalNodeCount = in.readInt();

        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        definition = new SequencerDefinition(getSequencerName(), epoch, maxLogicalNodeCount);
    }
}
