package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.impl.SequencerPortableHook;

import java.io.IOException;

public class ClientDetachLogicalNodeRequest
        extends AbstractClientSequencerPartitionRequest {

    private int logicalNodeId;

    public ClientDetachLogicalNodeRequest() {
    }

    public ClientDetachLogicalNodeRequest(String sequencerName, int partitionId, int logicalNodeId) {
        super(sequencerName, partitionId);
        this.logicalNodeId = logicalNodeId;
    }

    @Override
    protected Operation prepareOperation() {
        return new ClientDetachLogicalNodeOperation(getSequencerName(), getEndpoint(), logicalNodeId);
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
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);
        this.logicalNodeId = reader.readInt("lni");
    }
}
