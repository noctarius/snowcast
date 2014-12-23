package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

import java.io.IOException;
import java.security.Permission;

public abstract class AbstractClientSequencerPartitionRequest
        extends PartitionClientRequest {

    private String sequencerName;
    private int partitionId;

    public AbstractClientSequencerPartitionRequest() {
    }

    public AbstractClientSequencerPartitionRequest(String sequencerName, int partitionId) {
        this.sequencerName = sequencerName;
        this.partitionId = partitionId;
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return SnowcastConstants.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    public String getSequencerName() {
        return sequencerName;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {

        super.write(writer);
        writer.writeUTF("sqn", sequencerName);
        writer.writeInt("pid", partitionId);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);
        this.sequencerName = reader.readUTF("sqn");
        this.partitionId = reader.readInt("pid");
    }
}
