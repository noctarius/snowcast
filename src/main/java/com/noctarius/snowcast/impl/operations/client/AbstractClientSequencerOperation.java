package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

import java.io.IOException;
import java.security.Permission;

public abstract class AbstractClientSequencerOperation
        extends InvocationClientRequest {

    private String sequencerName;

    public AbstractClientSequencerOperation() {
    }

    public AbstractClientSequencerOperation(String sequencerName) {
        this.sequencerName = sequencerName;
    }

    public String getSequencerName() {
        return sequencerName;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {

        super.write(writer);
        writer.writeUTF("sequencerName", sequencerName);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);
        sequencerName = reader.readUTF("sequencerName");
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getServiceName() {
        return SnowcastConstants.SERVICE_NAME;
    }
}
