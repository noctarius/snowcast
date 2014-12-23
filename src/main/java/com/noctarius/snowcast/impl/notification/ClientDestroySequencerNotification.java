package com.noctarius.snowcast.impl.notification;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerPortableHook;

import java.io.IOException;

public class ClientDestroySequencerNotification
        implements Portable {

    private String sequencerName;

    public ClientDestroySequencerNotification() {
    }

    public ClientDestroySequencerNotification(String sequencerName) {
        this.sequencerName = sequencerName;
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_DESTROY_SEQUENCER;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {

        writer.writeUTF("sn", sequencerName);
    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {

        this.sequencerName = reader.readUTF("sn");
    }

    public String getSequencerName() {
        return sequencerName;
    }
}
