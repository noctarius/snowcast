package com.noctarius.snowcast.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.noctarius.snowcast.impl.PartitionReplication;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;

import java.io.IOException;

public class SequencerReplicationOperation
        extends AbstractOperation
        implements IdentifiedDataSerializable {

    private PartitionReplication partitionReplication;

    public SequencerReplicationOperation() {
    }

    public SequencerReplicationOperation(PartitionReplication partitionReplication) {
        this.partitionReplication = partitionReplication;
    }

    @Override
    public void run()
            throws Exception {

        // TODO Apply replication
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return SequencerDataSerializerHook.TYPE_REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {

        super.writeInternal(out);
        out.writeObject(partitionReplication);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {

        super.readInternal(in);
        partitionReplication = in.readObject();
    }
}
