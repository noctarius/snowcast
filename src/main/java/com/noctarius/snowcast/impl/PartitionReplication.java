package com.noctarius.snowcast.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class PartitionReplication
        implements IdentifiedDataSerializable {

    private int partitionId;
    private Collection<LogicalNodeTable> logicalNodeTables;

    public PartitionReplication() {
    }

    public PartitionReplication(int partitionId, Collection<LogicalNodeTable> logicalNodeTables) {
        this.partitionId = partitionId;
        this.logicalNodeTables = logicalNodeTables;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        out.writeInt(partitionId);
        out.writeInt(logicalNodeTables.size());
        for (LogicalNodeTable logicalNodeTable : logicalNodeTables) {
            LogicalNodeTable.writeLogicalNodeTable(logicalNodeTable, out);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        partitionId = in.readInt();
        int size = in.readInt();
        logicalNodeTables = new ArrayList<LogicalNodeTable>(size);
        for (int i = 0; i < size; i++) {
            LogicalNodeTable logicalNodeTable = LogicalNodeTable.readLogicalNodeTable(in);
            logicalNodeTables.add(logicalNodeTable);
        }
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return SequencerDataSerializerHook.TYPE_PARTITION_REPLICATION;
    }
}
