package com.noctarius.snowcast.impl.operations;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.SequencerPartition;

import java.io.IOException;

public class BackupDetachLogicalNodeOperation
        extends AbstractSequencerOperation
        implements BackupOperation {

    private SequencerDefinition definition;
    private int logicalNodeId;
    private Address address;

    public BackupDetachLogicalNodeOperation() {
    }

    public BackupDetachLogicalNodeOperation(SequencerDefinition definition, int logicalNodeId, Address address) {
        super(definition.getSequencerName());
        this.definition = definition;
        this.logicalNodeId = logicalNodeId;
        this.address = address;
    }

    @Override
    public int getId() {
        return SequencerDataSerializerHook.TYPE_BACKUP_DETACH_LOGICAL_NODE;
    }

    @Override
    public void run()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        SequencerPartition partition = sequencerService.getSequencerPartition(getPartitionId());
        partition.unassignLogicalNode(definition, logicalNodeId, address);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {

        super.writeInternal(out);
        out.writeInt(logicalNodeId);
        address.writeData(out);
        out.writeLong(definition.getEpoch().getEpochOffset());
        out.writeInt(definition.getMaxLogicalNodeCount());
        out.writeShort(definition.getBackupCount());
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {

        super.readInternal(in);
        logicalNodeId = in.readInt();
        address = new Address();
        address.readData(in);

        long epochOffset = in.readLong();
        int maxLogicalNodeCount = in.readInt();
        short backupCount = in.readShort();

        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        definition = new SequencerDefinition(getSequencerName(), epoch, maxLogicalNodeCount, backupCount);
    }
}
