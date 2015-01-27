package com.noctarius.snowcast.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerDefinition;

import java.io.IOException;

public class BackupCreateSequencerDefinitionOperation
        extends AbstractSequencerOperation
        implements BackupOperation {

    private SequencerDefinition definition;

    public BackupCreateSequencerDefinitionOperation() {
    }

    public BackupCreateSequencerDefinitionOperation(SequencerDefinition definition) {
        super(definition.getSequencerName());
        this.definition = definition;
    }

    @Override
    public int getId() {
        return SequencerDataSerializerHook.TYPE_BACKUP_CREATE_SEQUENCER_DEFINITION;
    }

    @Override
    public void run()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        sequencerService.registerSequencerDefinition(definition);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {

        super.writeInternal(out);
        out.writeLong(definition.getEpoch().getEpochOffset());
        out.writeInt(definition.getMaxLogicalNodeCount());
        out.writeShort(definition.getBackupCount());
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {

        super.readInternal(in);

        long epochOffset = in.readLong();
        int maxLogicalNodeCount = in.readInt();
        short backupCount = in.readShort();

        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        definition = new SequencerDefinition(getSequencerName(), epoch, maxLogicalNodeCount, backupCount);
    }
}
