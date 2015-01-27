package com.noctarius.snowcast.impl.operations;

import com.hazelcast.spi.BackupOperation;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;

public class BackupDestroySequencerDefinitionOperation
        extends AbstractSequencerOperation
        implements BackupOperation {

    public BackupDestroySequencerDefinitionOperation() {
    }

    public BackupDestroySequencerDefinitionOperation(String sequencerName) {
        super(sequencerName);
    }

    @Override
    public int getId() {
        return SequencerDataSerializerHook.TYPE_BACKUP_DESTROY_SEQUENCER_DEFINITION;
    }

    @Override
    public void run()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        String sequencerName = getSequencerName();
        sequencerService.destroySequencer(sequencerName, true);
    }
}
