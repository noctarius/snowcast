package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.operations.BackupCreateSequencerDefinitionOperation;

class ClientCreateSequencerDefinitionOperation
        extends AbstractClientRequestOperation
        implements BackupAwareOperation {

    private final SequencerDefinition definition;
    private SequencerDefinition response;

    ClientCreateSequencerDefinitionOperation(String sequencerName, ClientEndpoint endpoint, SequencerDefinition definition) {
        super(sequencerName, endpoint);
        this.definition = definition;
    }

    @Override
    public void run()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        response = sequencerService.registerSequencerDefinition(definition);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return response.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new BackupCreateSequencerDefinitionOperation(definition);
    }
}
