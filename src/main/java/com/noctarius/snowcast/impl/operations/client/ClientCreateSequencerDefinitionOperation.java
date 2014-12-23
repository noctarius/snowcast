package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.ClientEndpoint;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDefinition;

class ClientCreateSequencerDefinitionOperation
        extends AbstractClientRequestOperation {

    private final SequencerDefinition definition;
    private SequencerDefinition response;

    ClientCreateSequencerDefinitionOperation(String sequencerName, ClientEndpoint endpoint,
                                                    SequencerDefinition definition) {
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
}
