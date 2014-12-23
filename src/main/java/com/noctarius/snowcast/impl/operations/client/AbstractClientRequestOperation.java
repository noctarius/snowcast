package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public abstract class AbstractClientRequestOperation
        extends Operation
        implements PartitionAwareOperation {

    private final String sequencerName;
    private final ClientEndpoint endpoint;

    protected AbstractClientRequestOperation(String sequencerName, ClientEndpoint endpoint) {
        this.sequencerName = sequencerName;
        this.endpoint = endpoint;
    }

    @Override
    public void beforeRun()
            throws Exception {
    }

    @Override
    public void afterRun()
            throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
    }

    public String getSequencerName() {
        return sequencerName;
    }

    public ClientEndpoint getEndpoint() {
        return endpoint;
    }
}
