/*
 * Copyright (c) 2014, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.SequencerPartition;
import com.noctarius.snowcast.impl.operations.BackupDetachLogicalNodeOperation;

import java.io.IOException;

class ClientDetachLogicalNodeOperation
        extends AbstractClientRequestOperation
        implements BackupAwareOperation {

    private int logicalNodeId;
    private final SequencerDefinition definition;

    ClientDetachLogicalNodeOperation(String sequencerName, SequencerDefinition definition, ClientEndpoint endpoint,
                                     int logicalNodeId) {

        super(sequencerName, endpoint);
        this.definition = definition;
        this.logicalNodeId = logicalNodeId;
    }

    @Override
    public void run()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        SequencerPartition partition = sequencerService.getSequencerPartition(getPartitionId());
        partition.detachLogicalNode(getSequencerName(), getEndpoint().getConnection().getEndPoint(), logicalNodeId);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {

        super.writeInternal(out);
        out.writeInt(logicalNodeId);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {

        super.readInternal(in);
        logicalNodeId = in.readInt();
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return definition.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new BackupDetachLogicalNodeOperation(definition, logicalNodeId, getEndpoint().getConnection().getEndPoint());
    }
}
