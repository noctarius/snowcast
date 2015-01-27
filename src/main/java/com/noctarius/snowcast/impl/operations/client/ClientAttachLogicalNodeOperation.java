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
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.SequencerPartition;
import com.noctarius.snowcast.impl.operations.BackupAttachLogicalNodeOperation;

class ClientAttachLogicalNodeOperation
        extends AbstractClientRequestOperation
        implements BackupAwareOperation {

    private final SequencerDefinition definition;

    private Integer logicalNodeId;

    public ClientAttachLogicalNodeOperation(String sequencerName, ClientEndpoint endpoint, SequencerDefinition definition) {
        super(sequencerName, endpoint);
        this.definition = definition;
    }

    @Override
    public void run()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        SequencerPartition partition = sequencerService.getSequencerPartition(getPartitionId());
        logicalNodeId = partition.attachLogicalNode(definition, getEndpoint().getConnection().getEndPoint());
    }

    @Override
    public Object getResponse() {
        return logicalNodeId;
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
        return new BackupAttachLogicalNodeOperation(definition, logicalNodeId, getEndpoint().getConnection().getEndPoint());
    }
}
