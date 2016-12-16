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
package com.noctarius.snowcast.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.PartitionReplication;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerPartition;

import java.io.IOException;

public class SequencerReplicationOperation
        extends Operation
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

        NodeSequencerService sequencerService = getService();

        int partitionId = partitionReplication.getPartitionId();
        SequencerPartition partition = sequencerService.getSequencerPartition(partitionId);

        partitionReplication.applyReplication(partition);
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
