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
package com.noctarius.snowcast.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class PartitionReplication
        implements IdentifiedDataSerializable {

    private int partitionId;
    private Collection<LogicalNodeTable> logicalNodeTables;

    public PartitionReplication() {
    }

    public PartitionReplication(@Nonnegative int partitionId, @Nonnull Collection<LogicalNodeTable> logicalNodeTables) {
        this.partitionId = partitionId;
        this.logicalNodeTables = logicalNodeTables;
    }

    @Nonnegative
    public int getPartitionId() {
        return partitionId;
    }

    public void applyReplication(@Nonnull SequencerPartition targetPartition)
            throws Exception {

        targetPartition.freeze();
        try {
            for (LogicalNodeTable logicalNodeTable : logicalNodeTables) {
                targetPartition.mergeLogicalNodeTable(logicalNodeTable);
            }
        } finally {
            targetPartition.unfreeze();
        }
    }

    @Override
    public void writeData(@Nonnull ObjectDataOutput out)
            throws IOException {

        out.writeInt(partitionId);
        out.writeInt(logicalNodeTables.size());
        for (LogicalNodeTable logicalNodeTable : logicalNodeTables) {
            LogicalNodeTable.writeLogicalNodeTable(logicalNodeTable, out);
        }
    }

    @Override
    public void readData(@Nonnull ObjectDataInput in)
            throws IOException {

        partitionId = in.readInt();
        int size = in.readInt();
        logicalNodeTables = new ArrayList<LogicalNodeTable>(size);
        for (int i = 0; i < size; i++) {
            LogicalNodeTable logicalNodeTable = LogicalNodeTable.readLogicalNodeTable(partitionId, in);
            logicalNodeTables.add(logicalNodeTable);
        }
    }

    @Override
    @Nonnegative
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    @Nonnegative
    public int getId() {
        return SequencerDataSerializerHook.TYPE_PARTITION_REPLICATION;
    }
}
