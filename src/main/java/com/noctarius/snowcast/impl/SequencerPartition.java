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

import com.hazelcast.nio.Address;
import com.noctarius.snowcast.SnowcastSequencerAlreadyRegisteredException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class SequencerPartition {

    private final int partitionId;

    private final ConcurrentMap<String, LogicalNodeTable> logicalNodeTables;

    public SequencerPartition(int partitionId) {
        this.partitionId = partitionId;
        this.logicalNodeTables = new ConcurrentHashMap<String, LogicalNodeTable>();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Integer attachLogicalNode(SequencerDefinition definition, Address address) {
        SequencerDefinition safeDefinition = checkOrRegisterSequencerDefinition(definition);
        String sequencerName = safeDefinition.getSequencerName();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            logicalNodeTable = new LogicalNodeTable(safeDefinition);
            LogicalNodeTable temp = logicalNodeTables.putIfAbsent(sequencerName, logicalNodeTable);
            if (temp != null) {
                logicalNodeTable = temp;
            }
        }
        return logicalNodeTable.attachLogicalNode(address);
    }

    public void detachLogicalNode(String sequencerName, Address address, int logicalNodeId) {
        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable != null) {
            logicalNodeTable.detachLogicalNode(address, logicalNodeId);
        }
    }

    SequencerDefinition checkOrRegisterSequencerDefinition(SequencerDefinition definition) {
        String sequencerName = definition.getSequencerName();
        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable != null) {
            SequencerDefinition other = logicalNodeTable.getSequencerDefinition();
            return checkSequencerDefinitions(definition, other);
        }

        synchronized (logicalNodeTables) {
            logicalNodeTable = logicalNodeTables.get(sequencerName);
            if (logicalNodeTable != null) {
                SequencerDefinition other = logicalNodeTable.getSequencerDefinition();
                return checkSequencerDefinitions(definition, other);
            }

            logicalNodeTables.put(sequencerName, new LogicalNodeTable(definition));
            return definition;
        }
    }

    private SequencerDefinition checkSequencerDefinitions(SequencerDefinition definition, SequencerDefinition other) {
        if (other != null && !other.equals(definition)) {
            String message = ExceptionMessages.SEQUENCER_ALREADY_REGISTERED.buildMessage();
            throw new SnowcastSequencerAlreadyRegisteredException(message);
        }
        return other != null ? other : definition;
    }

    SequencerDefinition destroySequencerDefinition(String sequencerName) {
        LogicalNodeTable logicalNodeTable = logicalNodeTables.remove(sequencerName);
        return logicalNodeTable != null ? logicalNodeTable.getSequencerDefinition() : null;
    }
}
