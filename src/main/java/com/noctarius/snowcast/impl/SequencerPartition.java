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
import com.hazelcast.nio.UnsafeHelper;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastSequencerAlreadyRegisteredException;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class SequencerPartition {

    private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    private static final long FROZEN_OFFSET;
    private static final int FROZEN = 1;
    private static final int UNFROZEN = 0;

    static {
        try {
            Field frozen = SequencerPartition.class.getDeclaredField("frozen");
            frozen.setAccessible(true);

            FROZEN_OFFSET = UNSAFE.objectFieldOffset(frozen);
        } catch (Exception e) {
            throw new SnowcastException(e);
        }
    }

    private final int partitionId;

    private final ConcurrentMap<String, LogicalNodeTable> logicalNodeTables;

    private volatile int frozen = UNFROZEN;

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

    Address getAttachedLogicalNode(String sequencerName, int logicalNodeId) {
        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            throw new IllegalStateException();
        }
        return logicalNodeTable.getAttachedLogicalNode(logicalNodeId);
    }

    SequencerDefinition getSequencerDefinition(String sequencerName) {
        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            return null;
        }
        return logicalNodeTable.getSequencerDefinition();
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

            if (frozen == FROZEN) {
                throw new SnowcastException("Current partition is frozen!");
            }

            logicalNodeTables.put(sequencerName, new LogicalNodeTable(definition));
            return definition;
        }
    }

    PartitionReplication createPartitionReplication() {
        return new PartitionReplication(partitionId, logicalNodeTables.values());
    }

    SequencerDefinition destroySequencerDefinition(String sequencerName) {
        LogicalNodeTable logicalNodeTable = logicalNodeTables.remove(sequencerName);
        return logicalNodeTable != null ? logicalNodeTable.getSequencerDefinition() : null;
    }

    void freeze() {
        while (true) {
            int frozen = this.frozen;
            if (frozen == FROZEN) {
                return;
            }
            if (UNSAFE.compareAndSwapInt(this, FROZEN_OFFSET, frozen, FROZEN)) {
                return;
            }
        }
    }

    void unfreeze() {
        while (true) {
            int frozen = this.frozen;
            if (frozen == UNFROZEN) {
                return;
            }
            if (UNSAFE.compareAndSwapInt(this, FROZEN_OFFSET, frozen, UNFROZEN)) {
                return;
            }
        }
    }

    private SequencerDefinition checkSequencerDefinitions(SequencerDefinition definition, SequencerDefinition other) {
        if (other != null && !other.equals(definition)) {
            String message = ExceptionMessages.SEQUENCER_ALREADY_REGISTERED.buildMessage();
            throw new SnowcastSequencerAlreadyRegisteredException(message);
        }
        return other != null ? other : definition;
    }

    public void assignLogicalNode(SequencerDefinition definition, int logicalNodeId, Address address) {
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
        logicalNodeTable.assignLogicalNode(logicalNodeId, address);
    }

    public void unassignLogicalNode(SequencerDefinition definition, int logicalNodeId, Address address) {
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
        logicalNodeTable.detachLogicalNode(address, logicalNodeId);
    }
}
