/*
 * Copyright (c) 2015-2017, Christoph Engelbert (aka noctarius) and
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
import com.noctarius.snowcast.SnowcastIllegalStateException;
import com.noctarius.snowcast.SnowcastSequencerAlreadyRegisteredException;
import sun.misc.Unsafe;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    public SequencerPartition(@Nonnegative int partitionId) {
        this.partitionId = partitionId;
        this.logicalNodeTables = new ConcurrentHashMap<>();
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Nonnull
    @Nonnegative
    public Integer attachLogicalNode(@Nonnull SequencerDefinition definition, @Nonnull Address address) {
        checkPartitionFreezeStatus();

        SequencerDefinition safeDefinition = checkOrRegisterSequencerDefinition(definition);
        String sequencerName = safeDefinition.getSequencerName();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            String message = ExceptionMessages.UNREGISTERED_SEQUENCER_LOGICAL_NODE_TABLE.buildMessage(partitionId);
            throw new SnowcastIllegalStateException(message);
        }
        return logicalNodeTable.attachLogicalNode(address);
    }

    public void detachLogicalNode(@Nonnull String sequencerName, @Nonnull Address address, @Nonnegative int logicalNodeId) {
        checkPartitionFreezeStatus();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable != null) {
            logicalNodeTable.detachLogicalNode(address, logicalNodeId);
        }
    }

    public void assignLogicalNode(SequencerDefinition definition, int logicalNodeId, Address address) {
        checkPartitionFreezeStatus();

        SequencerDefinition safeDefinition = checkOrRegisterSequencerDefinition(definition);
        String sequencerName = safeDefinition.getSequencerName();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            String message = ExceptionMessages.UNREGISTERED_SEQUENCER_LOGICAL_NODE_TABLE.buildMessage(partitionId);
            throw new SnowcastIllegalStateException(message);
        }
        logicalNodeTable.assignLogicalNode(logicalNodeId, address);
    }

    public void unassignLogicalNode(@Nonnull SequencerDefinition definition, @Nonnegative int logicalNodeId,
                                    @Nonnull Address address) {

        checkPartitionFreezeStatus();

        SequencerDefinition safeDefinition = checkOrRegisterSequencerDefinition(definition);
        String sequencerName = safeDefinition.getSequencerName();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable != null) {
            logicalNodeTable.detachLogicalNode(address, logicalNodeId);
        }
    }

    @Nullable
    Address getAttachedLogicalNode(@Nonnull String sequencerName, @Nonnegative int logicalNodeId) {
        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            String message = ExceptionMessages.UNREGISTERED_SEQUENCER_LOGICAL_NODE_TABLE.buildMessage(partitionId);
            throw new SnowcastIllegalStateException(message);
        }
        return logicalNodeTable.getAttachedLogicalNode(logicalNodeId);
    }

    @Nullable
    SequencerDefinition getSequencerDefinition(@Nonnull String sequencerName) {
        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            return null;
        }
        return logicalNodeTable.getSequencerDefinition();
    }

    @Nonnull
    SequencerDefinition checkOrRegisterSequencerDefinition(@Nonnull SequencerDefinition definition) {
        String sequencerName = definition.getSequencerName();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.computeIfAbsent(sequencerName, name -> {
            checkPartitionFreezeStatus();
            return new LogicalNodeTable(partitionId, definition);
        });

        SequencerDefinition other = logicalNodeTable.getSequencerDefinition();
        return checkSequencerDefinitions(definition, other);
    }

    @Nullable
    SequencerDefinition destroySequencerDefinition(@Nonnull String sequencerName) {
        checkPartitionFreezeStatus();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.remove(sequencerName);
        return logicalNodeTable != null ? logicalNodeTable.getSequencerDefinition() : null;
    }

    void mergeLogicalNodeTable(@Nonnull LogicalNodeTable mergeable) {
        SequencerDefinition definition = mergeable.getSequencerDefinition();
        String sequencerName = definition.getSequencerName();

        LogicalNodeTable logicalNodeTable = logicalNodeTables.get(sequencerName);
        if (logicalNodeTable == null) {
            logicalNodeTables.put(sequencerName, mergeable);
            return;
        }

        // Checks the definitions and in the worst case throws an exception
        SequencerDefinition registered = logicalNodeTable.getSequencerDefinition();
        checkSequencerDefinitions(definition, registered);

        // Merge existing and replicated LogicalNodeTable if possible, if not
        // kill the replication process
        logicalNodeTable.merge(mergeable);
    }

    @Nonnull
    PartitionReplication createPartitionReplication() {
        return new PartitionReplication(partitionId, logicalNodeTables.values());
    }

    void freeze() {
        updateFreezeStatus(FROZEN);
    }

    void unfreeze() {
        updateFreezeStatus(UNFROZEN);
    }

    boolean isFrozen() {
        return frozen == FROZEN;
    }

    private void updateFreezeStatus(int newStatus) {
        while (true) {
            int frozen = this.frozen;
            if (frozen == newStatus) {
                return;
            }
            if (UNSAFE.compareAndSwapInt(this, FROZEN_OFFSET, frozen, newStatus)) {
                return;
            }
        }
    }

    private void checkPartitionFreezeStatus() {
        if (isFrozen()) {
            throw new SnowcastIllegalStateException(ExceptionMessages.PARTITION_IS_FROZEN.buildMessage());
        }
    }

    @Nonnull
    private SequencerDefinition checkSequencerDefinitions(@Nonnull SequencerDefinition definition,
                                                          @Nullable SequencerDefinition other) {

        if (other != null && !other.equals(definition)) {
            String message = ExceptionMessages.SEQUENCER_ALREADY_REGISTERED.buildMessage();
            throw new SnowcastSequencerAlreadyRegisteredException(message);
        }
        return other != null ? other : definition;
    }
}
