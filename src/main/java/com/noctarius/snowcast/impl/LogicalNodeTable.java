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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.UnsafeHelper;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastIllegalStateException;
import com.noctarius.snowcast.SnowcastNodeIdsExceededException;
import sun.misc.Unsafe;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.io.IOException;

class LogicalNodeTable {

    private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    private static final int ARRAY_BASE_OFFSET;
    private static final int ARRAY_INDEX_SCALE;
    private static final int ARRAY_INDEX_SHIFT;

    static {
        try {
            ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(Object[].class);
            ARRAY_INDEX_SCALE = UNSAFE.arrayIndexScale(Object[].class);

            if ((ARRAY_INDEX_SCALE & (ARRAY_INDEX_SCALE - 1)) != 0) {
                throw new SnowcastException(ExceptionMessages.DATA_NOT_POWER_OF_TWO.buildMessage());
            }
            ARRAY_INDEX_SHIFT = 31 - Integer.numberOfLeadingZeros(ARRAY_INDEX_SCALE);
        } catch (Exception e) {
            throw new SnowcastException(e);
        }
    }

    private final int partitionId;
    private final SequencerDefinition definition;

    private volatile Object[] assignmentTable;

    LogicalNodeTable(@Nonnegative int partitionId, @Nonnull SequencerDefinition definition) {
        this(partitionId, definition, new Object[definition.getBoundedMaxLogicalNodeCount()]);
    }

    private LogicalNodeTable(@Nonnegative int partitionId, @Nonnull SequencerDefinition definition,
                             @Nonnull Object[] assignmentTable) {

        this.partitionId = partitionId;
        this.definition = definition;
        this.assignmentTable = assignmentTable;
    }

    @Nonnull
    SequencerDefinition getSequencerDefinition() {
        return definition;
    }

    @Nonnull
    String getSequencerName() {
        return definition.getSequencerName();
    }

    @Nullable
    Address getAttachedLogicalNode(@Min(128) @Max(8192) int logicalNodeId) {
        long offset = offset(logicalNodeId);
        return (Address) UNSAFE.getObjectVolatile(assignmentTable, offset);
    }

    @Min(128)
    @Max(8192)
    int attachLogicalNode(@Nonnull Address address) {
        while (true) {
            Object[] assignmentTable = this.assignmentTable;
            int freeSlot = findFreeSlot(assignmentTable);
            if (freeSlot == -1) {
                throw new SnowcastNodeIdsExceededException();
            }
            long offset = offset(freeSlot);
            if (UNSAFE.compareAndSwapObject(assignmentTable, offset, null, address)) {
                return freeSlot;
            }
        }
    }

    void detachLogicalNode(@Nonnull Address address, @Min(128) @Max(8192) int logicalNodeId) {
        while (true) {
            Object[] assignmentTable = this.assignmentTable;
            Address addressOnSlot = (Address) assignmentTable[logicalNodeId];
            if (addressOnSlot == null) {
                break;
            }

            if (!address.equals(addressOnSlot)) {
                String message = ExceptionMessages.ILLEGAL_DETACH_ATTEMPT.buildMessage();
                throw new SnowcastIllegalStateException(message);
            }

            long offset = offset(logicalNodeId);
            if (UNSAFE.compareAndSwapObject(assignmentTable, offset, addressOnSlot, null)) {
                break;
            }
        }
    }

    void assignLogicalNode(@Min(128) @Max(8192) int logicalNodeId, @Nonnull Address address) {
        while (true) {
            Object[] assignmentTable = this.assignmentTable;
            if (assignmentTable[logicalNodeId] != null) {
                String message = ExceptionMessages.BACKUP_OUT_OF_SYNC.buildMessage(partitionId);
                throw new SnowcastIllegalStateException(message);
            }

            long offset = offset(logicalNodeId);
            if (UNSAFE.compareAndSwapObject(assignmentTable, offset, null, address)) {
                break;
            }
        }
    }

    void merge(@Nonnull LogicalNodeTable mergeable) {
        Object[] mergeableAssignmentTable = mergeable.assignmentTable;
        Object[] assignmentTable = this.assignmentTable;

        for (int index = 0; index < assignmentTable.length; index++) {
            Address mergeableAddress = (Address) mergeableAssignmentTable[index];
            Address currentAddress = (Address) assignmentTable[index];
            if (currentAddress != null) {
                if (mergeableAddress != null && !currentAddress.equals(mergeableAddress)) {
                    String message = ExceptionMessages.ERROR_MERGING_LOGICAL_NODE_TABLE.buildMessage(partitionId);
                    throw new SnowcastIllegalStateException(message);
                }
            }

            long offset = offset(index);
            UNSAFE.putObjectVolatile(assignmentTable, offset, mergeableAddress);
        }
    }

    private int findFreeSlot(@Nonnull Object[] assignmentTable) {
        for (int i = 0; i < assignmentTable.length; i++) {
            if (assignmentTable[i] == null) {
                return i;
            }
        }
        return -1;
    }

    @Nonnegative
    private long offset(@Nonnegative int index) {
        return ((long) index << ARRAY_INDEX_SHIFT) + ARRAY_BASE_OFFSET;
    }

    static void writeLogicalNodeTable(@Nonnull LogicalNodeTable logicalNodeTable, @Nonnull ObjectDataOutput out)
            throws IOException {

        out.writeObject(logicalNodeTable.definition);

        Object[] assignmentTable = logicalNodeTable.assignmentTable;
        int length = assignmentTable.length;

        int size = 0;
        for (int i = 0; i < length; i++) {
            if (assignmentTable[i] != null) {
                size++;
            }
        }

        out.writeShort(size);
        for (int i = 0; i < length; i++) {
            Address address = (Address) assignmentTable[i];
            if (address != null) {
                out.writeShort(i);
                address.writeData(out);
            }
        }
    }

    @Nonnull
    static LogicalNodeTable readLogicalNodeTable(@Nonnegative int partitionId, @Nonnull ObjectDataInput in)
            throws IOException {

        SequencerDefinition definition = in.readObject();
        short size = in.readShort();

        Object[] assignmentTable = new Object[definition.getBoundedMaxLogicalNodeCount()];
        for (int i = 0; i < size; i++) {
            short index = in.readShort();
            Address address = new Address();
            address.readData(in);
            assignmentTable[index] = address;
        }
        return new LogicalNodeTable(partitionId, definition, assignmentTable);
    }
}
