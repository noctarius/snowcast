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

import com.hazelcast.util.QuickMath;
import com.noctarius.snowcast.SnowcastMaxLogicalNodeIdOutOfBoundsException;

import static com.noctarius.snowcast.impl.SnowcastConstants.ID_TIMESTAMP_READ_MASK;
import static com.noctarius.snowcast.impl.SnowcastConstants.MAX_LOGICAL_NODE_COUNT_1024;
import static com.noctarius.snowcast.impl.SnowcastConstants.MAX_LOGICAL_NODE_COUNT_128;
import static com.noctarius.snowcast.impl.SnowcastConstants.MAX_LOGICAL_NODE_COUNT_2048;
import static com.noctarius.snowcast.impl.SnowcastConstants.MAX_LOGICAL_NODE_COUNT_256;
import static com.noctarius.snowcast.impl.SnowcastConstants.MAX_LOGICAL_NODE_COUNT_4096;
import static com.noctarius.snowcast.impl.SnowcastConstants.MAX_LOGICAL_NODE_COUNT_512;
import static com.noctarius.snowcast.impl.SnowcastConstants.MAX_LOGICAL_NODE_COUNT_8192;
import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_LOWER_BOUND;
import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_UPPER_BOUND;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_LOGICAL_NODE_ID_1024;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_LOGICAL_NODE_ID_128;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_LOGICAL_NODE_ID_2048;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_LOGICAL_NODE_ID_256;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_LOGICAL_NODE_ID_4096;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_LOGICAL_NODE_ID_512;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_LOGICAL_NODE_ID_8192;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_TIMESTAMP;

public final class InternalSequencerUtils {

    private InternalSequencerUtils() {
    }

    public static int calculateBoundedMaxLogicalNodeCount(int maxLogicalNodeCount) {
        if (maxLogicalNodeCount < NODE_ID_LOWER_BOUND) {
            String message = ExceptionMessages.ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY.buildMessage("smaller", NODE_ID_LOWER_BOUND);
            throw new SnowcastMaxLogicalNodeIdOutOfBoundsException(message);
        }
        if (maxLogicalNodeCount > NODE_ID_UPPER_BOUND) {
            String message = ExceptionMessages.ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY.buildMessage("larger", NODE_ID_UPPER_BOUND);
            throw new SnowcastMaxLogicalNodeIdOutOfBoundsException(message);
        }
        return QuickMath.nextPowerOfTwo(maxLogicalNodeCount) - 1;
    }

    public static int calculateMaxMillisCounter(int shiftLogicalNodeId) {
        return (int) Math.pow(2, shiftLogicalNodeId);
    }

    public static int calculateLogicalNodeShifting(int maxLogicalNodeCount) {
        switch (maxLogicalNodeCount) {
            case MAX_LOGICAL_NODE_COUNT_128:
                return SHIFT_LOGICAL_NODE_ID_128;
            case MAX_LOGICAL_NODE_COUNT_256:
                return SHIFT_LOGICAL_NODE_ID_256;
            case MAX_LOGICAL_NODE_COUNT_512:
                return SHIFT_LOGICAL_NODE_ID_512;
            case MAX_LOGICAL_NODE_COUNT_1024:
                return SHIFT_LOGICAL_NODE_ID_1024;
            case MAX_LOGICAL_NODE_COUNT_2048:
                return SHIFT_LOGICAL_NODE_ID_2048;
            case MAX_LOGICAL_NODE_COUNT_4096:
                return SHIFT_LOGICAL_NODE_ID_4096;
            case MAX_LOGICAL_NODE_COUNT_8192:
                return SHIFT_LOGICAL_NODE_ID_8192;
            default:
                throw new IllegalArgumentException(ExceptionMessages.ILLEGAL_MAX_LOGICAL_NODE_COUNT.buildMessage());
        }
    }

    public static long calculateLogicalNodeMask(long maxLogicalNodeCount, int nodeIdShiftFactor) {
        return (maxLogicalNodeCount) << nodeIdShiftFactor;
    }

    public static long calculateCounterMask(long maxLogicalNodeCount, int nodeIdShiftFactor) {
        long logicalNodeMask = maxLogicalNodeCount << nodeIdShiftFactor;
        long invMask = ID_TIMESTAMP_READ_MASK | logicalNodeMask;
        return ~invMask;
    }

    public static long generateSequenceId(long timestamp, int logicalNodeID, int nextId, int nodeIdShiftFactor) {
        long id = timestamp << SHIFT_TIMESTAMP;
        id |= logicalNodeID << nodeIdShiftFactor;
        id |= nextId;
        return id;
    }

    public static long timestampValue(long sequenceId) {
        return (sequenceId & ID_TIMESTAMP_READ_MASK) >>> SHIFT_TIMESTAMP;
    }

    public static int logicalNodeId(long sequenceId, int nodeIdShiftFactor, long mask) {
        return (int) ((sequenceId & mask) >>> nodeIdShiftFactor);
    }

    public static int counterValue(long sequenceId, long mask) {
        return (int) (sequenceId & mask);
    }
}
