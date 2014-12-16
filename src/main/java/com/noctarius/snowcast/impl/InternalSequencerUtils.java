package com.noctarius.snowcast.impl;

import com.hazelcast.util.QuickMath;
import com.noctarius.snowcast.SnowcastMaxLogicalNodeIdOutOfBoundsException;

import static com.noctarius.snowcast.impl.SnowcastConstants.BASE_SHIFT_LOGICAL_NODE_ID;
import static com.noctarius.snowcast.impl.SnowcastConstants.EXP_LOOKUP;
import static com.noctarius.snowcast.impl.SnowcastConstants.ID_TIMESTAMP_READ_MASK;
import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_LOWER_BOUND;
import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_UPPER_BOUND;
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
        int exp = BASE_SHIFT_LOGICAL_NODE_ID;
        for (int matcher : EXP_LOOKUP) {
            if (matcher == maxLogicalNodeCount + 1) {
                break;
            }
            exp++;
        }
        return exp;
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
