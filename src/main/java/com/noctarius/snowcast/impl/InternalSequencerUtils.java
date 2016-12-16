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

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.util.QuickMath;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastMaxLogicalNodeIdOutOfBoundsException;
import com.noctarius.snowcast.SnowcastSequenceComparator;
import com.noctarius.snowcast.SnowcastSequencer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.Comparator;

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

    @Nonnull
    public static Comparator<Long> snowcastSequenceComparator(@Nonnull SnowcastSequencer sequencer) {
        SequencerDefinition definition = ((InternalSequencer) sequencer).getSequencerDefinition();
        return new SnowcastSequenceComparator(definition.getMaxLogicalNodeCount());
    }

    @Nonnegative
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

    @Nonnegative
    public static int calculateMaxMillisCounter(@Nonnegative int shiftLogicalNodeId) {
        return (int) Math.pow(2, shiftLogicalNodeId);
    }

    @Nonnegative
    public static int calculateLogicalNodeShifting(@Min(128) @Max(8192) int maxLogicalNodeCount) {
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

    @Nonnegative
    public static long calculateLogicalNodeMask(@Min(128) @Max(8192) long maxLogicalNodeCount,
                                                @Nonnegative int nodeIdShiftFactor) {

        return (maxLogicalNodeCount) << nodeIdShiftFactor;
    }

    public static long calculateCounterMask(@Min(128) @Max(8192) long maxLogicalNodeCount, @Nonnegative int nodeIdShiftFactor) {
        long logicalNodeMask = maxLogicalNodeCount << nodeIdShiftFactor;
        long invMask = ID_TIMESTAMP_READ_MASK | logicalNodeMask;
        return ~invMask;
    }

    public static long generateSequenceId(@Nonnegative long timestamp, @Min(128) @Max(8192) int logicalNodeID,
                                          @Nonnegative int nextId, @Nonnegative int nodeIdShiftFactor) {

        int maxCounter = calculateMaxMillisCounter(nodeIdShiftFactor);
        if (maxCounter < nextId) {
            throw new SnowcastException("Given nextId is greater than allowed max counter value");
        }

        long id = timestamp << SHIFT_TIMESTAMP;
        id |= logicalNodeID << nodeIdShiftFactor;
        id |= nextId;
        return id;
    }

    @Nonnegative
    public static long timestampValue(long sequenceId) {
        return (sequenceId & ID_TIMESTAMP_READ_MASK) >>> SHIFT_TIMESTAMP;
    }

    @Nonnegative
    public static int logicalNodeId(long sequenceId, @Nonnegative int nodeIdShiftFactor, long mask) {
        return (int) ((sequenceId & mask) >>> nodeIdShiftFactor);
    }

    @Nonnegative
    public static int counterValue(long sequenceId, long mask) {
        return (int) (sequenceId & mask);
    }

    static void printStartupMessage(boolean client) {
        StringBuilder sb = new StringBuilder();
        if (!SnowcastConstants.LOGO_DISABLED) {
            sb.append(SnowcastConstants.SNOWCAST_ASCII_LOGO).append('\n');
        }
        sb.append("snowcast ").append(client ? "client" : "member").append(" mode - ");
        sb.append(" version: ").append(SnowcastConstants.VERSION).append("    ");
        sb.append("build-date: ").append(SnowcastConstants.BUILD_DATE).append('\n');
        System.out.println(sb.toString());
    }

    static SnowcastConstants.HazelcastVersion getHazelcastVersion() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        if (buildInfo.getVersion() == null) {
            return SnowcastConstants.HazelcastVersion.Unknown;
        }

        if (buildInfo.getVersion().startsWith("3.7")) {
            return SnowcastConstants.HazelcastVersion.V_3_7;
        } else if (buildInfo.getVersion().startsWith("3.8")) {
            return SnowcastConstants.HazelcastVersion.V_3_8;
        }
        return SnowcastConstants.HazelcastVersion.Unknown;
    }
}
