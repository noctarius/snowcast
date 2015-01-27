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
package com.noctarius.snowcast;

import com.noctarius.snowcast.impl.InternalSequencerUtils;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateCounterMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeShifting;
import static com.noctarius.snowcast.impl.SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS;

public final class SnowcastSequenceUtils {

    private SnowcastSequenceUtils() {
    }

    public static long timestampValue(long sequenceId) {
        return InternalSequencerUtils.timestampValue(sequenceId);
    }

    public static int logicalNodeId(long sequenceId) {
        return logicalNodeId(sequenceId, DEFAULT_MAX_LOGICAL_NODES_13_BITS);
    }

    public static int logicalNodeId(long sequenceId, int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShiftFactor = calculateLogicalNodeShifting(nodeCount);
        long mask = calculateLogicalNodeMask(maxLogicalNodeCount, nodeIdShiftFactor);
        return InternalSequencerUtils.logicalNodeId(sequenceId, nodeIdShiftFactor, mask);
    }

    public static int counterValue(long sequenceId) {
        return counterValue(sequenceId, DEFAULT_MAX_LOGICAL_NODES_13_BITS);
    }

    public static int counterValue(long sequenceId, int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShiftFactor = calculateLogicalNodeShifting(nodeCount);
        long mask = calculateCounterMask(maxLogicalNodeCount, nodeIdShiftFactor);
        return InternalSequencerUtils.counterValue(sequenceId, mask);
    }
}
