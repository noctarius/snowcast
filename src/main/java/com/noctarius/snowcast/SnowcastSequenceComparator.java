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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.Comparator;

import static com.noctarius.snowcast.SnowcastSequenceUtils.timestampValue;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateCounterMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeShifting;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.counterValue;

@ThreadSafe
public final class SnowcastSequenceComparator
        implements Comparator<Long> {

    private final long counterMask;

    public SnowcastSequenceComparator(@Min(128) @Max(8192) int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShifting = calculateLogicalNodeShifting(nodeCount);
        this.counterMask = calculateCounterMask(maxLogicalNodeCount, nodeIdShifting);
    }

    @Override
    public int compare(@Nonnull Long sequenceId1, @Nonnull Long sequenceId2) {
        long timestampValue1 = timestampValue(sequenceId1);
        long timestampValue2 = timestampValue(sequenceId2);

        if (timestampValue1 < timestampValue2) {
            return -1;
        } else if (timestampValue1 > timestampValue2) {
            return 1;
        }

        int counterValue1 = counterValue(sequenceId1, counterMask);
        int counterValue2 = counterValue(sequenceId2, counterMask);

        return counterValue1 < counterValue2 ? -1 : counterValue1 == counterValue2 ? 0 : 1;
    }
}
