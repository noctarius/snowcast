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

/**
 * <p>This {@link java.util.Comparator} implementation can be used to compare and order two distinct
 * snowcast sequencer ids by their corresponding timestamps and counter values.<br/>
 * This is meant for legacy code or to integrate it with existing frameworks. In general use cases
 * {@link com.noctarius.snowcast.SnowcastSequenceUtils#compareSequence(long, long)} or
 * {@link com.noctarius.snowcast.SnowcastSequenceUtils#compareSequence(long, long, int)}
 * should be preferred to prevent some unnecessary boxing/unboxing from <tt>long</tt> to <tt>Long</tt>
 * and back.</p>
 * <p>This comparator can be used just as any other Java {@link java.util.Comparator}:
 * <pre>
 *     List<Long> elements = getSequencerIds();
 *     Collections.sort( elements, new SnowcastSequenceComparator( 128 ) );
 *     System.out.println( elements );
 * </pre>
 * Alternatively an instance can be retrieved using a convenience method and a already existing
 * {@link com.noctarius.snowcast.SnowcastSequencer} instance:
 * <pre>
 *     SnowcastSequencer sequencer = getSnowcastSequencer();
 *     Comparator<Long> comparator = SnowcastSequenceUtils.snowcastSequenceComparator( sequencer );
 * </pre>
 * </p>
 */
@ThreadSafe
public final class SnowcastSequenceComparator
        implements Comparator<Long> {

    private final long counterMask;

    /**
     * This constructor creates a new SnowcastSequenceComparator instance bound to the given
     * maximal logical node count.<br/>
     * There is also a convenience method in {@link com.noctarius.snowcast.SnowcastSequenceUtils}
     * to easy the creation process when a {@link com.noctarius.snowcast.SnowcastSequencer} is
     * available:
     * <pre>
     *     SnowcastSequencer sequencer = getSnowcastSequencer();
     *     Comparator<Long> comparator = SnowcastSequenceUtils.snowcastSequenceComparator( sequencer );
     * </pre>
     *
     * @param maxLogicalNodeCount the maximal logical node count
     */
    public SnowcastSequenceComparator(@Min(128) @Max(8192) int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShifting = calculateLogicalNodeShifting(nodeCount);
        this.counterMask = calculateCounterMask(maxLogicalNodeCount, nodeIdShifting);
    }

    /**
     * {@inheritDoc}
     */
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
