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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.Comparator;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateCounterMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeShifting;
import static com.noctarius.snowcast.impl.SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS;

/**
 * This class contains a collection of helper methods to easy the use of snowcast. It provides
 * the user with functionality to extract timestamp-, counter-value and logicalNodeId from a
 * sequence id and to compare sequence ids without the need for boxing/unboxing as the
 * {@link com.noctarius.snowcast.SnowcastTimestampComparator} would have to.
 */
@ThreadSafe
public final class SnowcastSequenceUtils {

    private SnowcastSequenceUtils() {
    }

    /**
     * Creates a new {@link com.noctarius.snowcast.SnowcastSequenceComparator} instance based on the
     * given {@link com.noctarius.snowcast.SnowcastSequencer}'s internal configuration. This is a
     * convenience method over customly creating the comparator instance.
     *
     * @param sequencer the SnowcastSequencer to retrieve the maximum logical node count value from
     * @return a SnowcastSequenceComparator instance bound to the maximum logical node count
     */
    @Nonnull
    public static Comparator<Long> snowcastSequenceComparator(@Nonnull SnowcastSequencer sequencer) {
        return InternalSequencerUtils.snowcastSequenceComparator(sequencer);
    }

    /**
     * This helper method extracts the timestamp value from a given sequence id. This timestamp value can be
     * used to order multiple sequence ids depending on their generation time.<br>
     * To compare timestamps and order sequence ids accordingly please find {@link #compareTimestamp(long, long)} as
     * well as {@link com.noctarius.snowcast.SnowcastTimestampComparator}.
     *
     * @param sequenceId the sequence id to extract the timestamp value from
     * @return the extracted timestamp value based on the generation epoch
     */
    @Nonnegative
    public static long timestampValue(long sequenceId) {
        return InternalSequencerUtils.timestampValue(sequenceId);
    }

    /**
     * <p>This helper method extracts the logicalNodeId from a given sequence id. This logicalNodeId identifies
     * the logical node at generation time. Since assignment is automatic and there is no guarantee that a
     * node has always the same logicalNodeId users are not expected to make assumptions based on this id!.</p>
     * <p>This overload always assumes a maximal node count of 8192 nodes. For a divergent number of maximum
     * nodes at generation time of the sequenceId please use {@link #logicalNodeId(long, int)} and specify the
     * maximum node count.</p>
     *
     * @param sequenceId the sequence id to extract the logicalNodeId value from
     * @return the extracted logicalNodeId
     */
    @Nonnegative
    public static int logicalNodeId(long sequenceId) {
        return logicalNodeId(sequenceId, DEFAULT_MAX_LOGICAL_NODES_13_BITS);
    }

    /**
     * <p>This helper method extracts the logicalNodeId from a given sequence id. This logicalNodeId identifies
     * the logical node at generation time. Since assignment is automatic and there is no guarantee that a
     * node has always the same logicalNodeId users are not expected to make assumptions based on this id!.</p>
     *
     * @param sequenceId          the sequence id to extract the logicalNodeId value from
     * @param maxLogicalNodeCount the maximum node count that was specified at generation time
     * @return the extracted logicalNodeId
     * @throws SnowcastMaxLogicalNodeIdOutOfBoundsException when maxLogicalNodeCount is outside of the legal range
     */
    @Nonnegative
    public static int logicalNodeId(long sequenceId, @Min(128) @Max(8192) int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShiftFactor = calculateLogicalNodeShifting(nodeCount);
        long mask = calculateLogicalNodeMask(nodeCount, nodeIdShiftFactor);
        return InternalSequencerUtils.logicalNodeId(sequenceId, nodeIdShiftFactor, mask);
    }

    /**
     * <p>This helper method extracts the counter value from a given sequence id. This counter value identifies
     * the order and count of generated sequence ids on a node in this ids timestamp value.</p>
     * <p>This overload always assumes a maximal node count of 8192 nodes. For a divergent number of maximum
     * nodes at generation time of the sequenceId please use {@link #counterValue(long, int)} and specify the
     * maximum node count.</p>
     *
     * @param sequenceId the sequence id to extract the counter value from
     * @return the extracted logicalNodeId
     */
    @Nonnegative
    public static int counterValue(long sequenceId) {
        return counterValue(sequenceId, DEFAULT_MAX_LOGICAL_NODES_13_BITS);
    }

    /**
     * <p>This helper method extracts the counter value from a given sequence id. This counter value identifies
     * the order and count of generated sequence ids on a node in this ids timestamp value.</p>
     *
     * @param sequenceId          the sequence id to extract the logicalNodeId value from
     * @param maxLogicalNodeCount the maximum node count that was specified at generation time
     * @return the extracted logicalNodeId
     * @throws SnowcastMaxLogicalNodeIdOutOfBoundsException when maxLogicalNodeCount is outside of the legal range
     */
    @Nonnegative
    public static int counterValue(long sequenceId, @Min(128) @Max(8192) int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShiftFactor = calculateLogicalNodeShifting(nodeCount);
        long mask = calculateCounterMask(nodeCount, nodeIdShiftFactor);
        return InternalSequencerUtils.counterValue(sequenceId, mask);
    }

    /**
     * This helper method provides a comparison implementation to order or compare two distinct
     * sequence ids by their internal timestamp value. The counter value is not taken into account
     * which might be enough depending on the application. This saves some CPU cycles to extract the
     * second value.
     *
     * @param sequenceId1 the first sequence id to be compared
     * @param sequenceId2 the second sequence if to be compared
     * @return a negative integer, zero, or a positive integer as the first argument is less than,
     * equal to, or greater than the second.
     */
    public static int compareTimestamp(long sequenceId1, long sequenceId2) {
        long timestampValue1 = timestampValue(sequenceId1);
        long timestampValue2 = timestampValue(sequenceId2);
        return Long.compare(timestampValue1, timestampValue2);
    }

    /**
     * <p>This helper method provides a comparison implementation to order or compare two distinct
     * sequence ids by their internal timestamp <b>and</b> counter value.</p>
     * <p>This overload always assumes a maximal node count of 8192 nodes. For a divergent number of maximum
     * nodes at generation time of the sequenceId please use {@link #compareSequence(long, long, int)} and
     * specify the maximum node count.</p>
     *
     * @param sequenceId1 the first sequence id to be compared
     * @param sequenceId2 the second sequence if to be compared
     * @return a negative integer, zero, or a positive integer as the first argument is less than,
     * equal to, or greater than the second.
     */
    public static int compareSequence(long sequenceId1, long sequenceId2) {
        return compareSequence(sequenceId1, sequenceId2, DEFAULT_MAX_LOGICAL_NODES_13_BITS);
    }

    /**
     * This helper method provides a comparison implementation to order or compare two distinct
     * sequence ids by their internal timestamp <b>and</b> counter value.
     *
     * @param sequenceId1         the first sequence id to be compared
     * @param sequenceId2         the second sequence if to be compared
     * @param maxLogicalNodeCount the maximum node count that was specified at generation time
     * @return a negative integer, zero, or a positive integer as the first argument is less than,
     * equal to, or greater than the second.
     * @throws SnowcastMaxLogicalNodeIdOutOfBoundsException when maxLogicalNodeCount is outside of the legal range
     */
    public static int compareSequence(long sequenceId1, long sequenceId2, @Min(128) @Max(8192) int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShifting = calculateLogicalNodeShifting(nodeCount);
        long counterMask = calculateCounterMask(nodeCount, nodeIdShifting);

        long timestampValue1 = timestampValue(sequenceId1);
        long timestampValue2 = timestampValue(sequenceId2);

        int compare = Long.compare(timestampValue1, timestampValue2);
        if (compare != 0) {
            return compare;
        }

        int counterValue1 = InternalSequencerUtils.counterValue(sequenceId1, counterMask);
        int counterValue2 = InternalSequencerUtils.counterValue(sequenceId2, counterMask);
        return Integer.compare(counterValue1, counterValue2);
    }
}
