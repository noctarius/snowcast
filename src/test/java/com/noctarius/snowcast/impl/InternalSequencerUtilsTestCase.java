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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InternalSequencerUtilsTestCase {

    @Test
    public void test_sequence_extraction_timestamp() {
        long timestamp = InternalSequencerUtils.timestampValue(SnowcastConstants.ID_TIMESTAMP_READ_MASK);
        assertEquals((long) Math.pow(2, 41) - 1, timestamp);
    }

    @Test
    public void test_sequence_extraction_logical_node_id() {
        for (int nodeId = 128; nodeId < 8192; nodeId++) {
            int nodeCount = InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount(nodeId);

            int shift = InternalSequencerUtils.calculateLogicalNodeShifting(nodeCount);
            long mask = InternalSequencerUtils.calculateLogicalNodeMask(nodeCount, shift);

            long sequenceId = InternalSequencerUtils.generateSequenceId(0, nodeId - 1, 0, shift);

            int logicalNodeId = InternalSequencerUtils.logicalNodeId(sequenceId, shift, mask);
            assertEquals(nodeId - 1, logicalNodeId);
        }
    }

    @Test
    public void test_sequence_extraction_counter() {
        final int[] expLookup = {8192, 4096, 2048, 1024, 512, 256, 128};
        for (int maxLogicalNodeCount : expLookup) {
            int nodeCount = InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);

            int shift = InternalSequencerUtils.calculateLogicalNodeShifting(nodeCount);
            long mask = InternalSequencerUtils.calculateCounterMask(nodeCount, shift);

            int maxCounter = InternalSequencerUtils.calculateMaxMillisCounter(shift);

            for (int o = 0; o < maxCounter; o++) {
                long sequenceId = InternalSequencerUtils.generateSequenceId(0, nodeCount, o, shift);

                int counter = InternalSequencerUtils.counterValue(sequenceId, mask);
                assertEquals(o, counter);
            }
        }
    }
}
