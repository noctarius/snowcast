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
        for (int maxLogicalNodeCount : SnowcastConstants.EXP_LOOKUP) {
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
