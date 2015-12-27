package com.noctarius.snowcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.noctarius.snowcast.impl.SnowcastConstants;
import org.junit.Test;

import java.util.Calendar;
import java.util.Comparator;
import java.util.GregorianCalendar;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeShifting;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.generateSequenceId;
import static junit.framework.TestCase.assertEquals;

public class SnowcastSequenceUtilsTestCase
        extends HazelcastTestSupport {

    private static final Config CONFIG = SnowcastNodeConfigurator.buildSnowcastAwareConfig();

    @Test
    public void test_compareSequence_first_higher_by_timestamp() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(9999, 1, 1, shifting);

        assertEquals(1, SnowcastSequenceUtils.compareSequence(sequence1, sequence2));
    }

    @Test
    public void test_compareSequence_first_lower_by_timestamp() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(10001, 1, 1, shifting);

        assertEquals(-1, SnowcastSequenceUtils.compareSequence(sequence1, sequence2));
    }

    @Test
    public void test_compareSequence_first_higher_by_counter() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 2, shifting);
        long sequence2 = generateSequenceId(10000, 1, 1, shifting);

        assertEquals(1, SnowcastSequenceUtils.compareSequence(sequence1, sequence2));
    }

    @Test
    public void test_compareSequence_first_lower_by_counter() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(10000, 1, 2, shifting);

        assertEquals(-1, SnowcastSequenceUtils.compareSequence(sequence1, sequence2));
    }

    @Test
    public void test_compareSequence_equal() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(10000, 2, 1, shifting);

        assertEquals(0, SnowcastSequenceUtils.compareSequence(sequence1, sequence2));
    }

    @Test
    public void test_compareTimestamp_first_higher_by_timestamp() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(9999, 1, 1, shifting);

        assertEquals(1, SnowcastSequenceUtils.compareTimestamp(sequence1, sequence2));
    }

    @Test
    public void test_compareTimestamp_first_lower_by_timestamp() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(10001, 1, 1, shifting);

        assertEquals(-1, SnowcastSequenceUtils.compareTimestamp(sequence1, sequence2));
    }

    @Test
    public void test_compareTimestamp_equal() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(10000, 2, 1, shifting);

        assertEquals(0, SnowcastSequenceUtils.compareTimestamp(sequence1, sequence2));
    }

    @Test
    public void test_couter_value() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence = generateSequenceId(10000, 1, 100, shifting);

        assertEquals(100, SnowcastSequenceUtils.counterValue(sequence));
    }

    @Test(expected = SnowcastException.class)
    public void test_couter_value_exception_too_large() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        generateSequenceId(10000, 1, 1025, shifting);
    }

    @Test
    public void test_logical_node_id() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence = generateSequenceId(10000, 100, 1, shifting);

        assertEquals(100, SnowcastSequenceUtils.logicalNodeId(sequence));
    }

    @Test
    public void test_timestamp_value() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence = generateSequenceId(10000, 100, 1, shifting);

        assertEquals(10000, SnowcastSequenceUtils.timestampValue(sequence));
    }

    @Test
    public void test_comparator_first_higher_by_timestamp()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(CONFIG);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence1 = generateSequenceId(10000, 1, 1, shifting);
            long sequence2 = generateSequenceId(9999, 1, 1, shifting);

            Comparator<Long> comparator = SnowcastSequenceUtils.snowcastSequenceComparator(sequencer);

            assertEquals(1, comparator.compare(sequence1, sequence2));
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_comparator_first_lower_by_timestamp()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(CONFIG);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence1 = generateSequenceId(10000, 1, 1, shifting);
            long sequence2 = generateSequenceId(10001, 1, 1, shifting);

            Comparator<Long> comparator = SnowcastSequenceUtils.snowcastSequenceComparator(sequencer);

            assertEquals(-1, comparator.compare(sequence1, sequence2));
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_comparator_first_higher_by_counter()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(CONFIG);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence1 = generateSequenceId(10000, 1, 2, shifting);
            long sequence2 = generateSequenceId(10000, 1, 1, shifting);

            Comparator<Long> comparator = SnowcastSequenceUtils.snowcastSequenceComparator(sequencer);

            assertEquals(1, comparator.compare(sequence1, sequence2));
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_comparator_first_lower_by_counter()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(CONFIG);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence1 = generateSequenceId(10000, 1, 1, shifting);
            long sequence2 = generateSequenceId(10000, 1, 2, shifting);

            Comparator<Long> comparator = SnowcastSequenceUtils.snowcastSequenceComparator(sequencer);

            assertEquals(-1, comparator.compare(sequence1, sequence2));
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_comparator_equal()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(CONFIG);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence1 = generateSequenceId(10000, 1, 1, shifting);
            long sequence2 = generateSequenceId(10000, 2, 1, shifting);

            Comparator<Long> comparator = SnowcastSequenceUtils.snowcastSequenceComparator(sequencer);

            assertEquals(0, comparator.compare(sequence1, sequence2));
        } finally {
            factory.shutdownAll();
        }
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast) {
        // Build the custom epoch
        SnowcastEpoch epoch = buildEpoch();

        return buildSnowcastSequencer(snowcast, epoch);
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast, SnowcastEpoch epoch) {
        String sequencerName = "SimpleSequencer";
        int maxLogicalNodeCount = SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS;

        // Create a sequencer for ID generation
        return snowcast.createSequencer(sequencerName, epoch, maxLogicalNodeCount);
    }

    private SnowcastEpoch buildEpoch() {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(2014, 0, 1, 0, 0, 0);
        return SnowcastEpoch.byCalendar(calendar);
    }
}
