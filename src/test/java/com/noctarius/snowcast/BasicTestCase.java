/*
 * Copyright (c) 2015-2017, Christoph Engelbert (aka noctarius) and
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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import java.io.InputStream;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.*;
import static org.junit.Assert.*;

public class BasicTestCase
        extends HazelcastTestSupport {

    @Test
    public void test_simple_sequencer_initialization()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_sequencer_initialization_from_calendar()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencerFromCalendar(snowcast);

            assertNotNull(sequencer);
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_sequencer_initialization_declarative()
            throws Exception {

        ClassLoader classLoader = BasicTestCase.class.getClassLoader();
        InputStream stream = classLoader.getResourceAsStream("hazelcast-node-configuration.xml");
        Config config = new XmlConfigBuilder(stream).build();

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_single_id_generation()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);
        } finally {
            factory.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_destroyed_state()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
            assertNotNull(sequencer.next());

            snowcast.destroySequencer(sequencer);
            sequencer.next();

        } finally {
            factory.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_id_generation_in_detached_state()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
            assertNotNull(sequencer.next());

            // Detach the node and free the currently used logical node ID
            sequencer.detachLogicalNode();

            sequencer.next();
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_id_generation_in_reattached_state()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
            assertNotNull(sequencer.next());

            // Detach the node and free the currently used logical node ID
            sequencer.detachLogicalNode();

            try {
                // Must fail since we're in detached state!
                sequencer.next();
                fail();
            } catch (SnowcastStateException e) {
                // Expected, so ignore
            }

            // Re-attach the node and assign a free logical node ID
            sequencer.attachLogicalNode();

            assertNotNull(sequencer.next());
        } finally {
            factory.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_id_generation_in_attach_wrong_state()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);

            sequencer.attachLogicalNode();
        } finally {
            factory.shutdownAll();
        }
    }

    @Test(expected = SnowcastSequencerAlreadyRegisteredException.class)
    public void test_creation_wrong_definition()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);

            SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(System.currentTimeMillis());
            buildSnowcastSequencer(snowcast, epoch);
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_sequencer_counter_value()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(128);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence = generateSequenceId(10000, 10, 100, shifting);

            assertEquals(100, sequencer.counterValue(sequence));
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_sequencer_timestamp()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(128);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence = generateSequenceId(10000, 10, 100, shifting);

            assertEquals(10000, sequencer.timestampValue(sequence));
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_sequencer_logical_node_id()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(128);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence = generateSequenceId(10000, 10, 100, shifting);

            assertEquals(10, sequencer.logicalNodeId(sequence));
        } finally {
            factory.shutdownAll();
        }
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast) {
        // Build the custom epoch
        SnowcastEpoch epoch = buildEpoch();

        return buildSnowcastSequencer(snowcast, epoch);
    }

    private SnowcastSequencer buildSnowcastSequencerFromCalendar(Snowcast snowcast) {
        // Build the custom epoch
        SnowcastEpoch epoch = buildEpochFromCalendar();

        return buildSnowcastSequencer(snowcast, epoch);
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast, SnowcastEpoch epoch) {
        String sequencerName = "SimpleSequencer";
        int maxLogicalNodeCount = 128;

        // Create a sequencer for ID generation
        return snowcast.createSequencer(sequencerName, epoch, maxLogicalNodeCount);
    }

    private SnowcastEpoch buildEpochFromCalendar() {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(2014, 0, 1, 0, 0, 0);
        return SnowcastEpoch.byCalendar(calendar);
    }

    private SnowcastEpoch buildEpoch() {
        ZonedDateTime utc = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        return SnowcastEpoch.byInstant(utc.toInstant());
    }
}
