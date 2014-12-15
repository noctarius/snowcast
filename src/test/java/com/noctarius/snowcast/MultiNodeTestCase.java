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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class MultiNodeTestCase
        extends HazelcastTestSupport {

    private final Config config1 = SnowcastNodeConfigurator.buildSnowcastAwareConfig();
    private final Config config2 = SnowcastNodeConfigurator.buildSnowcastAwareConfig();
    private final Config config3 = SnowcastNodeConfigurator.buildSnowcastAwareConfig();

    @Test
    public void test_simple_sequencer_initialization()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config1);
        factory.newHazelcastInstance(config2);

        try {
            String sequencerName = generateKeyNotOwnedBy(hazelcastInstance);
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast, sequencerName);

            assertNotNull(sequencer);

            // Request the current state of the sequencer
            SnowcastSequenceState state = sequencer.getSequencerState();

            // Destroy the sequencer, cluster-wide operation
            snowcast.destroySequencer(sequencer);
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_sequencer_initialization_lazy_initialization()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config1);
        factory.newHazelcastInstance(config2);

        try {
            String sequencerName = generateKeyNotOwnedBy(hazelcastInstance);
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast, sequencerName);

            assertNotNull(sequencer);

            // Request the current state of the sequencer
            SnowcastSequenceState state = sequencer.getSequencerState();

            // Destroy the sequencer, cluster-wide operation
            snowcast.destroySequencer(sequencer);
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_single_id_generation()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config1);
        factory.newHazelcastInstance(config2);

        try {
            String sequencerName = generateKeyNotOwnedBy(hazelcastInstance);
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast, sequencerName);

            assertNotNull(sequencer);
            assertNotNull(sequencer.next());
        } finally {
            factory.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_id_generation_in_detached_state()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config1);
        factory.newHazelcastInstance(config2);

        try {
            String sequencerName = generateKeyNotOwnedBy(hazelcastInstance);
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast, sequencerName);

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

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config1);
        factory.newHazelcastInstance(config2);

        try {
            String sequencerName = generateKeyNotOwnedBy(hazelcastInstance);
            Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast, sequencerName);

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

    @Test
    public void test_distribute_destroy_node1()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            String sequencerName = generateKeyNotOwnedBy(hazelcastInstance1);
            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            final SnowcastSequencer sequencer1 = buildSnowcastSequencer(snowcast1, sequencerName, epoch);

            assertNotNull(sequencer1);
            assertNotNull(sequencer1.next());

            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);
            final SnowcastSequencer sequencer2 = buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            assertNotNull(sequencer2);
            assertNotNull(sequencer2.next());

            sequencer1.next();
            sequencer2.next();

            snowcast1.destroySequencer(sequencer1);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    assertEquals(SnowcastSequenceState.Destroyed, sequencer1.getSequencerState());
                    assertEquals(SnowcastSequenceState.Destroyed, sequencer2.getSequencerState());
                }
            });

        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void test_distribute_destroy_node2()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            String sequencerName = generateKeyNotOwnedBy(hazelcastInstance1);
            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            final SnowcastSequencer sequencer1 = buildSnowcastSequencer(snowcast1, sequencerName, epoch);

            assertNotNull(sequencer1);
            assertNotNull(sequencer1.next());

            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);
            final SnowcastSequencer sequencer2 = buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            assertNotNull(sequencer2);
            assertNotNull(sequencer2.next());

            assertNotEquals(sequencer1, sequencer2);

            sequencer1.next();
            sequencer2.next();

            snowcast2.destroySequencer(sequencer2);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    assertEquals(SnowcastSequenceState.Destroyed, sequencer1.getSequencerState());
                    assertEquals(SnowcastSequenceState.Destroyed, sequencer2.getSequencerState());
                }
            });

        } finally {
            factory.shutdownAll();
        }
    }
    @Test
    public void test_distribute_destroy_node3()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(3);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);
        HazelcastInstance hazelcastInstance3 = factory.newHazelcastInstance(config3);

        try {
            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            String sequencerName = generateKeyOwnedBy(hazelcastInstance2);
            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            final SnowcastSequencer sequencer1 = buildSnowcastSequencer(snowcast1, sequencerName, epoch);

            assertNotNull(sequencer1);
            assertNotNull(sequencer1.next());

            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);
            final SnowcastSequencer sequencer2 = buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            assertNotNull(sequencer2);
            assertNotNull(sequencer2.next());

            Snowcast snowcast3 = SnowcastSystem.snowcast(hazelcastInstance3);
            final SnowcastSequencer sequencer3 = buildSnowcastSequencer(snowcast3, sequencerName, epoch);

            assertNotNull(sequencer3);
            assertNotNull(sequencer3.next());

            assertNotEquals(sequencer1, sequencer2);
            assertNotEquals(sequencer1, sequencer3);
            assertNotEquals(sequencer2, sequencer3);

            sequencer1.next();
            sequencer2.next();
            sequencer3.next();

            snowcast3.destroySequencer(sequencer3);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    assertEquals(SnowcastSequenceState.Destroyed, sequencer1.getSequencerState());
                    assertEquals(SnowcastSequenceState.Destroyed, sequencer2.getSequencerState());
                    assertEquals(SnowcastSequenceState.Destroyed, sequencer3.getSequencerState());
                }
            });

        } finally {
            factory.shutdownAll();
        }
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast, String sequencerName) {
        // Build the custom epoch
        SnowcastEpoch epoch = buildEpoch();

        return buildSnowcastSequencer(snowcast, sequencerName, epoch);
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast, String sequencerName, SnowcastEpoch epoch) {
        int maxLogicalNodeCount = 128;

        // Create a sequencer for ID generation
        return snowcast.createSequencer(sequencerName, epoch, maxLogicalNodeCount);
    }

    private SnowcastEpoch buildEpoch() {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(2014, 1, 1, 0, 0, 0);
        return SnowcastEpoch.byCalendar(calendar);
    }
}
