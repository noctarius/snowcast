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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.*;
import static org.junit.Assert.*;

public class ClientBasicTestCase
        extends HazelcastTestSupport {

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        GroupProperty.PHONE_HOME_ENABLED.setSystemProperty("false");
    }

    private final Config config = new XmlConfigBuilder().build();
    private final ClientConfig clientConfig = new XmlClientConfigBuilder().build();

    public ClientBasicTestCase() {
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.getNetworkConfig().getInterfaces().setEnabled(true);
        config.getNetworkConfig().getInterfaces().setInterfaces(Arrays.asList("127.0.0.1"));
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
    }

    @Test
    public void test_simple_sequencer_initialization()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_single_id_generation()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
            assertNotNull(sequencer.next());
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_destroyed_state()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
            assertNotNull(sequencer.next());

            snowcast.destroySequencer(sequencer);

            long deadline = System.currentTimeMillis() + 60000;
            while (true) {
                // Will eventually fail
                sequencer.next();

                if (System.currentTimeMillis() >= deadline) {
                    // Safe exit after another minute of waiting for the event
                    return;
                }
            }

        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_destroyed_state_from_node()
            throws Exception {

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            SnowcastEpoch epoch = buildEpoch();

            Snowcast snowcastClient = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencerClient = buildSnowcastSequencer(snowcastClient, epoch);

            assertNotNull(sequencerClient);
            assertNotNull(sequencerClient.next());

            Snowcast snowcastNode = SnowcastSystem.snowcast(hazelcastInstance);
            SnowcastSequencer sequencerNode = buildSnowcastSequencer(snowcastNode, epoch);

            assertNotNull(sequencerNode);
            assertNotNull(sequencerNode.next());

            snowcastNode.destroySequencer(sequencerNode);

            long deadline = System.currentTimeMillis() + 60000;
            while (true) {
                // Will eventually fail
                sequencerClient.next();

                if (System.currentTimeMillis() >= deadline) {
                    // Safe exit after another minute of waiting for the event
                    return;
                }
            }

        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_id_generation_in_detached_state()
            throws Exception {

        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);
            assertNotNull(sequencer.next());

            // Detach the node and free the currently used logical node ID
            sequencer.detachLogicalNode();

            sequencer.next();
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_id_generation_in_reattached_state()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
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
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test(expected = SnowcastStateException.class)
    public void test_id_generation_in_attach_wrong_state()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            assertNotNull(sequencer);

            sequencer.attachLogicalNode();
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_sequencer_counter_value()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(128);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence = generateSequenceId(10000, 10, 100, shifting);

            assertEquals(100, sequencer.counterValue(sequence));
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_sequencer_timestamp()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(128);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence = generateSequenceId(10000, 10, 100, shifting);

            assertEquals(10000, sequencer.timestampValue(sequence));
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_sequencer_logical_node_id()
            throws Exception {

        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            Snowcast snowcast = SnowcastSystem.snowcast(client);
            SnowcastSequencer sequencer = buildSnowcastSequencer(snowcast);

            int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(128);
            int shifting = calculateLogicalNodeShifting(boundedNodeCount);
            long sequence = generateSequenceId(10000, 10, 100, shifting);

            assertEquals(10, sequencer.logicalNodeId(sequence));
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast) {
        // Build the custom epoch
        SnowcastEpoch epoch = buildEpoch();

        return buildSnowcastSequencer(snowcast, epoch);
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast, SnowcastEpoch epoch) {
        String sequencerName = "SimpleSequencer";
        int maxLogicalNodeCount = 128;

        // Create a sequencer for ID generation
        return snowcast.createSequencer(sequencerName, epoch, maxLogicalNodeCount);
    }

    private SnowcastEpoch buildEpoch() {
        ZonedDateTime utc = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        return SnowcastEpoch.byInstant(utc.toInstant());
    }
}
