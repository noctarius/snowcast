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
package com.noctarius.snowcast.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.noctarius.snowcast.Snowcast;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastSequencer;
import com.noctarius.snowcast.SnowcastSystem;
import org.junit.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ClientSequencerBackupTestCase
        extends HazelcastTestSupport {

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        GroupProperty.PHONE_HOME_ENABLED.setSystemProperty("false");
    }

    private final Config config1 = new XmlConfigBuilder().build();
    private final Config config2 = new XmlConfigBuilder().build();
    private final ClientConfig clientConfig = new XmlClientConfigBuilder().build();

    public ClientSequencerBackupTestCase() {
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config1.getNetworkConfig().getInterfaces().setEnabled(true);
        config1.getNetworkConfig().getInterfaces().setInterfaces(Arrays.asList("127.0.0.1"));

        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config2.getNetworkConfig().getInterfaces().setEnabled(true);
        config2.getNetworkConfig().getInterfaces().setInterfaces(Arrays.asList("127.0.0.1"));

        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
    }

    @Test
    public void test_simple_backup_create_sequencer_definition_client() {
        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config2);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast clientSnowcast = SnowcastSystem.snowcast(client);
            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

            buildSnowcastSequencer(clientSnowcast, sequencerName, epoch);

            InternalSequencer sequencer1 = (InternalSequencer) buildSnowcastSequencer(snowcast1, sequencerName, epoch);
            InternalSequencer sequencer2 = (InternalSequencer) buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            NodeSequencerService sequencerService1 = (NodeSequencerService) sequencer1.getSequencerService();
            NodeSequencerService sequencerService2 = (NodeSequencerService) sequencer2.getSequencerService();

            PartitionService partitionService = hazelcastInstance1.getPartitionService();
            int partitionId = partitionService.getPartition(sequencerName).getPartitionId();

            final SequencerPartition partition1 = sequencerService1.getSequencerPartition(partitionId);
            final SequencerPartition partition2 = sequencerService2.getSequencerPartition(partitionId);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    SequencerDefinition sequencerDefinition1 = partition1.getSequencerDefinition(sequencerName);
                    SequencerDefinition sequencerDefinition2 = partition2.getSequencerDefinition(sequencerName);

                    assertEquals(sequencerDefinition1, sequencerDefinition2);
                }
            });
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_destroy_sequencer_definition_client() {
        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config2);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast clientSnowcast = SnowcastSystem.snowcast(client);
            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

            SnowcastSequencer clientSequencer = buildSnowcastSequencer(clientSnowcast, sequencerName, epoch);

            InternalSequencer sequencer1 = (InternalSequencer) buildSnowcastSequencer(snowcast1, sequencerName, epoch);
            InternalSequencer sequencer2 = (InternalSequencer) buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            NodeSequencerService sequencerService1 = (NodeSequencerService) sequencer1.getSequencerService();
            NodeSequencerService sequencerService2 = (NodeSequencerService) sequencer2.getSequencerService();

            PartitionService partitionService = hazelcastInstance1.getPartitionService();
            int partitionId = partitionService.getPartition(sequencerName).getPartitionId();

            final SequencerPartition partition1 = sequencerService1.getSequencerPartition(partitionId);
            final SequencerPartition partition2 = sequencerService2.getSequencerPartition(partitionId);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    SequencerDefinition sequencerDefinition1 = partition1.getSequencerDefinition(sequencerName);
                    SequencerDefinition sequencerDefinition2 = partition2.getSequencerDefinition(sequencerName);

                    assertEquals(sequencerDefinition1, sequencerDefinition2);
                }
            });

            clientSnowcast.destroySequencer(clientSequencer);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    SequencerDefinition sequencerDefinition1 = partition1.getSequencerDefinition(sequencerName);
                    SequencerDefinition sequencerDefinition2 = partition2.getSequencerDefinition(sequencerName);

                    assertEquals(null, sequencerDefinition1);
                    assertEquals(null, sequencerDefinition2);
                }
            });
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_attach_logical_node_id_client()
            throws Exception {

        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config2);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast clientSnowcast = SnowcastSystem.snowcast(client);
            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

            InternalSequencer clientSequencer = (InternalSequencer) buildSnowcastSequencer(clientSnowcast, sequencerName, epoch);

            InternalSequencer sequencer1 = (InternalSequencer) buildSnowcastSequencer(snowcast1, sequencerName, epoch);
            InternalSequencer sequencer2 = (InternalSequencer) buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            NodeSequencerService sequencerService1 = (NodeSequencerService) sequencer1.getSequencerService();
            NodeSequencerService sequencerService2 = (NodeSequencerService) sequencer2.getSequencerService();

            final int clientLogicalNodeId = clientSequencer.logicalNodeId(clientSequencer.next());
            final int logicalNodeId1 = sequencer1.logicalNodeId(sequencer1.next());
            final int logicalNodeId2 = sequencer2.logicalNodeId(sequencer2.next());

            PartitionService partitionService = hazelcastInstance1.getPartitionService();
            int partitionId = partitionService.getPartition(sequencerName).getPartitionId();

            final SequencerPartition partition1 = sequencerService1.getSequencerPartition(partitionId);
            final SequencerPartition partition2 = sequencerService2.getSequencerPartition(partitionId);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    SequencerDefinition sequencerDefinition1 = partition1.getSequencerDefinition(sequencerName);
                    SequencerDefinition sequencerDefinition2 = partition2.getSequencerDefinition(sequencerName);
                    assertEquals(sequencerDefinition1, sequencerDefinition2);

                    Address address1_1 = partition1.getAttachedLogicalNode(sequencerName, logicalNodeId1);
                    Address address1_2 = partition2.getAttachedLogicalNode(sequencerName, logicalNodeId1);
                    assertEquals(address1_1, address1_2);

                    Address address2_1 = partition1.getAttachedLogicalNode(sequencerName, logicalNodeId2);
                    Address address2_2 = partition2.getAttachedLogicalNode(sequencerName, logicalNodeId2);
                    assertEquals(address2_1, address2_2);

                    Address address3_1 = partition1.getAttachedLogicalNode(sequencerName, clientLogicalNodeId);
                    Address address3_2 = partition2.getAttachedLogicalNode(sequencerName, clientLogicalNodeId);
                    assertEquals(address3_1, address3_2);
                }
            });

            snowcast2.destroySequencer(sequencer2);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    SequencerDefinition sequencerDefinition1 = partition1.getSequencerDefinition(sequencerName);
                    SequencerDefinition sequencerDefinition2 = partition2.getSequencerDefinition(sequencerName);

                    assertEquals(null, sequencerDefinition1);
                    assertEquals(null, sequencerDefinition2);
                }
            });
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_detach_logical_node_id_client()
            throws Exception {

        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config2);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast clientSnowcast = SnowcastSystem.snowcast(client);
            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

            InternalSequencer clientSequencer = (InternalSequencer) buildSnowcastSequencer(clientSnowcast, sequencerName, epoch);

            InternalSequencer sequencer1 = (InternalSequencer) buildSnowcastSequencer(snowcast1, sequencerName, epoch);
            InternalSequencer sequencer2 = (InternalSequencer) buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            NodeSequencerService sequencerService1 = (NodeSequencerService) sequencer1.getSequencerService();
            NodeSequencerService sequencerService2 = (NodeSequencerService) sequencer2.getSequencerService();

            final int clientLogicalNodeId = clientSequencer.logicalNodeId(clientSequencer.next());
            final int logicalNodeId1 = sequencer1.logicalNodeId(sequencer1.next());
            final int logicalNodeId2 = sequencer2.logicalNodeId(sequencer2.next());

            PartitionService partitionService = hazelcastInstance1.getPartitionService();
            int partitionId = partitionService.getPartition(sequencerName).getPartitionId();

            final SequencerPartition partition1 = sequencerService1.getSequencerPartition(partitionId);
            final SequencerPartition partition2 = sequencerService2.getSequencerPartition(partitionId);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    SequencerDefinition sequencerDefinition1 = partition1.getSequencerDefinition(sequencerName);
                    SequencerDefinition sequencerDefinition2 = partition2.getSequencerDefinition(sequencerName);
                    assertEquals(sequencerDefinition1, sequencerDefinition2);

                    Address address1_1 = partition1.getAttachedLogicalNode(sequencerName, logicalNodeId1);
                    Address address1_2 = partition2.getAttachedLogicalNode(sequencerName, logicalNodeId1);
                    assertEquals(address1_1, address1_2);

                    Address address2_1 = partition1.getAttachedLogicalNode(sequencerName, logicalNodeId2);
                    Address address2_2 = partition2.getAttachedLogicalNode(sequencerName, logicalNodeId2);
                    assertEquals(address2_1, address2_2);

                    Address address3_1 = partition1.getAttachedLogicalNode(sequencerName, clientLogicalNodeId);
                    Address address3_2 = partition2.getAttachedLogicalNode(sequencerName, clientLogicalNodeId);
                    assertEquals(address3_1, address3_2);
                }
            });

            clientSequencer.detachLogicalNode();
            sequencer1.detachLogicalNode();
            sequencer2.detachLogicalNode();

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    SequencerDefinition sequencerDefinition1 = partition1.getSequencerDefinition(sequencerName);
                    SequencerDefinition sequencerDefinition2 = partition2.getSequencerDefinition(sequencerName);
                    assertEquals(sequencerDefinition1, sequencerDefinition2);

                    Address address1_1 = partition1.getAttachedLogicalNode(sequencerName, logicalNodeId1);
                    Address address1_2 = partition2.getAttachedLogicalNode(sequencerName, logicalNodeId1);
                    assertEquals(null, address1_1);
                    assertEquals(null, address1_2);

                    Address address2_1 = partition1.getAttachedLogicalNode(sequencerName, logicalNodeId2);
                    Address address2_2 = partition2.getAttachedLogicalNode(sequencerName, logicalNodeId2);
                    assertEquals(null, address2_1);
                    assertEquals(null, address2_2);

                    Address address3_1 = partition1.getAttachedLogicalNode(sequencerName, clientLogicalNodeId);
                    Address address3_2 = partition2.getAttachedLogicalNode(sequencerName, clientLogicalNodeId);
                    assertEquals(null, address3_1);
                    assertEquals(null, address3_2);
                }
            });
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    private SnowcastSequencer buildSnowcastSequencer(Snowcast snowcast, String sequencerName, SnowcastEpoch epoch) {
        int maxLogicalNodeCount = 128;

        // Create a sequencer for ID generation
        return snowcast.createSequencer(sequencerName, epoch, maxLogicalNodeCount);
    }

    private SnowcastEpoch buildEpoch() {
        ZonedDateTime utc = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        return SnowcastEpoch.byInstant(utc.toInstant());
    }
}
