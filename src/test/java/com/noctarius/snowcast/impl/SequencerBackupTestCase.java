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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.noctarius.snowcast.Snowcast;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastNodeConfigurator;
import com.noctarius.snowcast.SnowcastSequencer;
import com.noctarius.snowcast.SnowcastSystem;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;

public class SequencerBackupTestCase
        extends HazelcastTestSupport {

    private final Config config1 = SnowcastNodeConfigurator.buildSnowcastAwareConfig();
    private final Config config2 = SnowcastNodeConfigurator.buildSnowcastAwareConfig();

    @Test
    public void test_simple_backup_create_sequencer_definition_owner() {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

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
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_create_sequencer_definition_non_owner() {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

            InternalSequencer sequencer2 = (InternalSequencer) buildSnowcastSequencer(snowcast2, sequencerName, epoch);
            InternalSequencer sequencer1 = (InternalSequencer) buildSnowcastSequencer(snowcast1, sequencerName, epoch);

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
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_destroy_sequencer_definition_owner() {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

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

            snowcast1.destroySequencer(sequencer1);

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
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_destroy_sequencer_definition_non_owner() {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

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
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_attach_logical_node_id()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

            InternalSequencer sequencer1 = (InternalSequencer) buildSnowcastSequencer(snowcast1, sequencerName, epoch);
            InternalSequencer sequencer2 = (InternalSequencer) buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            NodeSequencerService sequencerService1 = (NodeSequencerService) sequencer1.getSequencerService();
            NodeSequencerService sequencerService2 = (NodeSequencerService) sequencer2.getSequencerService();

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
            factory.shutdownAll();
        }
    }

    @Test
    public void test_simple_backup_detach_logical_node_id()
            throws Exception {

        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config1);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config2);

        try {
            final String sequencerName = generateKeyOwnedBy(hazelcastInstance1);

            // Build the custom epoch
            SnowcastEpoch epoch = buildEpoch();

            Snowcast snowcast1 = SnowcastSystem.snowcast(hazelcastInstance1);
            Snowcast snowcast2 = SnowcastSystem.snowcast(hazelcastInstance2);

            InternalSequencer sequencer1 = (InternalSequencer) buildSnowcastSequencer(snowcast1, sequencerName, epoch);
            InternalSequencer sequencer2 = (InternalSequencer) buildSnowcastSequencer(snowcast2, sequencerName, epoch);

            NodeSequencerService sequencerService1 = (NodeSequencerService) sequencer1.getSequencerService();
            NodeSequencerService sequencerService2 = (NodeSequencerService) sequencer2.getSequencerService();

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
                }
            });

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
                }
            });
        } finally {
            factory.shutdownAll();
        }
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
