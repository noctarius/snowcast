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

import com.hazelcast.nio.Address;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastIllegalStateException;
import com.noctarius.snowcast.SnowcastSequencerAlreadyRegisteredException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SequencePartitionTestCase {

    @Test
    public void test_double_registration()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.checkOrRegisterSequencerDefinition(definition);
    }

    @Test(expected = SnowcastSequencerAlreadyRegisteredException.class)
    public void test_double_registration_illegal()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        SequencerDefinition otherDefinition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 1000, (short) 1);
        partition.checkOrRegisterSequencerDefinition(otherDefinition);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_freeze_partition()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1002);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address1);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.attachLogicalNode(definition, address2);
    }

    @Test
    public void test_freeze_partition_double()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1002);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address1);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.freeze();
    }

    @Test
    public void test_unfreeze_partition()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1002);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address1);
        assertNotNull(logicalNodeId);

        partition.freeze();

        try {
            partition.attachLogicalNode(definition, address2);
            fail("Partition is not successfully frozen");
        } catch (SnowcastIllegalStateException e) {
            // expected, therefore ignore
        }

        partition.unfreeze();
        partition.attachLogicalNode(definition, address2);
    }

    @Test
    public void test_unfreeze_partition_double()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1002);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address1);
        assertNotNull(logicalNodeId);

        partition.freeze();

        try {
            partition.attachLogicalNode(definition, address2);
            fail("Partition is not successfully frozen");
        } catch (SnowcastIllegalStateException e) {
            // expected, therefore ignore
        }

        partition.unfreeze();
        partition.unfreeze();
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_partition_frozen_detach()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.detachLogicalNode(definition.getSequencerName(), address, logicalNodeId);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_partition_frozen_assign()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.assignLogicalNode(definition, logicalNodeId, address);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_partition_frozen_unassign()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.unassignLogicalNode(definition, logicalNodeId, address);
    }

    @Test
    public void test_partition_frozen_get_assignment()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        Address assigned = partition.getAttachedLogicalNode(definition.getSequencerName(), logicalNodeId);
        assertEquals(address, assigned);
    }

    @Test
    public void test_partition_frozen_get_definition()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        SequencerDefinition sequencerDefinition = partition.getSequencerDefinition(definition.getSequencerName());
        assertEquals(definition, sequencerDefinition);
    }

    @Test
    public void test_partition_frozen_register_definition_existing()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.checkOrRegisterSequencerDefinition(definition);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_partition_frozen_register_definition_new()
            throws Exception {

        SequencerDefinition definition1 = new SequencerDefinition("empty1", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        SequencerDefinition definition2 = new SequencerDefinition("empty2", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition1, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.checkOrRegisterSequencerDefinition(definition2);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_partition_frozen_destroy_definition()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address = new Address("localhost", 1000);

        SequencerPartition partition = new SequencerPartition(1);
        Integer logicalNodeId = partition.attachLogicalNode(definition, address);
        assertNotNull(logicalNodeId);

        partition.freeze();
        partition.destroySequencerDefinition(definition.getSequencerName());
    }

    @Test
    public void test_partition_merge_successful()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1001);

        SequencerPartition partition1 = new SequencerPartition(1);
        partition1.assignLogicalNode(definition, 0, address1);

        SequencerPartition partition2 = new SequencerPartition(1);
        partition2.assignLogicalNode(definition, 1, address2);

        PartitionReplication replication = partition1.createPartitionReplication();
        replication.applyReplication(partition2);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_partition_merge_unsuccessful_double_logical_node_id()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1001);

        SequencerPartition partition1 = new SequencerPartition(1);
        partition1.assignLogicalNode(definition, 0, address1);

        SequencerPartition partition2 = new SequencerPartition(1);
        partition2.assignLogicalNode(definition, 0, address2);

        PartitionReplication replication = partition1.createPartitionReplication();
        replication.applyReplication(partition2);
    }

    @Test
    public void test_partition_merge_unsuccessful_different_definitions_different_name()
            throws Exception {

        SequencerDefinition definition1 = new SequencerDefinition("empty1", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        SequencerDefinition definition2 = new SequencerDefinition("empty2", SnowcastEpoch.byTimestamp(1), 128, (short) 1);

        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1000);

        SequencerPartition partition1 = new SequencerPartition(1);
        partition1.assignLogicalNode(definition1, 0, address1);

        SequencerPartition partition2 = new SequencerPartition(1);
        partition2.assignLogicalNode(definition2, 0, address2);

        PartitionReplication replication = partition1.createPartitionReplication();
        replication.applyReplication(partition2);
    }

    @Test(expected = SnowcastSequencerAlreadyRegisteredException.class)
    public void test_partition_merge_unsuccessful_different_definitions_same_name()
            throws Exception {

        SequencerDefinition definition1 = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        SequencerDefinition definition2 = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(2), 128, (short) 1);

        Address address1 = new Address("localhost", 1000);
        Address address2 = new Address("localhost", 1000);

        SequencerPartition partition1 = new SequencerPartition(1);
        partition1.assignLogicalNode(definition1, 0, address1);

        SequencerPartition partition2 = new SequencerPartition(1);
        partition2.assignLogicalNode(definition2, 0, address2);

        PartitionReplication replication = partition1.createPartitionReplication();
        replication.applyReplication(partition2);
    }
}
