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
import com.noctarius.snowcast.SnowcastSequencerAlreadyRegisteredException;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

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
}
