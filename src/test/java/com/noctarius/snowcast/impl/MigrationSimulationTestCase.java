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

import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.partition.MigrationEndpoint;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MigrationSimulationTestCase {

    @Test
    public void test_frozen_before_migration_source()
            throws Exception {

        NodeSequencerService sequencerService = new NodeSequencerService();
        SequencerPartition partition = sequencerService.getSequencerPartition(1);

        PartitionMigrationEvent event = new PartitionMigrationEvent(MigrationEndpoint.SOURCE, 1, 2, 3);

        sequencerService.beforeMigration(event);
        assertTrue(partition.isFrozen());
    }

    @Test
    public void test_frozen_before_migration_destination()
            throws Exception {

        NodeSequencerService sequencerService = new NodeSequencerService();
        SequencerPartition partition = sequencerService.getSequencerPartition(1);

        PartitionMigrationEvent event = new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, 1, 2, 3);

        sequencerService.beforeMigration(event);
        assertTrue(partition.isFrozen());
    }

    @Test
    public void test_unfrozen_commit_migration_source()
            throws Exception {

        NodeSequencerService sequencerService = new NodeSequencerService();
        SequencerPartition partition = sequencerService.getSequencerPartition(1);

        PartitionMigrationEvent event = new PartitionMigrationEvent(MigrationEndpoint.SOURCE, 1, 2, 3);

        sequencerService.beforeMigration(event);
        assertTrue(partition.isFrozen());

        sequencerService.commitMigration(event);
        assertFalse(partition.isFrozen());
    }

    @Test
    public void test_unfrozen_commit_migration_destination()
            throws Exception {

        NodeSequencerService sequencerService = new NodeSequencerService();
        SequencerPartition partition = sequencerService.getSequencerPartition(1);

        PartitionMigrationEvent event = new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, 1, 2, 3);

        sequencerService.beforeMigration(event);
        assertTrue(partition.isFrozen());

        sequencerService.commitMigration(event);
        assertFalse(partition.isFrozen());
    }

    @Test
    public void test_unfrozen_rollback_migration_source()
            throws Exception {

        NodeSequencerService sequencerService = new NodeSequencerService();
        SequencerPartition partition = sequencerService.getSequencerPartition(1);

        PartitionMigrationEvent event = new PartitionMigrationEvent(MigrationEndpoint.SOURCE, 1, 2, 3);

        sequencerService.beforeMigration(event);
        assertTrue(partition.isFrozen());

        sequencerService.rollbackMigration(event);
        assertFalse(partition.isFrozen());
    }

    @Test
    public void test_unfrozen_rollback_migration_destination()
            throws Exception {

        NodeSequencerService sequencerService = new NodeSequencerService();
        SequencerPartition partition = sequencerService.getSequencerPartition(1);

        PartitionMigrationEvent event = new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, 1, 2, 3);

        sequencerService.beforeMigration(event);
        assertTrue(partition.isFrozen());

        sequencerService.rollbackMigration(event);
        assertFalse(partition.isFrozen());
    }
}
