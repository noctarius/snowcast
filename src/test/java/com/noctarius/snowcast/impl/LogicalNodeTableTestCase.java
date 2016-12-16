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

import com.hazelcast.nio.Address;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastIllegalStateException;
import com.noctarius.snowcast.SnowcastNodeIdsExceededException;
import com.noctarius.snowcast.SnowcastSequenceState;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class LogicalNodeTableTestCase {

    @Test
    public void test_attach()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        Address address = new Address("localhost", 12345);

        int logicalNodeId = logicalNodeTable.attachLogicalNode(address);
        Address assigned = logicalNodeTable.getAttachedLogicalNode(logicalNodeId);

        assertEquals(address, assigned);
    }

    @Test
    public void test_attach_multiple_nodes()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        Address address1 = new Address("localhost", 12345);
        int logicalNodeId1 = logicalNodeTable.attachLogicalNode(address1);
        Address assigned1 = logicalNodeTable.getAttachedLogicalNode(logicalNodeId1);
        assertEquals(address1, assigned1);

        Address address2 = new Address("localhost", 54321);
        int logicalNodeId2 = logicalNodeTable.attachLogicalNode(address2);
        Address assigned2 = logicalNodeTable.getAttachedLogicalNode(logicalNodeId2);
        assertNotEquals(logicalNodeId1, logicalNodeId2);
        assertEquals(address2, assigned2);
    }

    @Test(expected = SnowcastNodeIdsExceededException.class)
    public void test_attach_exceed_nodes_count()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        try {
            for (int i = 0; i < definition.getBoundedMaxLogicalNodeCount(); i++) {
                Address address = new Address("localhost", 10000 + i);
                int logicalNodeId = logicalNodeTable.attachLogicalNode(address);
                Address assigned = logicalNodeTable.getAttachedLogicalNode(logicalNodeId);
                assertEquals(address, assigned);
            }
        } catch (SnowcastNodeIdsExceededException e) {
            fail("Node Ids exceeded too early");
        }

        Address address2 = new Address("localhost", 54321);
        logicalNodeTable.attachLogicalNode(address2);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_illegal_detach()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        Address address1 = new Address("localhost", 12345);
        int logicalNodeId1 = logicalNodeTable.attachLogicalNode(address1);
        Address assigned1 = logicalNodeTable.getAttachedLogicalNode(logicalNodeId1);
        assertEquals(address1, assigned1);

        Address address2 = new Address("localhost", 54321);
        logicalNodeTable.attachLogicalNode(address2);

        logicalNodeTable.detachLogicalNode(address1, 1);
    }

    @Test
    public void test_attach_concurrently()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        final LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        class Task
                implements Runnable {

            private final int index;
            private final Address address;
            private final AtomicIntegerArray assignments;
            private final CountDownLatch start;
            private final CountDownLatch end;

            Task(int index, Address address, AtomicIntegerArray assignments, CountDownLatch start, CountDownLatch end) {
                this.index = index;
                this.address = address;
                this.assignments = assignments;
                this.start = start;
                this.end = end;
            }

            @Override
            public void run() {
                try {
                    start.await();

                    int logicalNodeId = logicalNodeTable.attachLogicalNode(address);
                    assignments.set(index, logicalNodeId);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    end.countDown();
                }
            }
        }

        int concurrencyLevel = 100;

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(concurrencyLevel);
        AtomicIntegerArray assignments = new AtomicIntegerArray(concurrencyLevel);
        for (int i = 0; i < concurrencyLevel; i++) {
            Address address = new Address("localhost", 1000 + i);
            new Thread(new Task(i, address, assignments, start, end)).start();
        }

        // Kick off all threads at once
        start.countDown();

        // Wait for assignments to finish
        end.await();

        // Search for duplicates
        Set<Integer> logicalNodeIds = new HashSet<Integer>();
        for (int i = 0; i < concurrencyLevel; i++) {
            logicalNodeIds.add(assignments.get(i));
        }

        assertEquals(concurrencyLevel, logicalNodeIds.size());
    }

    @Test
    public void test_attach_detach_reattach_concurrently()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        final LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        class Task
                implements Runnable {

            private final int index;
            private final Address address;
            private final AtomicIntegerArray assignments;
            private final CountDownLatch start;
            private final CountDownLatch end;

            Task(int index, Address address, AtomicIntegerArray assignments, CountDownLatch start, CountDownLatch end) {
                this.index = index;
                this.address = address;
                this.assignments = assignments;
                this.start = start;
                this.end = end;
            }

            @Override
            public void run() {
                SnowcastSequenceState state = SnowcastSequenceState.Detached;
                try {
                    start.await();

                    int logicalNodeId = 0;
                    for (int i = 0; i < 1000; i++) {
                        if (state == SnowcastSequenceState.Detached) {
                            logicalNodeId = logicalNodeTable.attachLogicalNode(address);
                            assignments.set(index, logicalNodeId);
                            state = SnowcastSequenceState.Attached;
                        } else {
                            logicalNodeTable.detachLogicalNode(address, logicalNodeId);
                            assignments.set(index, -1);
                            state = SnowcastSequenceState.Detached;
                        }
                    }

                    if (state == SnowcastSequenceState.Detached) {
                        logicalNodeId = logicalNodeTable.attachLogicalNode(address);
                        assignments.set(index, logicalNodeId);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    end.countDown();
                }
            }
        }

        int concurrencyLevel = 100;

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(concurrencyLevel);
        AtomicIntegerArray assignments = new AtomicIntegerArray(concurrencyLevel);
        for (int i = 0; i < concurrencyLevel; i++) {
            Address address = new Address("localhost", 1000 + i);
            new Thread(new Task(i, address, assignments, start, end)).start();
        }

        // Kick off all threads at once
        start.countDown();

        // Wait for assignments to finish
        end.await();

        // Search for duplicates
        Set<Integer> logicalNodeIds = new HashSet<Integer>();
        for (int i = 0; i < concurrencyLevel; i++) {
            int logicalNodeId = assignments.get(i);
            if (logicalNodeId == -1) {
                fail("Unassigned node attach thread!");
            }
            logicalNodeIds.add(logicalNodeId);
        }

        assertEquals(concurrencyLevel, logicalNodeIds.size());
    }

    @Test
    public void test_detach_but_not_attached()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        Address address = new Address("localhost", 12345);

        logicalNodeTable.detachLogicalNode(address, 1);
    }

    @Test(expected = SnowcastIllegalStateException.class)
    public void test_attach_but_already_attached()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        Address address = new Address("localhost", 12345);

        logicalNodeTable.assignLogicalNode(1, address);
        logicalNodeTable.assignLogicalNode(1, address);
    }

    @Test(expected = SnowcastNodeIdsExceededException.class)
    public void test_exceed_logical_node_ids()
            throws Exception {

        SequencerDefinition definition = new SequencerDefinition("empty", SnowcastEpoch.byTimestamp(1), 128, (short) 1);
        LogicalNodeTable logicalNodeTable = new LogicalNodeTable(1, definition);

        Address address = new Address("localhost", 12345);

        for (int i = 0; i < 128; i++) {
            logicalNodeTable.attachLogicalNode(address);
        }
        logicalNodeTable.attachLogicalNode(address);
    }
}
