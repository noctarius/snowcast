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

import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.QuickMath;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastIllegalStateException;
import com.noctarius.snowcast.SnowcastMaxLogicalNodeIdOutOfBoundsException;
import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;
import com.noctarius.snowcast.impl.operations.AttachLogicalNodeOperation;
import com.noctarius.snowcast.impl.operations.CreateSequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.DestroySequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.DetachLogicalNodeOperation;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_LOWER_BOUND;
import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_UPPER_BOUND;

public class NodeSequencerService
        implements SequencerService, ManagedService, MigrationAwareService {

    public static final String SERVICE_NAME = "noctarius::SequencerService";

    private final SequencerPartitionConstructorFunction partitionConstructor = new SequencerPartitionConstructorFunction();
    private final SequencerConstructorFunction sequencerConstructor = new SequencerConstructorFunction(this);

    private final ConcurrentMap<Integer, SequencerPartition> partitions;
    private final ConcurrentMap<String, SequencerProvision> provisions;

    private NodeEngine nodeEngine;

    public NodeSequencerService() {
        this.provisions = new ConcurrentHashMap<String, SequencerProvision>();
        this.partitions = new ConcurrentHashMap<Integer, SequencerPartition>();
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public int attachSequencer(SequencerDefinition definition) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());

        AttachLogicalNodeOperation operation = new AttachLogicalNodeOperation(definition);
        OperationService operationService = nodeEngine.getOperationService();

        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
        return (Integer) invocationBuilder.invoke().getSafely();
    }

    @Override
    public void detachSequencer(String sequencerName, int logicalNodeId) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(sequencerName);

        DetachLogicalNodeOperation operation = new DetachLogicalNodeOperation(sequencerName, logicalNodeId);
        OperationService operationService = nodeEngine.getOperationService();

        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
        invocationBuilder.invoke().getSafely();
    }

    @Override
    public SnowcastSequencer createSequencer(String sequencerName, SnowcastEpoch epoch, int maxLogicalNodeCount) {
        int boundedMaxLogicalNodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        SequencerDefinition definition = new SequencerDefinition(sequencerName, epoch, boundedMaxLogicalNodeCount);

        Operation operation = new CreateSequencerDefinitionOperation(definition);
        SequencerDefinition realDefinition = invoke(operation, sequencerName);

        if (!definition.equals(realDefinition)) {
            String message = ExceptionMessages.SEQUENCER_ALREADY_REGISTERED.buildMessage();
            throw new SnowcastIllegalStateException(message);
        }

        return getOrCreateSequencerProvision(realDefinition).getSequencer();
    }

    @Override
    public void destroySequencer(SnowcastSequencer sequencer) {
        // Remove the current provision
        SequencerProvision provision = provisions.remove(sequencer.getSequencerName());

        // Concurrent destroy
        if (provision == null) {
            return;
        }

        // Store destroyed state into the sequencer to prevent further id creation
        provision.getSequencer().stateTransition(SnowcastSequenceState.Destroyed);

        // Destroy sequencer definition in partition
        Operation operation = new DestroySequencerDefinitionOperation(provision.getSequencerName());
        invoke(operation, sequencer.getSequencerName());
    }

    @Override
    public SequencerDefinition registerSequencerDefinition(SequencerDefinition definition) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());
        SequencerPartition partition = getSequencerPartition(partitionId);
        return partition.checkOrRegisterSequencerDefinition(definition);
    }

    @Override
    public SequencerDefinition unregisterSequencerDefinition(String sequencerName) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(sequencerName);
        SequencerPartition partition = getSequencerPartition(partitionId);
        return partition.destroySequencerDefinition(sequencerName);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return null;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
    }

    public SequencerDefinition destroySequencer(String sequencerName, boolean local) {
        // Remove the current provision
        SequencerProvision provision = provisions.remove(sequencerName);

        SequencerDefinition definition = null;
        if (provision != null) {
            // Store destroyed state into the sequencer to prevent further id creation
            provision.getSequencer().stateTransition(SnowcastSequenceState.Destroyed);
            definition = provision.getDefinition();
        }

        if (local) {
            // Destroy sequencer definition on partition
            definition = unregisterSequencerDefinition(sequencerName);
        }
        return definition;
    }

    public SequencerPartition getSequencerPartition(int partitionId) {
        return ConcurrencyUtil.getOrPutIfAbsent(partitions, partitionId, partitionConstructor);
    }

    private int calculateBoundedMaxLogicalNodeCount(int maxLogicalNodeCount) {
        if (maxLogicalNodeCount < NODE_ID_LOWER_BOUND) {
            String message = ExceptionMessages.ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY.buildMessage("smaller", NODE_ID_LOWER_BOUND);
            throw new SnowcastMaxLogicalNodeIdOutOfBoundsException(message);
        }
        if (maxLogicalNodeCount > NODE_ID_UPPER_BOUND) {
            String message = ExceptionMessages.ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY.buildMessage("larger", NODE_ID_UPPER_BOUND);
            throw new SnowcastMaxLogicalNodeIdOutOfBoundsException(message);
        }
        return QuickMath.nextPowerOfTwo(maxLogicalNodeCount);
    }

    private <T> T invoke(Operation operation, String sequencerName) {
        try {
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            int partitionId = partitionService.getPartitionId(sequencerName);
            OperationService operationService = nodeEngine.getOperationService();

            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
            return (T) invocationBuilder.invoke().getSafely();
        } catch (Exception e) {
            if (e instanceof SnowcastException) {
                throw (SnowcastException) e;
            }
            throw new SnowcastException(e);
        }
    }

    private SequencerProvision getOrCreateSequencerProvision(SequencerDefinition definition) {
        String sequencerName = definition.getSequencerName();

        SequencerProvision provision = provisions.get(sequencerName);
        if (provision != null) {
            return provision;
        }

        synchronized (provisions) {
            provision = provisions.get(sequencerName);
            if (provision != null) {
                return provision;
            }

            provision = sequencerConstructor.createNew(definition);
            provisions.put(sequencerName, provision);
            return provision;
        }
    }
}
