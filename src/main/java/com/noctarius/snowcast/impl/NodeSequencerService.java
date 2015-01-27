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

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.EventServiceImpl;
import com.hazelcast.util.ConcurrencyUtil;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastIllegalStateException;
import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;
import com.noctarius.snowcast.impl.operations.AttachLogicalNodeOperation;
import com.noctarius.snowcast.impl.operations.CreateSequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.DestroySequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.DetachLogicalNodeOperation;
import com.noctarius.snowcast.impl.operations.SequencerReplicationOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount;
import static com.noctarius.snowcast.impl.SnowcastConstants.SERVICE_NAME;

public class NodeSequencerService
        implements SequencerService, ManagedService, MigrationAwareService, RemoteService,
                   EventPublishingService<Object, Object> {

    private final SequencerPartitionConstructorFunction partitionConstructor = new SequencerPartitionConstructorFunction();
    private final NodeSequencerConstructorFunction sequencerConstructor = new NodeSequencerConstructorFunction(this);

    private final ConcurrentMap<Integer, SequencerPartition> partitions;
    private final ConcurrentMap<String, SequencerProvision> provisions;

    private NodeEngine nodeEngine;
    private EventService eventService;
    private SerializationService serializationService;

    public NodeSequencerService() {
        this.provisions = new ConcurrentHashMap<String, SequencerProvision>();
        this.partitions = new ConcurrentHashMap<Integer, SequencerPartition>();
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        this.serializationService = nodeEngine.getSerializationService();
    }

    @Override
    public SnowcastSequencer createSequencer(String sequencerName, SnowcastEpoch epoch, int maxLogicalNodeCount,
                                             short backupCount) {

        int boundedMaxLogicalNodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        SequencerDefinition definition = new SequencerDefinition(sequencerName, epoch, boundedMaxLogicalNodeCount, backupCount);

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
    public void reset() {
        // TODO WUT?
    }

    @Override
    public void shutdown(boolean terminate) {
        // TODO kill all sequencers
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        SequencerPartition partition = partitions.get(partitionId);
        if (partition == null) {
            return null;
        }

        PartitionReplication partitionReplication = partition.createPartitionReplication();
        return new SequencerReplicationOperation(partitionReplication);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        SequencerPartition partition = partitions.get(partitionId);
        if (partition != null) {
            partition.freeze();
        }
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        partitions.remove(partitionId);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        SequencerPartition partition = partitions.get(partitionId);
        if (partition != null) {
            partition.unfreeze();
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        partitions.remove(partitionId);
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        if (listener instanceof ClientChannelHandler) {
            ((ClientChannelHandler) listener).handleEvent(event);
        }
    }

    public int attachSequencer(SequencerDefinition definition) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());

        AttachLogicalNodeOperation operation = new AttachLogicalNodeOperation(definition);
        OperationService operationService = nodeEngine.getOperationService();

        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
        return (Integer) invocationBuilder.invoke().getSafely();
    }

    public void detachSequencer(SequencerDefinition definition, int logicalNodeId) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());

        DetachLogicalNodeOperation operation = new DetachLogicalNodeOperation(definition, logicalNodeId);
        OperationService operationService = nodeEngine.getOperationService();

        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
        invocationBuilder.invoke().getSafely();
    }

    public SequencerDefinition registerSequencerDefinition(SequencerDefinition definition) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());
        SequencerPartition partition = getSequencerPartition(partitionId);
        return partition.checkOrRegisterSequencerDefinition(definition);
    }

    public SequencerDefinition unregisterSequencerDefinition(String sequencerName) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(sequencerName);
        SequencerPartition partition = getSequencerPartition(partitionId);
        return partition.destroySequencerDefinition(sequencerName);
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

    public EventRegistration registerClientChannel(String sequencerName, ClientEndpoint endpoint, int callId) {
        ClientChannelHandler clientChannelHandler = new ClientChannelHandler(endpoint, callId, serializationService);
        return eventService.registerLocalListener(SnowcastConstants.SERVICE_NAME, sequencerName, clientChannelHandler);
    }

    public void unregisterClientChannel(String sequencerName, String registrationId) {
        eventService.deregisterListener(SnowcastConstants.SERVICE_NAME, sequencerName, registrationId);
    }

    public Collection<EventRegistration> findClientChannelRegistrations(String sequencerName, String clientUuid) {
        Collection<EventRegistration> registrations = eventService
                .getRegistrations(SnowcastConstants.SERVICE_NAME, sequencerName);

        if (clientUuid == null) {
            return registrations;
        }

        // Copy the registrations since the original one is not modifiable
        registrations = new ArrayList<EventRegistration>(registrations);
        Iterator<EventRegistration> iterator = registrations.iterator();
        while (iterator.hasNext()) {
            EventServiceImpl.Registration registration = (EventServiceImpl.Registration) iterator.next();
            ClientChannelHandler channelHandler = (ClientChannelHandler) registration.getListener();
            if (clientUuid.equals(channelHandler.endpoint.getUuid())) {
                iterator.remove();
            }
        }

        return registrations;
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

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new DummyProxy(objectName);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }

    private static class ClientChannelHandler {
        private final ClientEndpoint endpoint;
        private final Data clientUuidData;
        private final int callId;

        public ClientChannelHandler(ClientEndpoint endpoint, int callId, SerializationService serializationService) {
            this.clientUuidData = serializationService.toData(endpoint.getUuid());
            this.endpoint = endpoint;
            this.callId = callId;
        }

        public void handleEvent(Object event) {
            endpoint.sendEvent(clientUuidData, event, callId);
        }
    }
}
