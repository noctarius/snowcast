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
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.noctarius.snowcast.impl.SnowcastConstants.SERVICE_NAME;

public class NodeSequencerService
        implements SequencerService, ManagedService, MigrationAwareService, RemoteService,
                   EventPublishingService<Object, Object> {

    private final SequencerPartitionConstructorFunction partitionConstructor = new SequencerPartitionConstructorFunction();
    private final NodeSequencerConstructorFunction sequencerConstructor = new NodeSequencerConstructorFunction(this);

    private final ConcurrentMap<Integer, SequencerPartition> partitions;
    private final ConcurrentMap<String, SequencerProvision> provisions;

    private final Method getListenerMethod;

    private NodeEngine nodeEngine;
    private EventService eventService;
    private SerializationService serializationService;

    public NodeSequencerService() {
        this.provisions = new ConcurrentHashMap<String, SequencerProvision>();
        this.partitions = new ConcurrentHashMap<Integer, SequencerPartition>();
        this.getListenerMethod = findEventRegistrationGetListener();
    }

    @Override
    public void init(@Nonnull NodeEngine nodeEngine, @Nullable Properties properties) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        this.serializationService = nodeEngine.getSerializationService();
    }

    @Nonnull
    @Override
    public SnowcastSequencer createSequencer(@Nonnull String sequencerName, @Nonnull SnowcastEpoch epoch,
                                             @Min(128) @Max(8192) int maxLogicalNodeCount, short backupCount) {

        SequencerDefinition definition = new SequencerDefinition(sequencerName, epoch, maxLogicalNodeCount, backupCount);

        Operation operation = new CreateSequencerDefinitionOperation(definition);
        SequencerDefinition realDefinition = invoke(operation, sequencerName);

        if (!definition.equals(realDefinition)) {
            String message = ExceptionMessages.SEQUENCER_ALREADY_REGISTERED.buildMessage();
            throw new SnowcastIllegalStateException(message);
        }

        return getOrCreateSequencerProvision(realDefinition).getSequencer();
    }

    @Override
    public void destroySequencer(@Nonnull SnowcastSequencer sequencer) {
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
        // No action here, however not sure if this shouldn't kill all sequencers
    }

    @Override
    public void shutdown(boolean terminate) {
        // No action here, however not sure if this shouldn't kill all sequencers
    }

    @Nullable
    @Override
    public Operation prepareReplicationOperation(@Nonnull PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        SequencerPartition partition = partitions.get(partitionId);
        if (partition == null) {
            return null;
        }

        PartitionReplication partitionReplication = partition.createPartitionReplication();
        return new SequencerReplicationOperation(partitionReplication);
    }

    @Override
    public void beforeMigration(@Nonnull PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        SequencerPartition partition = partitions.get(partitionId);
        if (partition != null) {
            partition.freeze();
        }
    }

    @Override
    public void commitMigration(@Nonnull PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        SequencerPartition partition = partitions.get(partitionId);
        if (partition != null) {
            if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                partitions.remove(partitionId);
            }
            partition.unfreeze();
        }
    }

    @Override
    public void rollbackMigration(@Nonnull PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        SequencerPartition partition = partitions.get(partitionId);
        if (partition != null) {
            partition.unfreeze();
        }
    }

    @Override
    public void clearPartitionReplica(@Nonnegative int partitionId) {
        partitions.remove(partitionId);
    }

    @Override
    public void dispatchEvent(@Nonnull Object event, @Nonnull Object listener) {
        if (listener instanceof ClientChannelHandler) {
            ((ClientChannelHandler) listener).handleEvent(event);
        }
    }

    public int attachSequencer(@Nonnull SequencerDefinition definition) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());

        AttachLogicalNodeOperation operation = new AttachLogicalNodeOperation(definition);
        OperationService operationService = nodeEngine.getOperationService();

        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
        return (Integer) invocationBuilder.invoke().getSafely();
    }

    public void detachSequencer(@Nonnull SequencerDefinition definition, @Min(128) @Max(8192) int logicalNodeId) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());

        DetachLogicalNodeOperation operation = new DetachLogicalNodeOperation(definition, logicalNodeId);
        OperationService operationService = nodeEngine.getOperationService();

        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
        invocationBuilder.invoke().getSafely();
    }

    @Nonnull
    public SequencerDefinition registerSequencerDefinition(@Nonnull SequencerDefinition definition) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(definition.getSequencerName());
        SequencerPartition partition = getSequencerPartition(partitionId);
        return partition.checkOrRegisterSequencerDefinition(definition);
    }

    @Nullable
    public SequencerDefinition unregisterSequencerDefinition(@Nonnull String sequencerName) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(sequencerName);
        SequencerPartition partition = getSequencerPartition(partitionId);
        return partition.destroySequencerDefinition(sequencerName);
    }

    @Nullable
    public SequencerDefinition destroySequencer(@Nonnull String sequencerName, boolean local) {
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

    @Nonnull
    public SequencerPartition getSequencerPartition(@Nonnegative int partitionId) {
        return ConcurrencyUtil.getOrPutIfAbsent(partitions, partitionId, partitionConstructor);
    }

    @Nonnull
    public EventRegistration registerClientChannel(@Nonnull String sequencerName, @Nonnull ClientEndpoint endpoint, int callId) {
        ClientChannelHandler clientChannelHandler = new ClientChannelHandler(endpoint, callId, serializationService);
        return eventService.registerLocalListener(SnowcastConstants.SERVICE_NAME, sequencerName, clientChannelHandler);
    }

    public void unregisterClientChannel(@Nonnull String sequencerName, @Nonnull String registrationId) {
        eventService.deregisterListener(SnowcastConstants.SERVICE_NAME, sequencerName, registrationId);
    }

    @Nonnull
    public Collection<EventRegistration> findClientChannelRegistrations(@Nonnull String sequencerName,
                                                                        @Nullable String clientUuid) {

        Collection<EventRegistration> registrations = eventService
                .getRegistrations(SnowcastConstants.SERVICE_NAME, sequencerName);

        if (clientUuid == null) {
            return registrations;
        }

        // Copy the registrations since the original one is not modifiable
        registrations = new ArrayList<EventRegistration>(registrations);
        Iterator<EventRegistration> iterator = registrations.iterator();
        while (iterator.hasNext()) {
            ClientChannelHandler channelHandler = getClientChannelHandler(iterator.next());
            if (clientUuid.equals(channelHandler.endpoint.getUuid())) {
                iterator.remove();
            }
        }

        return registrations;
    }

    @Nullable
    private <T> T invoke(@Nonnull Operation operation, @Nonnull String sequencerName) {
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

    @Nonnull
    private SequencerProvision getOrCreateSequencerProvision(@Nonnull SequencerDefinition definition) {
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

    @Nonnull
    @Override
    public DistributedObject createDistributedObject(@Nonnull String objectName) {
        return new DummyProxy(objectName);
    }

    @Override
    public void destroyDistributedObject(@Nonnull String objectName) {
    }

    private ClientChannelHandler getClientChannelHandler(EventRegistration registration) {
        try {
            return (ClientChannelHandler) getListenerMethod.invoke(registration);

        } catch (Exception e) {
            String message = ExceptionMessages.PARAMETER_IS_NOT_SUPPORTED.buildMessage("registration");
            throw new SnowcastException(message, e);
        }
    }

    private Method findEventRegistrationGetListener() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        if (InternalSequencerUtils.getHazelcastVersion() == SnowcastConstants.HazelcastVersion.V_3_4) {
            return hz34EventRegistrationGetListener(buildInfo);
        }
        return hz35EventRegistrationGetListener(buildInfo);
    }

    private Method hz34EventRegistrationGetListener(BuildInfo buildInfo) {
        try {
            Class<?> clazz = Class.forName("com.hazelcast.spi.impl.EventServiceImpl$Registration");
            return clazz.getMethod("getListener");

        } catch (Exception e) {
            String message = ExceptionMessages.INTERNAL_SETUP_FAILED.buildMessage(buildInfo.getVersion());
            throw new SnowcastException(message, e);
        }
    }

    private Method hz35EventRegistrationGetListener(BuildInfo buildInfo) {
        try {
            Class<?> clazz = Class.forName("com.hazelcast.spi.impl.eventservice.impl.Registration");
            return clazz.getMethod("getListener");

        } catch (Exception e) {
            String message = ExceptionMessages.INTERNAL_SETUP_FAILED.buildMessage(buildInfo.getVersion());
            throw new SnowcastException(message, e);
        }
    }

    private static class ClientChannelHandler {
        private final ClientEndpoint endpoint;
        private final Data clientUuidData;
        private final int callId;

        public ClientChannelHandler(@Nonnull ClientEndpoint endpoint, int callId,
                                    @Nonnull SerializationService serializationService) {

            this.clientUuidData = serializationService.toData(endpoint.getUuid());
            this.endpoint = endpoint;
            this.callId = callId;
        }

        public void handleEvent(@Nonnull Object event) {
            endpoint.sendEvent(clientUuidData, event, callId);
        }
    }
}
