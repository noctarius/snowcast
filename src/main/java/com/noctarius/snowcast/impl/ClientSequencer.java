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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.SerializationService;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;
import com.noctarius.snowcast.impl.notification.ClientDestroySequencerNotification;
import com.noctarius.snowcast.impl.operations.client.ClientAttachLogicalNodeRequest;
import com.noctarius.snowcast.impl.operations.client.ClientDetachLogicalNodeRequest;
import com.noctarius.snowcast.impl.operations.client.ClientRegisterChannelOperation;
import com.noctarius.snowcast.impl.operations.client.ClientRemoveChannelOperation;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class ClientSequencer
        extends ClientProxy
        implements InternalSequencer {

    private final ClientSequencerContext sequencerContext;
    private final ClientSequencerService sequencerService;

    ClientSequencer(@Nonnull HazelcastClientInstanceImpl client, @Nonnull ClientSequencerService sequencerService,
                    @Nonnull SequencerDefinition definition, @Nonnull ClientInvocator clientInvocator) {

        super(SnowcastConstants.SERVICE_NAME, definition.getSequencerName());
        this.sequencerService = sequencerService;
        this.sequencerContext = new ClientSequencerContext(client, definition, clientInvocator);
    }

    @Nonnull
    @Override
    public String getSequencerName() {
        return sequencerContext.getSequencerName();
    }

    @Override
    public long next()
            throws InterruptedException {

        return sequencerContext.next();
    }

    @Nonnull
    @Override
    public SnowcastSequenceState getSequencerState() {
        return sequencerContext.getSequencerState();
    }

    @Nonnull
    @Override
    public SequencerDefinition getSequencerDefinition() {
        return sequencerContext.getSequencerDefinition();
    }

    @Nonnull
    @Override
    public SnowcastSequencer attachLogicalNode() {
        sequencerContext.attachLogicalNode();
        return this;
    }

    @Nonnull
    @Override
    public SnowcastSequencer detachLogicalNode() {
        sequencerContext.detachLogicalNode();
        return this;
    }

    @Override
    @Nonnegative
    public long timestampValue(long sequenceId) {
        return sequencerContext.timestampValue(sequenceId);
    }

    @Override
    @Nonnegative
    public int logicalNodeId(long sequenceId) {
        return sequencerContext.logicalNodeId(sequenceId);
    }

    @Override
    @Nonnegative
    public int counterValue(long sequenceId) {
        return sequencerContext.counterValue(sequenceId);
    }

    @Override
    protected void onInitialize() {
        sequencerContext.initialize(this);
    }

    @Override
    public void stateTransition(@Nonnull SnowcastSequenceState newState) {
        sequencerContext.stateTransition(newState);
    }

    @Nonnull
    @Override
    public SequencerService getSequencerService() {
        return sequencerService;
    }

    private void unregisterClientChannel(@Nonnull String uuid) {
        ClientContext context = getContext();
        context.getListenerService().deRegisterListener(uuid);
    }

    private static class ClientSequencerContext
            extends AbstractSequencerContext {

        private static final Tracer TRACER = TracingUtils.tracer(ClientSequencerContext.class);

        private final HazelcastClientInstanceImpl client;
        private final ClientInvocator clientInvocator;

        private volatile String channelRegistration;

        private ClientSequencerContext(@Nonnull HazelcastClientInstanceImpl client, @Nonnull SequencerDefinition definition,
                                       @Nonnull ClientInvocator clientInvocator) {

            super(definition);
            this.client = client;
            this.clientInvocator = clientInvocator;
        }

        @Min(128)
        @Override
        @Max(8192)
        protected int doAttachLogicalNode(@Nonnull SequencerDefinition definition) {
            TRACER.trace("doAttachLogicalNode begin");
            PartitionService partitionService = client.getPartitionService();
            Partition partition = partitionService.getPartition(getSequencerName());
            int partitionId = partition.getPartitionId();

            try {
                PartitionClientRequest request = new ClientAttachLogicalNodeRequest(getSequencerName(), partitionId, definition);
                ICompletableFuture<Object> future = clientInvocator.invoke(partitionId, request);
                Object response = future.get();
                return client.getSerializationService().toObject(response);
            } catch (Exception e) {
                throw new SnowcastException(e);
            } finally {
                TRACER.trace("doAttachLogicalNode end");
            }
        }

        @Override
        protected void doDetachLogicalNode(@Nonnull SequencerDefinition definition, @Min(128) @Max(8192) int logicalNodeId) {
            TRACER.trace("doDetachLogicalNode begin");
            PartitionService partitionService = client.getPartitionService();
            Partition partition = partitionService.getPartition(getSequencerName());
            int partitionId = partition.getPartitionId();

            try {
                PartitionClientRequest request = new ClientDetachLogicalNodeRequest(definition, partitionId, logicalNodeId);
                ICompletableFuture<Object> future = clientInvocator.invoke(partitionId, request);
                future.get();
            } catch (Exception e) {
                throw new SnowcastException(e);
            } finally {
                TRACER.trace("doDetachLogicalNode end");
            }
        }

        private void unregisterClientChannel(@Nonnull ClientSequencer clientSequencer) {
            TRACER.trace("unregister from channel for sequencer %s", clientSequencer.getSequencerName());
            try {
                ClientRemoveChannelOperation operation = new ClientRemoveChannelOperation(getSequencerName(), channelRegistration);
                ICompletableFuture<Boolean> future = clientInvocator.invoke(-1, operation);
                boolean result = future.get() == null ? false : (Boolean) future.get();
                if (result) {
                    clientSequencer.unregisterClientChannel(channelRegistration);
                }
            } catch (Exception e) {
                throw new SnowcastException(e);
            } finally {
                TRACER.trace("unregisterClientChannel end");
            }
        }

        public void initialize(@Nonnull ClientSequencer clientSequencer) {
            TRACER.trace("register on channel for sequencer %s", clientSequencer.getSequencerName());
            ClientRegisterChannelOperation operation = new ClientRegisterChannelOperation(getSequencerName());
            ClientChannelHandler handler = new ClientChannelHandler(this, clientSequencer);
            channelRegistration = clientSequencer.listen(operation, getSequencerName(), handler);
        }
    }

    private static class ClientChannelHandler
            implements EventHandler<Portable> {

        private static final Tracer TRACER = TracingUtils.tracer(ClientChannelHandler.class);

        private final ClientSequencerContext sequencerContext;
        private final ClientSequencer clientSequencer;

        private ClientChannelHandler(@Nonnull ClientSequencerContext sequencerContext, @Nonnull ClientSequencer clientSequencer) {
            this.sequencerContext = sequencerContext;
            this.clientSequencer = clientSequencer;
        }

        @Override
        public void handle(@Nonnull Portable event) {
            TRACER.trace("New event message retrieved");
            ClientContext context = clientSequencer.getContext();
            SerializationService serializationService = context.getSerializationService();

            Object message = serializationService.toObject(event);
            if (message instanceof ClientDestroySequencerNotification) {
                TRACER.trace("ClientDestroySequencerNotification received");
                clientSequencer.stateTransition(SnowcastSequenceState.Destroyed);
                sequencerContext.unregisterClientChannel(clientSequencer);
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }
}
