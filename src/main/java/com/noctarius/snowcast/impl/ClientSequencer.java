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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SnowcastRegisterChannelCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastRemoveChannelCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;
import com.noctarius.snowcast.impl.notification.ClientDestroySequencerNotification;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class ClientSequencer
        extends ClientProxy
        implements InternalSequencer {

    private final ClientSequencerContext sequencerContext;
    private final ClientSequencerService sequencerService;

    ClientSequencer(@Nonnull ClientSequencerService sequencerService, @Nonnull SequencerDefinition definition,
                    @Nonnull ClientCodec clientCodec) {

        super(SnowcastConstants.SERVICE_NAME, definition.getSequencerName());
        this.sequencerService = sequencerService;
        this.sequencerContext = new ClientSequencerContext(definition, clientCodec);
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
        context.getListenerService().deregisterListener(uuid);
    }

    private static class ClientSequencerContext
            extends AbstractSequencerContext {

        private static final Tracer TRACER = TracingUtils.tracer(ClientSequencerContext.class);

        private final ListenerMessageCodec listenerMessageCodec = new SequencerListenerMessageCodec();
        private final ClientCodec clientCodec;

        private volatile String channelRegistration;

        private ClientSequencerContext(@Nonnull SequencerDefinition definition, @Nonnull ClientCodec clientCodec) {
            super(definition);
            this.clientCodec = clientCodec;
        }

        @Min(128)
        @Override
        @Max(8192)
        protected int doAttachLogicalNode(@Nonnull SequencerDefinition definition) {
            TRACER.trace("doAttachLogicalNode begin");
            try {
                return clientCodec.attachLogicalNode(getSequencerName(), definition);
            } finally {
                TRACER.trace("doAttachLogicalNode end");
            }
        }

        @Override
        protected void doDetachLogicalNode(@Nonnull SequencerDefinition definition, @Min(128) @Max(8192) int logicalNodeId) {
            TRACER.trace("doDetachLogicalNode begin");
            try {
                clientCodec.detachLogicalNode(getSequencerName(), definition, logicalNodeId);
            } finally {
                TRACER.trace("doDetachLogicalNode end");
            }
        }

        private void unregisterClientChannel(@Nonnull ClientSequencer clientSequencer) {
            TRACER.trace("unregister from channel for sequencer %s", clientSequencer.getSequencerName());
            try {
                boolean result = clientCodec.removeChannel(getSequencerName(), channelRegistration);
                if (result) {
                    clientSequencer.unregisterClientChannel(channelRegistration);
                }
            } finally {
                TRACER.trace("unregisterClientChannel end");
            }
        }

        void initialize(@Nonnull ClientSequencer clientSequencer) {
            TRACER.trace("register on channel for sequencer %s", clientSequencer.getSequencerName());
            ClientChannelHandler handler = new ClientChannelHandler(this, clientSequencer);
            channelRegistration = clientSequencer.registerListener(listenerMessageCodec, handler);
        }

        private class SequencerListenerMessageCodec
                implements ListenerMessageCodec {

            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return SnowcastRegisterChannelCodec.encodeRequest(getSequencerName());
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return SnowcastRegisterChannelCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return SnowcastRemoveChannelCodec.encodeRequest(getSequencerName(), realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return SnowcastRemoveChannelCodec.decodeResponse(clientMessage).response;
            }
        }
    }

    private static class ClientChannelHandler
            extends SnowcastRegisterChannelCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private static final Tracer TRACER = TracingUtils.tracer(ClientChannelHandler.class);

        private final ClientSequencerContext sequencerContext;
        private final ClientSequencer clientSequencer;

        private ClientChannelHandler(@Nonnull ClientSequencerContext sequencerContext, @Nonnull ClientSequencer clientSequencer) {
            this.sequencerContext = sequencerContext;
            this.clientSequencer = clientSequencer;
        }

        @Override
        public void handle(Data item, long publishTime, String uuid) {
            TRACER.trace("New event message retrieved");
            ClientContext context = clientSequencer.getContext();
            SerializationService serializationService = context.getSerializationService();

            Object message = serializationService.toObject(item);
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
