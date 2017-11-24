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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxyCodec;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ConstructorFunction;

import javax.annotation.Nonnull;

final class ClientSequencerConstructorFunction
        implements ConstructorFunction<SequencerDefinition, SequencerProvision> {

    private static final Tracer TRACER = TracingUtils.tracer(ClientSequencerConstructorFunction.class);

    private final ClientCodec clientCodec;
    private final ProxyManager proxyManager;
    private final ClientSequencerService sequencerService;
    private final HazelcastClientInstanceImpl client;

    ClientSequencerConstructorFunction(@Nonnull HazelcastClientInstanceImpl client,
                                       @Nonnull ProxyManager proxyManager,
                                       @Nonnull ClientSequencerService sequencerService,
                                       @Nonnull ClientCodec clientCodec) {
        this.client = client;
        this.clientCodec = clientCodec;
        this.proxyManager = proxyManager;
        this.sequencerService = sequencerService;
    }

    @Nonnull
    @Override
    public SequencerProvision createNew(@Nonnull SequencerDefinition definition) {
        TRACER.trace("create new provision for definition %s", definition);
        ClientSequencer sequencer = new ClientSequencer(sequencerService, definition, clientCodec, proxyManager.getContext());

        Address initializationTarget = proxyManager.findNextAddressToSendCreateRequest();
        if (initializationTarget == null) {
            throw new RuntimeException("Not able to find a member to create proxy on!");
        } else {
            ClientMessage clientMessage = ClientCreateProxyCodec.encodeRequest(
                    definition.getSequencerName(), sequencer.getServiceName(), initializationTarget);
            ExceptionUtils.execute(() -> {
                new ClientInvocation(client, clientMessage, sequencer.getServiceName(), initializationTarget)
                        .invoke().get();
                return null;
            });
            sequencer.onInitialize();
        }
        sequencer.attachLogicalNode();
        return new SequencerProvision(definition, sequencer);
    }
}
