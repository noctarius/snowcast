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

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.util.ConstructorFunction;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

final class ClientSequencerConstructorFunction
        implements ConstructorFunction<SequencerDefinition, SequencerProvision> {

    private static final Tracer TRACER = TracingUtils.tracer(ClientSequencerConstructorFunction.class);

    private final ClientCodec clientCodec;
    private final ProxyManager proxyManager;
    private final ClientSequencerService sequencerService;
    private final MethodHandle proxyManagerInitialize;

    ClientSequencerConstructorFunction(@Nonnull ProxyManager proxyManager, @Nonnull ClientSequencerService sequencerService,
                                       @Nonnull ClientCodec clientCodec) {

        this.clientCodec = clientCodec;
        this.proxyManager = proxyManager;
        this.sequencerService = sequencerService;
        this.proxyManagerInitialize = getInitializeMethod();
    }

    @Nonnull
    @Override
    public SequencerProvision createNew(@Nonnull SequencerDefinition definition) {
        TRACER.trace("create new provision for definition %s", definition);
        ClientSequencer sequencer = new ClientSequencer(sequencerService, definition, clientCodec, proxyManager.getContext());
        initializeProxy(sequencer);
        sequencer.attachLogicalNode();
        return new SequencerProvision(definition, sequencer);
    }

    private void initializeProxy(@Nonnull ClientSequencer sequencer) {
        //ACCESSIBILITY_HACK
        ExceptionUtils.execute(() -> {
            TRACER.trace("initialize sequencer proxy %s", sequencer);
            return proxyManagerInitialize.invoke(proxyManager, sequencer);
        });
    }

    @Nonnull
    private MethodHandle getInitializeMethod() {
        return ExceptionUtils.execute(() -> {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Method method = ProxyManager.class.getDeclaredMethod("initialize", ClientProxy.class);
            method.setAccessible(true);
            return lookup.unreflect(method);
        });
    }
}
