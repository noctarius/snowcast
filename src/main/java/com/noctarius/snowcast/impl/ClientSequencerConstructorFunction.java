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
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.util.ConstructorFunction;
import com.noctarius.snowcast.SnowcastException;

import java.lang.reflect.Method;

final class ClientSequencerConstructorFunction
        implements ConstructorFunction<SequencerDefinition, SequencerProvision> {

    private final HazelcastClientInstanceImpl client;
    private final ProxyManager proxyManager;
    private final Method proxyManagerInitialize;

    ClientSequencerConstructorFunction(HazelcastClientInstanceImpl client, ProxyManager proxyManager) {
        this.client = client;
        this.proxyManager = proxyManager;
        this.proxyManagerInitialize = getInitializeMethod();
    }

    @Override
    public SequencerProvision createNew(SequencerDefinition definition) {
        ClientSequencer sequencer = new ClientSequencer(client, definition);
        initializeProxy(sequencer);
        sequencer.attachLogicalNode();
        return new SequencerProvision(definition, sequencer);
    }

    private void initializeProxy(ClientSequencer sequencer) {
        try {
            proxyManagerInitialize.invoke(proxyManager, sequencer);
        } catch (Exception e) {
            throw new SnowcastException(e);
        }
    }

    private Method getInitializeMethod() {
        try {
            Method method = ProxyManager.class.getDeclaredMethod("initialize", ClientProxy.class);
            method.setAccessible(true);
            return method;
        } catch (Exception e) {
            throw new SnowcastException(e);
        }
    }
}
