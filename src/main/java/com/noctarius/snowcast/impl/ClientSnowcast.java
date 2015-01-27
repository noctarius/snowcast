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
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.core.HazelcastInstance;
import com.noctarius.snowcast.Snowcast;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastSequencer;

import java.lang.reflect.Field;

import static com.noctarius.snowcast.impl.SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS;

class ClientSnowcast
        implements Snowcast {

    private final short backupCount;
    private final HazelcastClientInstanceImpl client;
    private final ClientSequencerService sequencerService;

    ClientSnowcast(HazelcastInstance hazelcastInstance, short backupCount) {
        this.backupCount = backupCount;
        this.client = getHazelcastClient(hazelcastInstance);
        ProxyManager proxyManager = getProxyManager(client);
        this.sequencerService = new ClientSequencerService(client, proxyManager);
    }

    @Override
    public SnowcastSequencer createSequencer(String sequencerName, SnowcastEpoch epoch) {
        return createSequencer(sequencerName, epoch, DEFAULT_MAX_LOGICAL_NODES_13_BITS);
    }

    @Override
    public SnowcastSequencer createSequencer(String sequencerName, SnowcastEpoch epoch, int maxLogicalNodeCount) {
        return sequencerService.createSequencer(sequencerName, epoch, maxLogicalNodeCount, backupCount);
    }

    @Override
    public void destroySequencer(SnowcastSequencer sequencer) {
        sequencerService.destroySequencer(sequencer);
    }

    private HazelcastClientInstanceImpl getHazelcastClient(HazelcastInstance hazelcastInstance) {
        try {
            // Ugly hack due to lack in SPI
            Field clientField = HazelcastClientProxy.class.getDeclaredField("client");
            clientField.setAccessible(true);
            return (HazelcastClientInstanceImpl) clientField.get(hazelcastInstance);
        } catch (Exception e) {
            String message = ExceptionMessages.RETRIEVE_CLIENT_ENGINE_FAILED.buildMessage();
            throw new SnowcastException(message, e);
        }
    }

    private ProxyManager getProxyManager(HazelcastClientInstanceImpl client) {
        try {
            // And another ugly hack due to lack in SPI
            Field proxyManagerField = HazelcastClientInstanceImpl.class.getDeclaredField("proxyManager");
            proxyManagerField.setAccessible(true);

            return (ProxyManager) proxyManagerField.get(client);
        } catch (Exception e) {
            String message = ExceptionMessages.RETRIEVE_CLIENT_ENGINE_FAILED.buildMessage();
            throw new SnowcastException(message, e);
        }
    }
}
