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
package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.codec.SnowcastAttachLogicalNodeCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastCreateSequencerDefinitionCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastDestroySequencerDefinitionCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastDetachLogicalNodeCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastMessageType;
import com.hazelcast.client.impl.protocol.codec.SnowcastRegisterChannelCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastRemoveChannelCodec;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public final class SnowcastMessageTaskFactoryProvider
        implements MessageTaskFactoryProvider {

    private final Node node;
    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];

    public SnowcastMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        register(SnowcastAttachLogicalNodeCodec.RequestParameters.TYPE, AttachLogicalNodeMessageTask::new);
        register(SnowcastCreateSequencerDefinitionCodec.RequestParameters.TYPE, CreateSequencerDefinitionMessageTask::new);
        register(SnowcastDestroySequencerDefinitionCodec.RequestParameters.TYPE, DestroySequencerDefinitionMessageTask::new);
        register(SnowcastDetachLogicalNodeCodec.RequestParameters.TYPE, DetachLogicalNodeMessageTask::new);
        register(SnowcastRegisterChannelCodec.RequestParameters.TYPE, RegisterChannelMessageTask::new);
        register(SnowcastRemoveChannelCodec.RequestParameters.TYPE, RemoveChannelMessageTask::new);
    }

    @Override
    public MessageTaskFactory[] getFactories() {
        return factories.clone();
    }

    private MessageTaskFactory toFactory(MessageTaskConstructor constructor) {
        return ((clientMessage, connection) -> constructor.construct(clientMessage, node, connection));
    }

    private interface MessageTaskConstructor {
        MessageTask construct(ClientMessage message, Node node, Connection connection);
    }

    private void register(SnowcastMessageType messageType, MessageTaskConstructor constructor) {
        factories[messageType.id()] = toFactory(constructor);
    }
}
