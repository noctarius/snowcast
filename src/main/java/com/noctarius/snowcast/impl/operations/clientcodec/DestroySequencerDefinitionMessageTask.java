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
import com.hazelcast.client.impl.protocol.codec.SnowcastDestroySequencerDefinitionCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;

class DestroySequencerDefinitionMessageTask
        extends AbstractSnowcastMessageTask<SnowcastDestroySequencerDefinitionCodec.RequestParameters> {

    DestroySequencerDefinitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation createOperation() {
        return new ClientDestroySequencerDefinitionOperation(parameters.sequencerName, this);
    }

    @Override
    protected SnowcastDestroySequencerDefinitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SnowcastDestroySequencerDefinitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SnowcastDestroySequencerDefinitionCodec.encodeResponse((Boolean) response);
    }
}
