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
import com.hazelcast.client.impl.protocol.codec.SnowcastAttachLogicalNodeCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.impl.SequencerDefinition;

class AttachLogicalNodeMessageTask
        extends AbstractSnowcastMessageTask<SnowcastAttachLogicalNodeCodec.RequestParameters> {

    AttachLogicalNodeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected SnowcastAttachLogicalNodeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SnowcastAttachLogicalNodeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SnowcastAttachLogicalNodeCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.sequencerName;
    }

    @Override
    public String getMethodName() {
        return "execute";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    protected Operation createOperation() {
        String sequencerName = parameters.sequencerName;
        long epochOffset = parameters.epochOffset;
        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        int maxLogicalNodeCount = parameters.maxLogicalNodeCount;
        short backupCount = (short) parameters.backupCount;

        SequencerDefinition sequencerDefinition = new SequencerDefinition(sequencerName, epoch, maxLogicalNodeCount, backupCount);
        return new ClientAttachLogicalNodeOperation(sequencerName, this, sequencerDefinition);
    }
}
