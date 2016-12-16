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
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.noctarius.snowcast.impl.SnowcastConstants;

import java.security.Permission;

abstract class AbstractSnowcastMessageTask<P>
        extends AbstractInvocationMessageTask<P>
        implements MessageChannel {

    AbstractSnowcastMessageTask(ClientMessage clientMessage, Node node, Connection connection) {

        super(clientMessage, node, connection);
    }

    @Override
    protected final Operation prepareOperation() {
        Operation operation = createOperation();
        return operation.setPartitionId(getPartitionId()).setServiceName(getServiceName());
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation operation) {
        return nodeEngine.getOperationService().createInvocationBuilder( //
                SnowcastConstants.SERVICE_NAME, operation, nodeEngine.getThisAddress());
    }

    @Override
    public String getServiceName() {
        return SnowcastConstants.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void sendClientMessage(ClientMessage clientMessage) {
        super.sendClientMessage(clientMessage);
    }

    @Override
    public void sendClientMessage(Object partitionKey, ClientMessage clientMessage) {
        super.sendClientMessage(partitionKey, clientMessage);
    }

    @Override
    public Address getAddress() {
        return endpoint.getConnection().getEndPoint();
    }

    @Override
    public String getUuid() {
        return endpoint.getUuid();
    }

    protected abstract Operation createOperation();
}
