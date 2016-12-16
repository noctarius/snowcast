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
