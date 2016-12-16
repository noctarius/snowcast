package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.Address;

public interface MessageChannel {

    void sendClientMessage(ClientMessage clientMessage);

    void sendClientMessage(Object partitionKey, ClientMessage clientMessage);

    Address getAddress();

    String getUuid();

}
