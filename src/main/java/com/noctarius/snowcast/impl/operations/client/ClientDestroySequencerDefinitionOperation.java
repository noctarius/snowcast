package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.notification.ClientDestroySequencerNotification;
import com.noctarius.snowcast.impl.operations.DestroySequencerOperation;

import java.util.Collection;

import static com.noctarius.snowcast.impl.SnowcastConstants.SERVICE_NAME;

class ClientDestroySequencerDefinitionOperation
        extends AbstractClientRequestOperation {

    ClientDestroySequencerDefinitionOperation(String sequencerName, ClientEndpoint endpoint) {
        super(sequencerName, endpoint);
    }

    @Override
    public void run()
            throws Exception {

        String sequencerName = getSequencerName();

        NodeSequencerService sequencerService = getService();
        sequencerService.destroySequencer(sequencerName, true);

        NodeEngine nodeEngine = getNodeEngine();

        OperationService operationService = nodeEngine.getOperationService();
        DestroySequencerOperation operation = new DestroySequencerOperation(sequencerName);
        for (MemberImpl member : nodeEngine.getClusterService().getMemberList()) {
            if (!member.localMember()) {
                operationService.invokeOnTarget(SERVICE_NAME, operation, member.getAddress());
            }
        }

        String clientUuid = getEndpoint().getUuid();

        ClientDestroySequencerNotification notification = new ClientDestroySequencerNotification(sequencerName);
        Collection<EventRegistration> registrations = sequencerService.findClientChannelRegistrations(sequencerName, clientUuid);
        EventService eventService = nodeEngine.getEventService();
        eventService.publishEvent(SERVICE_NAME, registrations, notification, 1);
        eventService.deregisterAllListeners(SERVICE_NAME, sequencerName);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }
}
