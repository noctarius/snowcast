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
package com.noctarius.snowcast.impl.operations;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerDefinition;
import com.noctarius.snowcast.impl.notification.ClientDestroySequencerNotification;

import java.util.Collection;

import static com.noctarius.snowcast.impl.SnowcastConstants.SERVICE_NAME;

public class DestroySequencerDefinitionOperation
        extends AbstractSequencerOperation
        implements BackupAwareOperation {

    private transient int backupCount;

    public DestroySequencerDefinitionOperation() {
    }

    public DestroySequencerDefinitionOperation(String sequencerName) {
        super(sequencerName);
    }

    @Override
    public int getId() {
        return SequencerDataSerializerHook.TYPE_DESTROY_SEQUENCER_DEFINITION;
    }

    @Override
    public void run()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        String sequencerName = getSequencerName();

        SequencerDefinition definition = sequencerService.destroySequencer(sequencerName, true);

        // Definition might be already destroyed concurrently
        if (definition == null) {
            return;
        }

        backupCount = definition.getBackupCount();

        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();

        DestroySequencerOperation operation = new DestroySequencerOperation(sequencerName);
        for (MemberImpl member : nodeEngine.getClusterService().getMemberImpls()) {
            if (!member.localMember() && !member.getAddress().equals(getCallerAddress())) {
                operationService.invokeOnTarget(SERVICE_NAME, operation, member.getAddress());
            }
        }

        ClientDestroySequencerNotification notification = new ClientDestroySequencerNotification(sequencerName);
        Collection<EventRegistration> registrations = sequencerService.findClientChannelRegistrations(sequencerName, null);
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrations, notification, 1);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return backupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new BackupDestroySequencerDefinitionOperation(getSequencerName());
    }
}
