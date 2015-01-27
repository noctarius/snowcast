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
package com.noctarius.snowcast.impl.operations.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.spi.EventRegistration;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerPortableHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

public class ClientRegisterChannelOperation
        extends AbstractClientSequencerOperation {

    public ClientRegisterChannelOperation() {
    }

    public ClientRegisterChannelOperation(String sequencerName) {
        super(sequencerName);
    }

    @Override
    protected void invoke() {
        NodeSequencerService sequencerService = getService();
        EventRegistration registration = sequencerService.registerClientChannel(getSequencerName(), getEndpoint(), getCallId());

        ClientEndpoint endpoint = getEndpoint();
        String registrationId = registration.getId();

        endpoint.setListenerRegistration(SnowcastConstants.SERVICE_NAME, endpoint.getUuid(), registrationId);
        endpoint.sendResponse(registrationId, getCallId());
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_REGISTER_CHANNEL;
    }
}
