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

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerPortableHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

import java.security.Permission;

public class ClientRemoveChannelOperation
        extends BaseClientRemoveListenerRequest {

    public ClientRemoveChannelOperation() {
    }

    public ClientRemoveChannelOperation(String sequencerName, String registrationId) {
        super(sequencerName, registrationId);
    }

    @Override
    public Object call()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        sequencerService.unregisterClientChannel(getName(), registrationId);
        return Boolean.TRUE;
    }

    @Override
    public String getServiceName() {
        return SnowcastConstants.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_REMOVE_CHANNEL;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
