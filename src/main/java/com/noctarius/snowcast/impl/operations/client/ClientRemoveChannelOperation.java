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

import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.noctarius.snowcast.impl.NodeSequencerService;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerPortableHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Permission;

public class ClientRemoveChannelOperation
        extends CallableClientRequest {

    protected String sequencerName;
    protected String registrationId;

    public ClientRemoveChannelOperation() {
    }

    public ClientRemoveChannelOperation(@Nonnull String sequencerName, @Nonnull String registrationId) {
        this.sequencerName = sequencerName;
        this.registrationId = registrationId;
    }

    @Override
    public Object call()
            throws Exception {

        NodeSequencerService sequencerService = getService();
        sequencerService.unregisterClientChannel(sequencerName, registrationId);
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

    @Override
    public String getDistributedObjectName() {
        return sequencerName;
    }

    @Override
    public void write(@Nonnull PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", sequencerName);
        writer.writeUTF("r", registrationId);
    }

    @Override
    public void read(@Nonnull PortableReader reader)
            throws IOException {
        sequencerName = reader.readUTF("n");
        registrationId = reader.readUTF("r");
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{registrationId};
    }

    public String getSequencerName() {
        return sequencerName;
    }
}
