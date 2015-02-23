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

import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SnowcastConstants;

import java.io.IOException;
import java.security.Permission;

abstract class AbstractClientSequencerOperation
        extends InvocationClientRequest {

    private String sequencerName;

    AbstractClientSequencerOperation() {
    }

    AbstractClientSequencerOperation(String sequencerName) {
        this.sequencerName = sequencerName;
    }

    String getSequencerName() {
        return sequencerName;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {

        super.write(writer);
        writer.writeUTF("sequencerName", sequencerName);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);
        sequencerName = reader.readUTF("sequencerName");
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getServiceName() {
        return SnowcastConstants.SERVICE_NAME;
    }
}
