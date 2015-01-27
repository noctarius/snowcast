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
package com.noctarius.snowcast.impl.notification;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.noctarius.snowcast.impl.SequencerDataSerializerHook;
import com.noctarius.snowcast.impl.SequencerPortableHook;

import java.io.IOException;

public class ClientDestroySequencerNotification
        implements Portable {

    private String sequencerName;

    public ClientDestroySequencerNotification() {
    }

    public ClientDestroySequencerNotification(String sequencerName) {
        this.sequencerName = sequencerName;
    }

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return SequencerPortableHook.TYPE_DESTROY_SEQUENCER;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {

        writer.writeUTF("sn", sequencerName);
    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {

        this.sequencerName = reader.readUTF("sn");
    }

    public String getSequencerName() {
        return sequencerName;
    }
}
