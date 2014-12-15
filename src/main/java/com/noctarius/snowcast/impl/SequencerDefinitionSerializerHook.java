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
package com.noctarius.snowcast.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.noctarius.snowcast.SnowcastEpoch;

import java.io.IOException;

public class SequencerDefinitionSerializerHook
        implements SerializerHook<SequencerDefinition> {

    @Override
    public Class<SequencerDefinition> getSerializationType() {
        return SequencerDefinition.class;
    }

    @Override
    public Serializer createSerializer() {
        return new SequencerDefinitionSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }

    private static class SequencerDefinitionSerializer
            implements StreamSerializer<SequencerDefinition> {

        @Override
        public void write(ObjectDataOutput out, SequencerDefinition definition)
                throws IOException {

            out.writeUTF(definition.getSequencerName());
            out.writeLong(definition.getEpoch().getEpochOffset());
            out.writeInt(definition.getMaxLogicalNodeCount());
        }

        @Override
        public SequencerDefinition read(ObjectDataInput in)
                throws IOException {

            String sequencerName = in.readUTF();
            long epochOffset = in.readLong();
            int maxLogicalNodeCount = in.readInt();
            SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
            return new SequencerDefinition(sequencerName, epoch, maxLogicalNodeCount);
        }

        @Override
        public int getTypeId() {
            return SequencerDataSerializerHook.FACTORY_ID;
        }

        @Override
        public void destroy() {
        }
    }
}
