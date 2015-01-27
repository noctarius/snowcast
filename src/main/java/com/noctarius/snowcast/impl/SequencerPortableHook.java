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

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;
import com.noctarius.snowcast.impl.notification.ClientDestroySequencerNotification;
import com.noctarius.snowcast.impl.operations.client.ClientAttachLogicalNodeRequest;
import com.noctarius.snowcast.impl.operations.client.ClientCreateSequencerDefinitionRequest;
import com.noctarius.snowcast.impl.operations.client.ClientDestroySequencerDefinitionRequest;
import com.noctarius.snowcast.impl.operations.client.ClientDetachLogicalNodeRequest;
import com.noctarius.snowcast.impl.operations.client.ClientRegisterChannelOperation;
import com.noctarius.snowcast.impl.operations.client.ClientRemoveChannelOperation;

import java.util.Collection;

public class SequencerPortableHook
        implements PortableHook {

    public static final int TYPE_ATTACH_LOGICAL_NODE = 1;
    public static final int TYPE_DETACH_LOGICAL_NODE = 2;
    public static final int TYPE_CREATE_SEQUENCER_DEFINITION = 3;
    public static final int TYPE_DESTROY_SEQUENCER_DEFINITION = 4;
    public static final int TYPE_DESTROY_SEQUENCER = 5;
    public static final int TYPE_REGISTER_CHANNEL = 6;
    public static final int TYPE_REMOVE_CHANNEL = 7;

    private static final int LEN = TYPE_REMOVE_CHANNEL + 1;

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            private final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[LEN];

            {
                constructors[TYPE_ATTACH_LOGICAL_NODE] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientAttachLogicalNodeRequest();
                    }
                };
                constructors[TYPE_DETACH_LOGICAL_NODE] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientDetachLogicalNodeRequest();
                    }
                };
                constructors[TYPE_CREATE_SEQUENCER_DEFINITION] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer integer) {
                        return new ClientCreateSequencerDefinitionRequest();
                    }
                };
                constructors[TYPE_DESTROY_SEQUENCER_DEFINITION] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientDestroySequencerDefinitionRequest();
                    }
                };
                constructors[TYPE_DESTROY_SEQUENCER] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientDestroySequencerNotification();
                    }
                };
                constructors[TYPE_REGISTER_CHANNEL] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientRegisterChannelOperation();
                    }
                };
                constructors[TYPE_REMOVE_CHANNEL] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer arg) {
                        return new ClientRemoveChannelOperation();
                    }
                };
            }

            @Override
            public Portable create(int classId) {
                return (classId > 0 && classId <= constructors.length) ? constructors[classId].createNew(classId) : null;
            }
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
