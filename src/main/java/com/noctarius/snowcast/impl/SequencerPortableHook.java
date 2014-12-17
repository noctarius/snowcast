package com.noctarius.snowcast.impl;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;
import com.noctarius.snowcast.impl.operations.client.ClientCreateSequencerDefinitionOperation;

import java.util.Collection;

public class SequencerPortableHook
        implements PortableHook {

    public static final int TYPE_ATTACH_LOGICAL_NODE = 1;
    public static final int TYPE_DETACH_LOGICAL_NODE = 2;
    public static final int TYPE_CREATE_SEQUENCER_DEFINITION = 3;
    public static final int TYPE_DESTROY_SEQUENCER_DEFINITION = 4;
    public static final int TYPE_DESTROY_SEQUENCER = 5;

    private static final int LEN = TYPE_DESTROY_SEQUENCER + 1;

    @Override
    public int getFactoryId() {
        return SequencerDataSerializerHook.FACTORY_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            private final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[LEN];

            {
                constructors[TYPE_CREATE_SEQUENCER_DEFINITION] = new ConstructorFunction<Integer, Portable>() {
                    @Override
                    public Portable createNew(Integer integer) {
                        return new ClientCreateSequencerDefinitionOperation();
                    }
                };
            }

            @Override
            public Portable create(int i) {
                return null;
            }
        };
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
