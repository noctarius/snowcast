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

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;
import com.noctarius.snowcast.impl.operations.AttachLogicalNodeOperation;
import com.noctarius.snowcast.impl.operations.BackupAttachLogicalNodeOperation;
import com.noctarius.snowcast.impl.operations.BackupCreateSequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.BackupDestroySequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.BackupDetachLogicalNodeOperation;
import com.noctarius.snowcast.impl.operations.CreateSequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.DestroySequencerDefinitionOperation;
import com.noctarius.snowcast.impl.operations.DestroySequencerOperation;
import com.noctarius.snowcast.impl.operations.DetachLogicalNodeOperation;
import com.noctarius.snowcast.impl.operations.SequencerReplicationOperation;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class SequencerDataSerializerHook
        implements DataSerializerHook {

    public static final int FACTORY_ID;
    public static final int SEQUENCER_DEFINITION_FACTORY_ID;

    public static final int TYPE_ATTACH_LOGICAL_NODE = 0;
    public static final int TYPE_DETACH_LOGICAL_NODE = 1;
    public static final int TYPE_CREATE_SEQUENCER_DEFINITION = 2;
    public static final int TYPE_DESTROY_SEQUENCER_DEFINITION = 3;
    public static final int TYPE_DESTROY_SEQUENCER = 4;
    public static final int TYPE_REPLICATION_OPERATION = 5;
    public static final int TYPE_PARTITION_REPLICATION = 6;
    public static final int TYPE_BACKUP_CREATE_SEQUENCER_DEFINITION = 7;
    public static final int TYPE_BACKUP_DESTROY_SEQUENCER_DEFINITION = 8;
    public static final int TYPE_BACKUP_ATTACH_LOGICAL_NODE = 9;
    public static final int TYPE_BACKUP_DETACH_LOGICAL_NODE = 10;

    private static final int LEN = TYPE_BACKUP_DETACH_LOGICAL_NODE + 1;

    private static final int DEFAULT_FACTORY_ID = 78412;

    static {
        FACTORY_ID = Integer.getInteger("com.noctarius.snowcast.factoryid", DEFAULT_FACTORY_ID);
        SEQUENCER_DEFINITION_FACTORY_ID = FACTORY_ID + 1;
    }

    @Override
    @Nonnegative
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Nonnull
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[TYPE_ATTACH_LOGICAL_NODE] = id -> new AttachLogicalNodeOperation();
        constructors[TYPE_DETACH_LOGICAL_NODE] = id -> new DetachLogicalNodeOperation();
        constructors[TYPE_CREATE_SEQUENCER_DEFINITION] = id -> new CreateSequencerDefinitionOperation();
        constructors[TYPE_DESTROY_SEQUENCER_DEFINITION] = id -> new DestroySequencerDefinitionOperation();
        constructors[TYPE_DESTROY_SEQUENCER] = id -> new DestroySequencerOperation();
        constructors[TYPE_REPLICATION_OPERATION] = id -> new SequencerReplicationOperation();
        constructors[TYPE_PARTITION_REPLICATION] = id -> new PartitionReplication();
        constructors[TYPE_BACKUP_CREATE_SEQUENCER_DEFINITION] = id -> new BackupCreateSequencerDefinitionOperation();
        constructors[TYPE_BACKUP_DESTROY_SEQUENCER_DEFINITION] = id -> new BackupDestroySequencerDefinitionOperation();
        constructors[TYPE_BACKUP_ATTACH_LOGICAL_NODE] = id -> new BackupAttachLogicalNodeOperation();
        constructors[TYPE_BACKUP_DETACH_LOGICAL_NODE] = id -> new BackupDetachLogicalNodeOperation();
        return new ArrayDataSerializableFactory(constructors);
    }
}
