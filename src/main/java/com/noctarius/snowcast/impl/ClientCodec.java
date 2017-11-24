/*
 * Copyright (c) 2015-2017, Christoph Engelbert (aka noctarius) and
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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.*;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.noctarius.snowcast.SnowcastEpoch;

import javax.annotation.Nonnull;

final class ClientCodec {

    private final HazelcastClientInstanceImpl client;
    private final PartitionService partitionService;

    ClientCodec(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.partitionService = client.getPartitionService();
    }

    int attachLogicalNode(@Nonnull String sequencerName, @Nonnull SequencerDefinition sequencerDefinition) {
        long epochOffset = sequencerDefinition.getEpoch().getEpochOffset();
        int maxLogicalNodeCount = sequencerDefinition.getMaxLogicalNodeCount();
        short backupCount = sequencerDefinition.getBackupCount();

        ClientMessage request = SnowcastAttachLogicalNodeCodec
                .encodeRequest(sequencerName, epochOffset, maxLogicalNodeCount, backupCount);

        ClientMessage response = invoke(sequencerName, request);
        return SnowcastAttachLogicalNodeCodec.decodeResponse(response).response;
    }

    SequencerDefinition createSequencerDefinition(@Nonnull String sequencerName,
                                                  @Nonnull SequencerDefinition sequencerDefinition) {

        long epochOffset = sequencerDefinition.getEpoch().getEpochOffset();
        int maxLogicalNodeCount = sequencerDefinition.getMaxLogicalNodeCount();
        short backupCount = sequencerDefinition.getBackupCount();

        ClientMessage request = SnowcastCreateSequencerDefinitionCodec
                .encodeRequest(sequencerName, epochOffset, maxLogicalNodeCount, backupCount);

        ClientMessage response = invoke(sequencerName, request);
        return decodeSequencerDefinition(response);
    }

    boolean destroySequencerDefinition(@Nonnull String sequencerName) {
        ClientMessage request = SnowcastDestroySequencerDefinitionCodec.encodeRequest(sequencerName);
        ClientMessage response = invoke(sequencerName, request);
        return SnowcastDestroySequencerDefinitionCodec.decodeResponse(response).response;
    }

    boolean detachLogicalNode(@Nonnull String sequencerName, @Nonnull SequencerDefinition sequencerDefinition,
                              int logicalNodeId) {

        long epochOffset = sequencerDefinition.getEpoch().getEpochOffset();
        int maxLogicalNodeCount = sequencerDefinition.getMaxLogicalNodeCount();
        short backupCount = sequencerDefinition.getBackupCount();

        ClientMessage request = SnowcastDetachLogicalNodeCodec
                .encodeRequest(sequencerName, epochOffset, maxLogicalNodeCount, backupCount, logicalNodeId);

        ClientMessage response = invoke(sequencerName, request);
        return SnowcastDetachLogicalNodeCodec.decodeResponse(response).response;
    }

    String registerChannel(@Nonnull String sequencerName) {
        ClientMessage request = SnowcastRegisterChannelCodec.encodeRequest(sequencerName);
        ClientMessage response = invoke(sequencerName, request);
        return SnowcastRegisterChannelCodec.decodeResponse(response).response;
    }

    boolean removeChannel(@Nonnull String sequencerName, @Nonnull String registrationId) {
        ClientMessage request = SnowcastRemoveChannelCodec.encodeRequest(sequencerName, registrationId);
        ClientMessage response = invoke(sequencerName, request);
        return SnowcastRemoveChannelCodec.decodeResponse(response).response;
    }

    private int partitionId(@Nonnull String sequencerName) {
        Partition partition = partitionService.getPartition(sequencerName);
        return partition.getPartitionId();
    }

    private ClientMessage invoke(@Nonnull String sequencerName, @Nonnull ClientMessage request) {
        return ExceptionUtils.execute(() -> {
            int partitionId = partitionId(sequencerName);
            ClientInvocation clientInvocation = new ClientInvocation(client, request, "snowcast", partitionId);
            return clientInvocation.invoke().get();
        });
    }

    private SequencerDefinition decodeSequencerDefinition(ClientMessage response) {
        SnowcastCreateSequencerDefinitionCodec.ResponseParameters responseParameters = //
                SnowcastCreateSequencerDefinitionCodec.decodeResponse(response);

        String sequencerName = responseParameters.sequencerName;
        int maxLogicalNodeCount = responseParameters.maxLogicalNodeCount;
        long epochOffset = responseParameters.epochOffset;
        SnowcastEpoch epoch = SnowcastEpoch.byTimestamp(epochOffset);
        short backupCount = (short) responseParameters.backupCount;

        return new SequencerDefinition(sequencerName, epoch, maxLogicalNodeCount, backupCount);
    }
}
