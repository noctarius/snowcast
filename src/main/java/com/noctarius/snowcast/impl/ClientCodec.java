package com.noctarius.snowcast.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SnowcastAttachLogicalNodeCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastCreateSequencerDefinitionCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastDestroySequencerDefinitionCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastDetachLogicalNodeCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastRegisterChannelCodec;
import com.hazelcast.client.impl.protocol.codec.SnowcastRemoveChannelCodec;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastException;

import javax.annotation.Nonnull;

final class ClientCodec {

    private final ClientInvocator clientInvocator;
    private final PartitionService partitionService;

    ClientCodec(HazelcastClientInstanceImpl client, ClientInvocator clientInvocator) {
        this.clientInvocator = clientInvocator;
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
        try {
            int partitionId = partitionId(sequencerName);
            return clientInvocator.invoke(partitionId, request).get();
        } catch (Exception e) {
            throw new SnowcastException(e);
        }
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
