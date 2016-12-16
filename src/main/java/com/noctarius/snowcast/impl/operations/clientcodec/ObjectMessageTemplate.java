package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.annotation.EventResponse;
import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Response;
import com.hazelcast.client.impl.protocol.constants.EventMessageConst;
import com.hazelcast.nio.serialization.Data;
import com.noctarius.snowcast.impl.SequencerDefinition;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

@GenerateCodec(id = 0, name = "response", ns = "")
public interface ObjectMessageTemplate {

    @Response(ObjectMessageConstants.SEQUENCER_DEFINITION)
    SequencerDefinition SequencerDefinitionSequencerDefinition(@Nonnull String sequencerName, @Nonnull long epochOffset,
                                                               @Min(128) @Max(8192) int maxLogicalNodeCount,
                                                               @Nonnegative @Max(Short.MAX_VALUE) int backupCount);

    @EventResponse(EventMessageConst.EVENT_TOPIC)
    void Topic(@Nonnull Data item, @Nonnegative long publishTime, @Nonnull String uuid);

}
