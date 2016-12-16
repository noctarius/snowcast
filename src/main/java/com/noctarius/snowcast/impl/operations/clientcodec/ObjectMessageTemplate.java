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
