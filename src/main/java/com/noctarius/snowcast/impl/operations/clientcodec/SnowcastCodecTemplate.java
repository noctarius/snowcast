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
package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.noctarius.snowcast.impl.SequencerDefinition;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

@GenerateCodec(id = 100, name = "Snowcast", ns = "snowcast")
public interface SnowcastCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.INTEGER)
    int attachLogicalNode(@Nonnull String sequencerName, @Nonnull long epochOffset, @Min(128) @Max(8192) int maxLogicalNodeCount,
                          @Nonnegative @Max(Short.MAX_VALUE) int backupCount);

    @Request(id = 2, retryable = false, response = ObjectMessageConstants.SEQUENCER_DEFINITION)
    SequencerDefinition createSequencerDefinition(@Nonnull String sequencerName, @Nonnull long epochOffset,
                                                  @Min(128) @Max(8192) int maxLogicalNodeCount,
                                                  @Nonnegative @Max(Short.MAX_VALUE) int backupCount);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.BOOLEAN)
    boolean destroySequencerDefinition(@Nonnull String sequencerName);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    boolean detachLogicalNode(@Nonnull String sequencerName, @Nonnull long epochOffset,
                              @Min(128) @Max(8192) int maxLogicalNodeCount, @Nonnegative @Max(Short.MAX_VALUE) int backupCount,
                              int logicalNodeId);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.STRING, event = {EventMessageConst.EVENT_TOPIC})
    String registerChannel(@Nonnull String sequencerName);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    boolean removeChannel(@Nonnull String sequencerName, @Nonnull String registrationId);

}