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

import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.util.ConstructorFunction;
import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.noctarius.snowcast.impl.ExceptionMessages.ILLEGAL_SEQUENCER_TYPE;
import static com.noctarius.snowcast.impl.ExceptionUtils.exception;

class ClientSequencerService
        implements SequencerService {

    private static final Tracer TRACER = TracingUtils.tracer(ClientSequencerService.class);

    private final ConstructorFunction<SequencerDefinition, SequencerProvision> sequencerConstructor;

    private final ClientCodec clientCodec;

    private final ConcurrentMap<String, SequencerProvision> provisions;

    ClientSequencerService(@Nonnull ProxyManager proxyManager, @Nonnull ClientCodec clientCodec) {
        if (InternalSequencerUtils.getHazelcastVersion() == SnowcastConstants.HazelcastVersion.V_3_9) {
            this.sequencerConstructor = new ClientSequencerConstructorFunction39(proxyManager, this, clientCodec);
        } else {
            this.sequencerConstructor = new ClientSequencerConstructorFunction(proxyManager, this, clientCodec);
        }
        this.provisions = new ConcurrentHashMap<>();
        this.clientCodec = clientCodec;
    }

    @Nonnull
    @Override
    public SnowcastSequencer createSequencer(@Nonnull String sequencerName, @Nonnull SnowcastEpoch epoch,
                                             @Min(128) @Max(8192) int maxLogicalNodeCount,
                                             @Nonnegative @Max(Short.MAX_VALUE) short backupCount) {

        TRACER.trace("register sequencer %s with epoch %s, max nodes %s, backups %s", //
                sequencerName, epoch, maxLogicalNodeCount, backupCount);

        SequencerDefinition definition = new SequencerDefinition(sequencerName, epoch, maxLogicalNodeCount, backupCount);

        try {
            SequencerDefinition realDefinition = clientCodec.createSequencerDefinition(sequencerName, definition);
            return getOrCreateSequencerProvision(realDefinition).getSequencer();
        } finally {
            TRACER.trace("register sequencer %s end", sequencerName);
        }
    }

    @Override
    public void destroySequencer(@Nonnull SnowcastSequencer sequencer) {
        if (!(sequencer instanceof ClientSequencer)) {
            throw exception(ILLEGAL_SEQUENCER_TYPE);
        }

        ((InternalSequencer) sequencer).stateTransition(SnowcastSequenceState.Destroyed);

        String sequencerName = sequencer.getSequencerName();
        TRACER.trace("destroy sequencer %s", sequencerName);

        try {
            clientCodec.destroySequencerDefinition(sequencerName);
        } finally {
            TRACER.trace("destroy sequencer %s end", sequencerName);
        }
    }

    @Nonnull
    private SequencerProvision getOrCreateSequencerProvision(@Nonnull SequencerDefinition definition) {
        String sequencerName = definition.getSequencerName();

        SequencerProvision provision = provisions.computeIfAbsent(sequencerName, name -> {
            TRACER.trace("return and cache new sequencer instance for %s", sequencerName);
            return sequencerConstructor.createNew(definition);
        });

        TRACER.trace("return existing sequencer instance for %s", sequencerName);
        return provision;
    }
}
