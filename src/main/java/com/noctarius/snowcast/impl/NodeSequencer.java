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

import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class NodeSequencer
        implements InternalSequencer {

    private final NodeSequencerContext sequencerContext;

    NodeSequencer(@Nonnull NodeSequencerService service, @Nonnull SequencerDefinition definition) {
        this.sequencerContext = new NodeSequencerContext(service, definition);
    }

    @Nonnull
    @Override
    public String getSequencerName() {
        return sequencerContext.getSequencerName();
    }

    @Override
    @Nonnegative
    public long next()
            throws InterruptedException {

        return sequencerContext.next();
    }

    @Nonnull
    @Override
    public SnowcastSequenceState getSequencerState() {
        return sequencerContext.getSequencerState();
    }

    @Nonnull
    @Override
    public SequencerDefinition getSequencerDefinition() {
        return sequencerContext.getSequencerDefinition();
    }

    @Nonnull
    @Override
    public final SnowcastSequencer attachLogicalNode() {
        sequencerContext.attachLogicalNode();
        return this;
    }

    @Nonnull
    @Override
    public final SnowcastSequencer detachLogicalNode() {
        sequencerContext.detachLogicalNode();
        return this;
    }

    @Override
    @Nonnegative
    public long timestampValue(long sequenceId) {
        return sequencerContext.timestampValue(sequenceId);
    }

    @Override
    @Nonnegative
    public int logicalNodeId(long sequenceId) {
        return sequencerContext.logicalNodeId(sequenceId);
    }

    @Override
    @Nonnegative
    public int counterValue(long sequenceId) {
        return sequencerContext.counterValue(sequenceId);
    }

    @Override
    public void stateTransition(@Nonnull SnowcastSequenceState newState) {
        sequencerContext.stateTransition(newState);
    }

    @Nonnull
    @Override
    public SequencerService getSequencerService() {
        return sequencerContext.service;
    }

    private static class NodeSequencerContext
            extends AbstractSequencerContext {

        private final NodeSequencerService service;

        private NodeSequencerContext(@Nonnull NodeSequencerService service, @Nonnull SequencerDefinition definition) {
            super(definition);
            this.service = service;
        }

        @Min(128)
        @Override
        @Max(8192)
        protected int doAttachLogicalNode(@Nonnull SequencerDefinition definition) {
            return service.attachSequencer(definition);
        }

        @Override
        protected void doDetachLogicalNode(@Nonnull SequencerDefinition definition, @Min(128) @Max(8192) int logicalNodeId) {
            service.detachSequencer(definition, logicalNodeId);
        }
    }
}
