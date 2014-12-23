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

import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;

public class NodeSequencer
        implements InternalSequencer {

    private final NodeSequencerContext sequencerContext;

    NodeSequencer(NodeSequencerService service, SequencerDefinition definition) {
        this.sequencerContext = new NodeSequencerContext(service, definition);
    }

    @Override
    public String getSequencerName() {
        return sequencerContext.getSequencerName();
    }

    @Override
    public long next()
            throws InterruptedException {

        return sequencerContext.next();
    }

    @Override
    public SnowcastSequenceState getSequencerState() {
        return sequencerContext.getSequencerState();
    }

    @Override
    public final SnowcastSequencer attachLogicalNode() {
        sequencerContext.attachLogicalNode();
        return this;
    }

    @Override
    public final SnowcastSequencer detachLogicalNode() {
        sequencerContext.detachLogicalNode();
        return this;
    }

    @Override
    public long timestampValue(long sequenceId) {
        return sequencerContext.timestampValue(sequenceId);
    }

    @Override
    public int logicalNodeId(long sequenceId) {
        return sequencerContext.logicalNodeId(sequenceId);
    }

    @Override
    public int counterValue(long sequenceId) {
        return sequencerContext.counterValue(sequenceId);
    }

    @Override
    public void stateTransition(SnowcastSequenceState newState) {
        sequencerContext.stateTransition(newState);
    }

    private static class NodeSequencerContext
            extends AbstractSequencerContext {

        private final NodeSequencerService service;

        private NodeSequencerContext(NodeSequencerService service, SequencerDefinition definition) {
            super(definition);
            this.service = service;
        }

        @Override
        protected int doAttachLogicalNode(SequencerDefinition definition) {
            return service.attachSequencer(definition);
        }

        @Override
        protected void doDetachLogicalNode(String sequencerName, int logicalNodeId) {
            service.detachSequencer(sequencerName, logicalNodeId);
        }
    }
}
