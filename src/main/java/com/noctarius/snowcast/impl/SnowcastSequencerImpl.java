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

import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastException;
import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;
import com.noctarius.snowcast.SnowcastStateException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class SnowcastSequencerImpl
        implements SnowcastSequencer {

    private static final int INCREMENT_RETRY_TIMEOUT_NANOS = 100;

    // ID Shifting values
    private static final int SHIFT_TIMESTAMP = 23;
    private static final int BASE_SHIFT_LOGICAL_NODE_ID = 10;

    // Exponent for power of 2 lookup
    private static final int[] EXP_LOOKUP = {8192, 4096, 2048, 1024, 512, 256, 128};

    private static final AtomicReferenceFieldUpdater<SnowcastSequencerImpl, SnowcastSequenceState> STATE_FIELD_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(SnowcastSequencerImpl.class, SnowcastSequenceState.class, "state");

    private static final AtomicLongFieldUpdater<SnowcastSequencerImpl> LAST_TIMESTAMP_FIELD_UPDATER = AtomicLongFieldUpdater
            .newUpdater(SnowcastSequencerImpl.class, "lastTimestamp");

    private static final AtomicIntegerFieldUpdater<SnowcastSequencerImpl> COUNTER_FIELD_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(SnowcastSequencerImpl.class, "counter");

    private final SequencerService service;
    private final SequencerDefinition definition;
    private final String sequencerName;
    private final SnowcastEpoch epoch;

    private final int shiftLogicalNodeId;
    private final int maxMillisCounter;

    private volatile SnowcastSequenceState state = SnowcastSequenceState.Detached;
    private volatile int logicalNodeId;

    // Fields only accessed or written through field updaters
    private volatile int counter;
    private volatile long lastTimestamp;

    public SnowcastSequencerImpl(SequencerService service, SequencerDefinition definition) {
        this.service = service;
        this.definition = definition;
        this.sequencerName = definition.getSequencerName();
        this.epoch = definition.getEpoch();
        this.shiftLogicalNodeId = calculateLogicalNodeShifting(definition.getMaxLogicalNodeCount());
        this.maxMillisCounter = calculateMaxMillisCounter(shiftLogicalNodeId);
    }

    @Override
    public String getSequencerName() {
        return sequencerName;
    }

    @Override
    public long next() {
        int logicalNodeID = checkStateAndLogicalNodeId();
        long timestamp = epoch.getEpochTimestamp();
        int nextId = nextId(timestamp);

        long id = timestamp << SHIFT_TIMESTAMP;
        id |= logicalNodeID << shiftLogicalNodeId;
        id |= nextId;
        return id;
    }

    @Override
    public SnowcastSequenceState getSequencerState() {
        return state;
    }

    @Override
    public SnowcastSequencer attachLogicalNode() {
        // Will fail if state transition is not allowed
        stateTransition(SnowcastSequenceState.Attached);

        // Request sequencer remote assignment
        logicalNodeId = service.attachSequencer(definition);

        return this;
    }

    @Override
    public SnowcastSequencer detachLogicalNode() {
        // Will fail if state transition is not allowed
        stateTransition(SnowcastSequenceState.Detached);

        int logicalNodeId = this.logicalNodeId;
        this.logicalNodeId = -1;

        // Remove sequencer remote assignment
        service.detachSequencer(sequencerName, logicalNodeId);

        return this;
    }

    void stateTransition(SnowcastSequenceState newState) {
        while (true) {
            SnowcastSequenceState state = this.state;
            if (state == newState) {
                return;
            }

            if (newState == SnowcastSequenceState.Detached) {
                if (state != SnowcastSequenceState.Attached) {
                    String message = ExceptionMessages.SEQUENCER_WRONG_STATE_CANNOT_DETACH.buildMessage(sequencerName);
                    throw new SnowcastStateException(message);
                }

            } else if (newState == SnowcastSequenceState.Attached) {
                if (state != SnowcastSequenceState.Detached) {
                    String message = ExceptionMessages.SEQUENCER_WRONG_STATE_CANNOT_ATTACH.buildMessage(sequencerName);
                    throw new SnowcastStateException(message);
                }
            } else {
                if (state == SnowcastSequenceState.Destroyed) {
                    return;
                }
            }

            if (STATE_FIELD_UPDATER.compareAndSet(this, state, newState)) {
                return;
            }
        }
    }

    private int nextId(long timestamp) {
        while (true) {
            checkAndUpdateTimestamp(timestamp);

            int id = increment();
            if (id != -1) {
                return id;
            }

            try {
                TimeUnit.NANOSECONDS.sleep(INCREMENT_RETRY_TIMEOUT_NANOS);
            } catch (Exception e) {
                throw new SnowcastException(e);
            }
        }
    }

    private void checkAndUpdateTimestamp(long timestamp) {
        while (true) {
            long lastTimestamp = this.lastTimestamp;
            if (lastTimestamp < timestamp) {
                if (LAST_TIMESTAMP_FIELD_UPDATER.compareAndSet(this, lastTimestamp, timestamp)) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    private int increment() {
        int nextId = COUNTER_FIELD_UPDATER.incrementAndGet(this);
        return nextId > maxMillisCounter ? -1 : nextId;
    }

    private int checkStateAndLogicalNodeId() {
        int logicalNodeId = this.logicalNodeId;
        if (logicalNodeId == -1) {
            String message = ExceptionMessages.SEQUENCER_NOT_ASSIGNED.buildMessage(sequencerName);
            throw new SnowcastStateException(message);
        }
        SnowcastSequenceState state = this.state;
        if (state != SnowcastSequenceState.Attached) {
            String message = ExceptionMessages.SEQUENCER_IN_WRONG_STATE
                    .buildMessage(sequencerName, SnowcastSequenceState.Attached, state);
            throw new SnowcastStateException(message);
        }
        return logicalNodeId;
    }

    private int calculateLogicalNodeShifting(int maxLogicalNodeCount) {
        int exp = BASE_SHIFT_LOGICAL_NODE_ID;
        for (int matcher : EXP_LOOKUP) {
            if (matcher == maxLogicalNodeCount) {
                break;
            }
            exp++;
        }
        return exp;
    }

    private int calculateMaxMillisCounter(int shiftLogicalNodeId) {
        return (int) Math.pow(2, shiftLogicalNodeId);
    }
}
