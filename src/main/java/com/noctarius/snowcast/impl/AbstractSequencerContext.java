package com.noctarius.snowcast.impl;

import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastStateException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateCounterMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeShifting;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateMaxMillisCounter;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.generateSequenceId;
import static com.noctarius.snowcast.impl.SnowcastConstants.INCREMENT_RETRY_TIMEOUT_NANOS;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_COUNTER;
import static com.noctarius.snowcast.impl.SnowcastConstants.TC_COUNTER_READ_MASK;
import static com.noctarius.snowcast.impl.SnowcastConstants.TC_TIMESTAMP_READ_MASK;

abstract class AbstractSequencerContext {

    private static final AtomicReferenceFieldUpdater<AbstractSequencerContext, SnowcastSequenceState> STATE_UPDATER;

    private static final AtomicLongFieldUpdater<AbstractSequencerContext> TIMESTAMP_AND_COUNTER_UPDATER;

    static {
        STATE_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(AbstractSequencerContext.class, SnowcastSequenceState.class, "state");
        TIMESTAMP_AND_COUNTER_UPDATER = AtomicLongFieldUpdater.newUpdater(AbstractSequencerContext.class, "timestampAndCounter");
    }

    private final SequencerDefinition definition;
    private final String sequencerName;
    private final SnowcastEpoch epoch;

    private final int nodeIdShiftFactor;
    private final int maxMillisCounter;

    private final long logicalNodeIdReadMask;
    private final long counterReadMask;

    private volatile SnowcastSequenceState state = SnowcastSequenceState.Detached;

    // Holds the currently assigned logical node id
    private volatile int logicalNodeId = -1;

    // This field is only accessed or written through the field updater
    private volatile long timestampAndCounter;

    protected AbstractSequencerContext(SequencerDefinition definition) {
        this.definition = definition;
        this.sequencerName = definition.getSequencerName();
        this.epoch = definition.getEpoch();

        int maxLogicalNodeCount = definition.getMaxLogicalNodeCount();
        this.nodeIdShiftFactor = calculateLogicalNodeShifting(maxLogicalNodeCount);
        this.logicalNodeIdReadMask = calculateLogicalNodeMask(maxLogicalNodeCount, nodeIdShiftFactor);
        this.counterReadMask = calculateCounterMask(maxLogicalNodeCount, nodeIdShiftFactor);
        this.maxMillisCounter = calculateMaxMillisCounter(nodeIdShiftFactor);

        // Just to prevent the "never-written" warning
        this.timestampAndCounter = 0;
    }

    final String getSequencerName() {
        return sequencerName;
    }

    final long next()
            throws InterruptedException {

        int logicalNodeID = checkStateAndLogicalNodeId();
        long timestamp = epoch.getEpochTimestamp();

        int nextId;
        while (true) {
            checkAndUpdateTimestamp(timestamp);

            nextId = increment(timestamp);
            if (nextId != -1) {
                break;
            }

            TimeUnit.NANOSECONDS.sleep(INCREMENT_RETRY_TIMEOUT_NANOS);
            timestamp = epoch.getEpochTimestamp();
        }

        return generateSequenceId(timestamp, logicalNodeID, nextId, nodeIdShiftFactor);
    }

    final SnowcastSequenceState getSequencerState() {
        return state;
    }

    final void attachLogicalNode() {
        // Will fail if state transition is not allowed
        stateTransition(SnowcastSequenceState.Attached);

        // Request sequencer remote assignment
        this.logicalNodeId = doAttachLogicalNode(definition);
    }

    final void detachLogicalNode() {
        // Will fail if state transition is not allowed
        stateTransition(SnowcastSequenceState.Detached);

        int logicalNodeId = this.logicalNodeId;
        this.logicalNodeId = -1;

        // Remove sequencer remote assignment
        doDetachLogicalNode(sequencerName, logicalNodeId);
    }

    final long timestampValue(long sequenceId) {
        return InternalSequencerUtils.timestampValue(sequenceId);
    }

    final int logicalNodeId(long sequenceId) {
        return InternalSequencerUtils.logicalNodeId(sequenceId, nodeIdShiftFactor, logicalNodeIdReadMask);
    }

    final int counterValue(long sequenceId) {
        return InternalSequencerUtils.counterValue(sequenceId, counterReadMask);
    }

    protected abstract int doAttachLogicalNode(SequencerDefinition definition);

    protected abstract void doDetachLogicalNode(String sequencerName, int logicalNodeId);

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

            if (STATE_UPDATER.compareAndSet(this, state, newState)) {
                return;
            }
        }
    }

    private void checkAndUpdateTimestamp(long timestamp) {
        while (true) {
            long timestampAndCounter = this.timestampAndCounter;
            long lastTimestamp = timestampAndCounter & TC_TIMESTAMP_READ_MASK;
            if (lastTimestamp < timestamp) {
                if (TIMESTAMP_AND_COUNTER_UPDATER.compareAndSet(this, timestampAndCounter, timestamp)) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    private int increment(long expectedTimestamp) {
        while (true) {
            long timestampAndCounter = this.timestampAndCounter;

            // Extract values
            long counter = (timestampAndCounter & TC_COUNTER_READ_MASK) >> SHIFT_COUNTER;
            long timestamp = timestampAndCounter & TC_TIMESTAMP_READ_MASK;

            if (expectedTimestamp != timestamp) {
                return -1;
            }

            // Increment the counter
            counter++;

            // Exceeded the counters value for a single millisecond
            if (counter > maxMillisCounter) {
                return -1;
            }

            // Build the new combined timestamp and counter value
            long newTC = timestamp | (counter << SHIFT_COUNTER);
            if (TIMESTAMP_AND_COUNTER_UPDATER.compareAndSet(this, timestampAndCounter, newTC)) {
                return (int) counter;
            }
        }
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
}
