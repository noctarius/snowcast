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
package com.noctarius.snowcast;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * <p>The SnowcastSequencer interface describes the general sequencer contract. This
 * interface will be backed by different implementations based on client or server
 * mode but is fully transparent to the end user.</p>
 * <p>The sequencer is used to generate the snowcast sequence ID and can also be used to
 * extract values from previously generated IDs based on the sequencers configuration
 * (maximal number of logical node IDs and max counter).</p>
 * <p>The SnowcastSequencer is acquired by a call to
 * {@link com.noctarius.snowcast.Snowcast#createSequencer(String, SnowcastEpoch)} or
 * {@link com.noctarius.snowcast.Snowcast#createSequencer(String, SnowcastEpoch, int)}
 * depending on the user preference or need.</p>
 * <p>A SnowcastSequencer implementation is expected to be fully thread-safe. Creation
 * as well as all methods need to be aware of concurrent use and multi-threaded access
 * or instance sharing!</p>
 */
@ThreadSafe
public interface SnowcastSequencer {

    /**
     * Returns the reference name of the sequencer. This name is shared in the Hazelcast
     * cluster. Whenever the same sequencer name is referenced when creating a
     * SnowcastSequencer the same internal sequencer instance is returned (unless destroyed).
     *
     * @return the sequencers reference name
     */
    @Nonnull
    String getSequencerName();

    /**
     * <p>Generates and returns the next snowcast sequence ID. This call blocks until a ID is
     * free to be generated. The call blocks whenever a millisecond on the current node runs
     * out of possible IDs by exceeding the maximum counter value.</p>
     * <p>This method is fully thread-safe and expected to be used in multi-threading
     * environments.</p>
     *
     * @return the next available snowcast sequence ID
     * @throws InterruptedException if the blocking wait is interrupted
     */
    long next()
            throws InterruptedException;

    /**
     * Returns the current {@link com.noctarius.snowcast.SnowcastSequenceState} of this
     * sequencer. It might be attached, detached or destroyed. Please refer to the state
     * documentations to find out more about their meanings.
     *
     * @return the sequencers current state
     */
    @Nonnull
    SnowcastSequenceState getSequencerState();

    /**
     * Attaches the current SnowcastSequencer instance to a logical node ID. This is
     * a cluster operation and blocks until the owner of the reference name assigns
     * a logical node ID and returns it back to the current node or client.
     *
     * @return the current SnowcastSequencer instance for chaining of method calls
     */
    @Nonnull
    SnowcastSequencer attachLogicalNode();

    /**
     * Detaches the current SnowcastSequencer instance from the currently assigned
     * logical node ID. This is a cluster operation and blocks until the owner of
     * the reference name removed the logical node ID assignment and returns back
     * to the current node or client.
     *
     * @return the current SnowcastSequencer instance for chaining of method calls
     */
    @Nonnull
    SnowcastSequencer detachLogicalNode();

    /**
     * Extracts the timestamp value of the given snowcast sequence ID based on the
     * current configuration. Extracted timestamps are based on the configured
     * {@link com.noctarius.snowcast.SnowcastEpoch}. To get the real world point in
     * time the epoch value plus this returned timestamp must be summed together.
     *
     * @param sequenceId the sequence ID to extract the timestamp from
     * @return the extracted timestamp based on the configured epoch
     */
    @Nonnegative
    long timestampValue(long sequenceId);

    /**
     * Extracts the logical node ID of the given snowcast sequence ID based on the
     * current configuration. The extracted logical node ID is based no the configured
     * maximal logical node ID (number of bits used).
     *
     * @param sequenceId the sequence ID to extract the logical node ID from
     * @return the extracted logical node ID based on the configured maximal logical node ID
     */
    @Nonnegative
    int logicalNodeId(long sequenceId);

    /**
     * Extracts the counter value of the given snowcast sequence ID based on the
     * current configuration. The extracted counter value is based no the configured
     * maximal logical node ID (number of bits used).
     *
     * @param sequenceId the sequence ID to extract the counter value from
     * @return the extracted counter value based on the configured maximal logical node ID
     */
    @Nonnegative
    int counterValue(long sequenceId);
}
