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
package com.noctarius.snowcast;

/**
 * This enumeration describes the internal states of the
 * {@link com.noctarius.snowcast.SnowcastSequencer} state machine. Those states
 * define either a ready-to-use, non-assigned or destroyed sequencer. Please
 * refer to the documentation of the different states for details.
 */
public enum SnowcastSequenceState {

    /**
     * A {@link com.noctarius.snowcast.SnowcastSequencer} in the state <tt>Detached</tt>
     * is not destroyed but <b>cannot</b> be used to generate IDs since there is no
     * logical node id assigned.
     */
    Detached,

    /**
     * A {@link com.noctarius.snowcast.SnowcastSequencer} in the state <tt>Attached</tt>
     * has a logical node id assigned and can be used to generate IDs.<br>
     * This is the <b>default</b> state after acquiring the sequencer for the first time!
     */
    Attached,

    /**
     * A {@link com.noctarius.snowcast.SnowcastSequencer} in the state <tt>Destroyed</tt>
     * does not have a legal configuration anymore. This instance <b>can never ever</b> be
     * used again to generate IDs. A sequencer with the same referral name might be created
     * again at that point.
     */
    Destroyed
}
