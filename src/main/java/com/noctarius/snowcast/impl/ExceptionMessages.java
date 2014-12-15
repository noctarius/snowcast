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

public enum ExceptionMessages {
    /**
     * Sequencer is already registered but configuration does not match
     */
    SEQUENCER_ALREADY_REGISTERED("Sequencer is already registered but configuration does not match"),

    /**
     * maxLogicalNodeCount cannot be %s than %s
     */
    ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY("maxLogicalNodeCount cannot be %s than %s"),

    /**
     * Illegal detach attempt, address on slot is wrong
     */
    ILLEGAL_DETACH_ATTEMPT("Illegal detach attempt, address on slot is wrong"),

    /**
     * Failed to register Snowcast service lazily
     */
    SERVICE_REGISTRATION_FAILED("Failed to register Snowcast service lazily"),

    /**
     * Failed to retrieve NodeEngine from HazelcastInstance, not a proxy?
     */
    RETRIEVE_NODE_ENGINE_FAILED("Failed to retrieve NodeEngine from HazelcastInstance, not a proxy?"),

    /**
     * Cannot detach the sequencer %s, not in attached state
     */
    SEQUENCER_WRONG_STATE_CANNOT_DETACH("Cannot detach the sequencer %s, not in attached state"),

    /**
     * Cannot attach the sequencer %s, not in detached state
     */
    SEQUENCER_WRONG_STATE_CANNOT_ATTACH("Cannot attach the sequencer %s, not in detached state"),

    /**
     * Sequencer %s is already destroyed
     */
    SEQUENCER_ALREADY_DESTROYED("Sequencer %s is already destroyed"),

    /**
     * Sequencer %s is not yet assigned to a logical node ID
     */
    SEQUENCER_NOT_ASSIGNED("Sequencer %s is not yet assigned to a logical node id"),

    /**
     * Sequencer %s is in wrong state, %s expected but %s found
     */
    SEQUENCER_IN_WRONG_STATE("Sequencer %s is in wrong state, %s expected but %s found"),

    /**
     * Parameter %s is not supported
     */
    PARAMETER_IS_NOT_SUPPORTED("Parameter %s is not supported");

    private final String template;

    private ExceptionMessages(String template) {
        this.template = template;
    }

    public String buildMessage() {
        return template;
    }

    public String buildMessage(Object arg1) {
        return String.format(template, arg1);
    }

    public String buildMessage(Object arg1, Object arg2) {
        return String.format(template, arg1, arg2);
    }

    public String buildMessage(Object arg1, Object arg2, Object arg3) {
        return String.format(template, arg1, arg2, arg3);
    }
}
