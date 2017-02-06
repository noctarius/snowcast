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
     * maxLogicalNodeCount has an illegal value
     */
    ILLEGAL_MAX_LOGICAL_NODE_COUNT("maxLogicalNodeCount has an illegal value"),

    /**
     * Illegal detach attempt, address on slot is wrong
     */
    ILLEGAL_DETACH_ATTEMPT("Illegal detach attempt, address on slot is wrong"),

    /**
     * snowcast service is not registered
     */
    SERVICE_NOT_REGISTERED("snowcast service is not registered"),

    /**
     * Failed to retrieve NodeEngine from HazelcastInstance, not a proxy?
     */
    RETRIEVE_NODE_ENGINE_FAILED("Failed to retrieve NodeEngine from HazelcastInstance, not a proxy?"),

    /**
     * Failed to retrieve NodeEngine from HazelcastInstance, not a proxy?
     */
    RETRIEVE_CLIENT_ENGINE_FAILED("Failed to retrieve internal client from HazelcastInstance, not a proxy?"),

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
    PARAMETER_IS_NOT_SUPPORTED("Parameter %s is not supported"),

    /**
     * Illegal Sequencer type passed to the Snowcast::destroy method
     */
    ILLEGAL_SEQUENCER_TYPE("Illegal Sequencer type passed to the Snowcast::destroy method"),

    /**
     * Error on merging LogicalNodeTable on partition %s. Partitions seem to be out of sync - stopping
     */
    ERROR_MERGING_LOGICAL_NODE_TABLE(
            "Error on merging LogicalNodeTable on partition %s. Partitions seem to be out of sync - stopping"),

    /**
     * Backup on partition %s is out of sync. Backup cannot be applied.
     */
    BACKUP_OUT_OF_SYNC("Backup on partition %s is out of sync. Backup cannot be applied."),

    /**
     * data type scale not a power of two
     */
    DATA_NOT_POWER_OF_TWO("data type scale not a power of two"),

    /**
     * Partition %s is frozen and cannot be changed!
     */
    PARTITION_IS_FROZEN("Partition %s is frozen and cannot be changed!"),

    /**
     * LogicalNodeTable is not registered on partition %s.
     */
    UNREGISTERED_SEQUENCER_LOGICAL_NODE_TABLE("LogicalNodeTable is not registered on partition %s."),

    /**
     * backupCount must be equal or greater than 0
     */
    BACKUP_COUNT_TOO_LOW("backupCount must be equal or greater than 0"),

    /**
     * backupCount must be equal or lower than %s
     */
    BACKUP_COUNT_TOO_HIGH("backupCount must be equal or lower than %s"),

    /**
     * Illegal Timestamp generated, value below 0
     */
    ILLEGAL_TIMESTAMP_GENERATED("Illegal Timestamp generated, value below 0"),

    /**
     * The given Calendar's value '%s' seems to be in the future! Calendar months start at index 0
     */
    ILLEGAL_CALENDAR_INSTANCE("The given Calendar's value '%s' seems to be in the future! Calendar months start at index 0"),

    /**
     * The given Instant's value '%s' seems to be in the future!
     */
    ILLEGAL_INSTANT("The given Instant's value '%s' seems to be in the future!"),

    /**
     * Generation of an ID failed after %s retries
     */
    GENERATION_MAX_RETRY_EXCEEDED("Generation of an ID failed after %s retries"),

    /**
     * Setting up the internal state failed, unsupported Hazelcast version?
     */
    INTERNAL_SETUP_FAILED("Setting up the internal state failed, unsupported Hazelcast (%s) version?"),

    /**
     * Given nextId is greater than allowed max counter value
     */
    NEXT_ID_LARGER_THAN_ALLOWED_MAX_COUNTER("Given nextId is greater than allowed max counter value"),

    /**
     * Found an unknown Hazelcast version
     */
    UNKNOWN_HAZELCAST_VERSION("Found an unknown Hazelcast version");

    private final String template;

    ExceptionMessages(String template) {
        this.template = template;
    }

    public String buildMessage() {
        return template;
    }

    public String buildMessage(Object... args) {
        return String.format(template, args);
    }
}
