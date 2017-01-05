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

import com.noctarius.snowcast.SnowcastEpoch;
import com.noctarius.snowcast.SnowcastMaxLogicalNodeIdOutOfBoundsException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import static com.noctarius.snowcast.impl.ExceptionMessages.ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY;
import static com.noctarius.snowcast.impl.ExceptionUtils.exception;
import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_LOWER_BOUND;
import static com.noctarius.snowcast.impl.SnowcastConstants.NODE_ID_UPPER_BOUND;

public final class SequencerDefinition {
    private final String sequencerName;
    private final SnowcastEpoch epoch;
    private final int maxLogicalNodeCount;
    private final int boundedMaxLogicalNodeCount;
    private final short backupCount;

    public SequencerDefinition(@Nonnull String sequencerName, @Nonnull SnowcastEpoch epoch,
                               @Min(128) @Max(8192) int maxLogicalNodeCount,
                               @Nonnegative @Max(Short.MAX_VALUE) short backupCount) {

        if (maxLogicalNodeCount < NODE_ID_LOWER_BOUND) {
            throw exception(SnowcastMaxLogicalNodeIdOutOfBoundsException::new, //
                    ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY, "smaller", NODE_ID_LOWER_BOUND);
        }
        if (maxLogicalNodeCount > NODE_ID_UPPER_BOUND) {
            throw exception(SnowcastMaxLogicalNodeIdOutOfBoundsException::new, //
                    ILLEGAL_MAX_LOGICAL_NODE_ID_BOUNDARY, "larger", NODE_ID_UPPER_BOUND);
        }

        this.sequencerName = sequencerName;
        this.epoch = epoch;
        this.maxLogicalNodeCount = maxLogicalNodeCount;
        this.backupCount = backupCount;
        this.boundedMaxLogicalNodeCount = InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
    }

    @Nonnull
    public String getSequencerName() {
        return sequencerName;
    }

    @Nonnull
    public SnowcastEpoch getEpoch() {
        return epoch;
    }

    @Nonnegative
    public int getMaxLogicalNodeCount() {
        return maxLogicalNodeCount;
    }

    @Nonnegative
    public int getBoundedMaxLogicalNodeCount() {
        return boundedMaxLogicalNodeCount;
    }

    @Min(128)
    @Max(8192)
    public short getBackupCount() {
        return backupCount;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SequencerDefinition that = (SequencerDefinition) o;

        if (maxLogicalNodeCount != that.maxLogicalNodeCount) {
            return false;
        }
        if (!epoch.equals(that.epoch)) {
            return false;
        }
        if (!sequencerName.equals(that.sequencerName)) {
            return false;
        }
        return backupCount == that.backupCount;
    }

    @Override
    public int hashCode() {
        int result = sequencerName.hashCode();
        result = 31 * result + (epoch.hashCode());
        result = 31 * result + maxLogicalNodeCount;
        result = 31 * result + backupCount;
        return result;
    }

    @Override
    public String toString() {
        return "SequencerDefinition{" + "sequencerName='" + sequencerName + '\'' + ", epoch=" + epoch + ", maxLogicalNodeCount="
                + maxLogicalNodeCount + ", backupCount=" + backupCount + '}';
    }
}
