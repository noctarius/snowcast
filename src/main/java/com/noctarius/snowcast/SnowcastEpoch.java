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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public final class SnowcastEpoch {

    private static final long INITIALIZATION_TIMESTAMP = System.currentTimeMillis();
    private static final long INITIALIZATION_NANOTIME = System.nanoTime();

    private final long offset;

    private SnowcastEpoch(@Nonnegative long offset) {
        this.offset = offset;
    }

    public long getEpochTimestamp() {
        return getEpochTimestamp(getNow());
    }

    long getEpochTimestamp(@Nonnegative long timestamp) {
        return timestamp - offset;
    }

    public long getEpochOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnowcastEpoch epoch = (SnowcastEpoch) o;
        return offset == epoch.offset;
    }

    @Override
    public int hashCode() {
        return (int) (offset ^ (offset >>> 32));
    }

    @Override
    public String toString() {
        return "SnowcastEpoch{" + "offset=" + offset + '}';
    }

    public static SnowcastEpoch byCalendar(@Nonnull Calendar calendar) {
        long offset = calendar.getTimeInMillis();
        return new SnowcastEpoch(offset);
    }

    @Nonnull
    public static SnowcastEpoch byTimestamp(@Nonnegative long timestamp) {
        return new SnowcastEpoch(timestamp);
    }

    private static long getNow() {
        long nanoTime = System.nanoTime();
        long delta = nanoTime - INITIALIZATION_NANOTIME;
        return INITIALIZATION_TIMESTAMP + TimeUnit.NANOSECONDS.toMillis(delta);
    }
}
