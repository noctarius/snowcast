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
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import static com.noctarius.snowcast.impl.ExceptionMessages.ILLEGAL_CALENDAR_INSTANCE;
import static com.noctarius.snowcast.impl.ExceptionUtils.exception;

/**
 * <p>The SnowcastEpoch is used to define a custom epoch for snowcast. A custom epoch
 * is defined as a offset to the standard Java timestamp and is used to offer more
 * bits to the internal timestamp value of a snowcast sequence ID.</p>
 * <p>To build a SnowcastEpoch multiple ways are possible. The most commonly used is
 * utilizing the Java Calendar API as shown in the following snippet:</p>
 * <pre>
 *     Calendar calendar = GregorianCalendar.getInstance();
 *     calendar.set( 2014, 1, 1, 0, 0, 0 );
 *     SnowcastEpoch epoch = SnowcastEpoch.byCalendar( calendar );
 * </pre>
 * <p>Another way is by providing a Java long timestamp value. The important note is,
 * that Java timestamps are based on milliseconds whereas standard Linux timestamps
 * are based on seconds</p>
 * <pre>
 *     SnowcastEpoch epoch = SnowcastEpoch.byTimestamp( 13885308000000 );
 * </pre>
 */
public final class SnowcastEpoch {

    private static final long INITIALIZATION_TIMESTAMP = System.currentTimeMillis();
    private static final long INITIALIZATION_NANOTIME = System.nanoTime();

    private final long offset;

    private SnowcastEpoch(@Nonnegative long offset) {
        this.offset = offset;
    }

    /**
     * Returns the current point in time as a timestamp based on this custom epoch.
     *
     * @return a current timestamp based on this custom epoch
     */
    public long getEpochTimestamp() {
        return getEpochTimestamp(getNow());
    }

    /**
     * Returns the timestamp value between this custom epoch and the standard Linux
     * timestamp epoch (in milliseconds) for the given timestamp.
     *
     * @param timestamp a timestamp based on the standard Linux timestamp epoch
     * @return a timestamp based on this custom epoch
     */
    long getEpochTimestamp(@Nonnegative long timestamp) {
        return timestamp - offset;
    }

    /**
     * Returns the configured offset of this custom epoch. This value is the number
     * of milliseconds between the standard Linux timestamp (in milliseconds) and the
     * configured date / time of this epoch.
     *
     * @return the number of milliseconds (offset) from the standard Linux timestamp
     */
    public long getEpochOffset() {
        return offset;
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return (int) (offset ^ (offset >>> 32));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "SnowcastEpoch{" + "offset=" + offset + '}';
    }

    /**
     * Creates a custom epoch based on the given {@link java.util.Calendar} instance.
     *
     * @param calendar the calendar instance to base the epoch on
     * @return a SnowcastEpoch instance based on the given calendars value
     */
    public static SnowcastEpoch byCalendar(@Nonnull Calendar calendar) {
        calendar.set(Calendar.MILLISECOND, 0);
        long offset = calendar.getTimeInMillis();
        SnowcastEpoch epoch = new SnowcastEpoch(offset);

        if (epoch.getEpochTimestamp() < 0) {
            throw exception(ILLEGAL_CALENDAR_INSTANCE, calendar.getTime());
        }
        return epoch;
    }

    /**
     * Creates a custom epoch based on the given milliseconds based timestamp. This given
     * timestamp must be based on the standard Linux timestamp epoch (in milliseconds).
     *
     * @param timestamp the timestamp value to base the epoch on
     * @return a SnowcastEpoch instance based on the given timestamp value
     */
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
