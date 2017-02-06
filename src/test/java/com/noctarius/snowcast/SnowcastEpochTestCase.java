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

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertTrue;

public class SnowcastEpochTestCase {

    @Test
    public void testPositiveTimestampByCalendar()
            throws Exception {

        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        SnowcastEpoch epoch = SnowcastEpoch.byCalendar(calendar);
        long timestamp = epoch.getEpochTimestamp();

        assertTrue(timestamp > 0);
    }

    @Test
    public void testPositiveTimestamp()
            throws Exception {

        Instant instant = Instant.now().minus(1, ChronoUnit.MINUTES);

        SnowcastEpoch epoch = SnowcastEpoch.byInstant(instant);
        long timestamp = epoch.getEpochTimestamp();

        assertTrue(timestamp > 0);
    }

    @Test(expected = SnowcastException.class)
    public void testFutureCalendarInstance()
            throws Exception {

        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) + 1);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        SnowcastEpoch.byCalendar(calendar);
    }

    @Test(expected = SnowcastException.class)
    public void testFutureZonedDateTimeInstance()
            throws Exception {

        ZonedDateTime utc = ZonedDateTime.of(LocalDateTime.now(), ZoneOffset.UTC);
        utc = utc.plusMonths(1);
        SnowcastEpoch.byInstant(utc.toInstant());
    }

}
