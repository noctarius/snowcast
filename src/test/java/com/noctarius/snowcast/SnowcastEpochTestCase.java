package com.noctarius.snowcast;

import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertTrue;

public class SnowcastEpochTestCase {

    @Test
    public void testPositiveTimestamp()
            throws Exception {

        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        SnowcastEpoch epoch = SnowcastEpoch.byCalendar(calendar);
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

}
