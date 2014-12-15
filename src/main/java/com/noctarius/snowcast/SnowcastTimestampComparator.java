package com.noctarius.snowcast;

import java.util.Comparator;

import static com.noctarius.snowcast.impl.SnowcastConstants.ID_TIMESTAMP_READ_MASK;
import static com.noctarius.snowcast.impl.SnowcastConstants.SHIFT_TIMESTAMP;

public enum SnowcastTimestampComparator
        implements Comparator<Long> {

    INSTANCE;

    @Override
    public int compare(Long timestamp1, Long timestamp2) {
        long timestampValue1 = (timestamp1 & ID_TIMESTAMP_READ_MASK) >> SHIFT_TIMESTAMP;
        long timestampValue2 = (timestamp2 & ID_TIMESTAMP_READ_MASK) >> SHIFT_TIMESTAMP;
        return timestampValue1 < timestampValue2 ? -1 : timestampValue1 == timestampValue2 ? 0 : 1;
    }
}
