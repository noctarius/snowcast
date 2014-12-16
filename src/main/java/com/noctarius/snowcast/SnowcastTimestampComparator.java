package com.noctarius.snowcast;

import java.util.Comparator;

import static com.noctarius.snowcast.SnowcastSequenceUtils.timestampValue;

public enum SnowcastTimestampComparator
        implements Comparator<Long> {

    INSTANCE;

    @Override
    public int compare(Long sequenceId1, Long sequenceId2) {
        long timestampValue1 = timestampValue(sequenceId1);
        long timestampValue2 = timestampValue(sequenceId2);
        return timestampValue1 < timestampValue2 ? -1 : timestampValue1 == timestampValue2 ? 0 : 1;
    }
}
