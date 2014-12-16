package com.noctarius.snowcast;

import java.util.Comparator;

import static com.noctarius.snowcast.SnowcastSequenceUtils.timestampValue;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateBoundedMaxLogicalNodeCount;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateCounterMask;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.calculateLogicalNodeShifting;
import static com.noctarius.snowcast.impl.InternalSequencerUtils.counterValue;

public final class SnowcastSequenceComparator
        implements Comparator<Long> {

    private final long counterMask;

    public SnowcastSequenceComparator(int maxLogicalNodeCount) {
        int nodeCount = calculateBoundedMaxLogicalNodeCount(maxLogicalNodeCount);
        int nodeIdShifting = calculateLogicalNodeShifting(nodeCount);
        this.counterMask = calculateCounterMask(maxLogicalNodeCount, nodeIdShifting);
    }

    @Override
    public int compare(Long sequenceId1, Long sequenceId2) {
        long timestampValue1 = timestampValue(sequenceId1);
        long timestampValue2 = timestampValue(sequenceId2);

        if (timestampValue1 < timestampValue2) {
            return -1;
        } else if (timestampValue1 > timestampValue2) {
            return 1;
        }

        int counterValue1 = counterValue(sequenceId1, counterMask);
        int counterValue2 = counterValue(sequenceId2, counterMask);

        return counterValue1 < counterValue2 ? -1 : counterValue1 == counterValue2 ? 0 : 1;
    }
}
