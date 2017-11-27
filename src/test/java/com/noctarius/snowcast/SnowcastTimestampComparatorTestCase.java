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

import com.noctarius.snowcast.impl.SnowcastConstants;
import org.junit.Test;

import static com.noctarius.snowcast.impl.InternalSequencerUtils.*;
import static junit.framework.TestCase.assertEquals;

public class SnowcastTimestampComparatorTestCase {

    @Test
    public void test_compareTimestamp_first_higher_by_timestamp() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(9999, 1, 1, shifting);

        assertEquals(1, SnowcastTimestampComparator.INSTANCE.compare(sequence1, sequence2));
    }

    @Test
    public void test_compareTimestamp_first_lower_by_timestamp() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(10001, 1, 1, shifting);

        assertEquals(-1, SnowcastTimestampComparator.INSTANCE.compare(sequence1, sequence2));
    }

    @Test
    public void test_compareTimestamp_equal() {
        int boundedNodeCount = calculateBoundedMaxLogicalNodeCount(SnowcastConstants.DEFAULT_MAX_LOGICAL_NODES_13_BITS);
        int shifting = calculateLogicalNodeShifting(boundedNodeCount);
        long sequence1 = generateSequenceId(10000, 1, 1, shifting);
        long sequence2 = generateSequenceId(10000, 2, 1, shifting);

        assertEquals(0, SnowcastTimestampComparator.INSTANCE.compare(sequence1, sequence2));
    }

}
