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

import java.util.Comparator;

/**
 * <p>This {@link java.util.Comparator} implementation can be used to compare and order two distinct
 * snowcast sequencer ids.<br/>
 * This is meant for legacy code or to integrate it with existing frameworks. In general use cases
 * {@link com.noctarius.snowcast.SnowcastSequenceUtils#compareTimestamp(long, long)} (long, long)}
 * should be preferred to prevent some unnecessary boxing/unboxing from <tt>long</tt> to <tt>Long</tt>
 * and back.</p>
 * <p>This comparator can be used just as any other Java {@link java.util.Comparator}:
 * <pre>
 *     List<Long> elements = getSequencerIds();
 *     Collections.sort( elements, SnowcastTimestampComparator.INSTANCE );
 *     System.out.println( elements );
 * </pre>
 * </p>
 */
public enum SnowcastTimestampComparator
        implements Comparator<Long> {

    /**
     * The singleton instance for this {@link java.util.Comparator} implementation. Since the
     * implementation is completely stateless this instance is fine to be used in multi-threading
     * environments.<br/>
     * More information on the implementation detail are available here:
     * {@link com.noctarius.snowcast.SnowcastTimestampComparator}.
     */
    INSTANCE;

    @Override
    public int compare(Long sequenceId1, Long sequenceId2) {
        return SnowcastSequenceUtils.compareTimestamp(sequenceId1, sequenceId2);
    }
}
