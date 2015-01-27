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
