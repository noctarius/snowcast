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
package com.noctarius.snowcast.impl;

import com.noctarius.snowcast.SnowcastEpoch;
import org.junit.Test;

public class NodeSequencerTestCase {

    @Test(timeout = 60000)
    public void test_counter()
            throws Exception {

        // Definition leaves 10 bits for the counter
        SequencerDefinition definition = new SequencerDefinition("foo", SnowcastEpoch.byTimestamp(0), 8192, (short) 1);

        NodeSequencer sequencer = new NodeSequencer(new NodeSequencerService() {
            @Override
            public int attachSequencer(final SequencerDefinition definition) {
                // Faking we're in node 0
                return 0;
            }
        }, definition);

        sequencer.attachLogicalNode();

        for (int i = 0; i < 2000; i++) {
            // This would hang if counter doesn't work
            sequencer.next();
        }
    }
}
