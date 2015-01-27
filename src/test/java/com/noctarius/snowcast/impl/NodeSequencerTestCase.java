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
