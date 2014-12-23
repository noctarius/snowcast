package com.noctarius.snowcast.impl;

import com.noctarius.snowcast.SnowcastSequenceState;
import com.noctarius.snowcast.SnowcastSequencer;

interface InternalSequencer
        extends SnowcastSequencer {

    void stateTransition(SnowcastSequenceState newState);
}
