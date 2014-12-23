package com.noctarius.snowcast.impl;

public final class SnowcastConstants {

    // Retry timeout for increment function of the counter
    public static final int INCREMENT_RETRY_TIMEOUT_NANOS = 100;

    // Shifting values
    public static final int SHIFT_COUNTER = 41;
    public static final int SHIFT_TIMESTAMP = 23;
    public static final int BASE_SHIFT_LOGICAL_NODE_ID = 10;

    // Read masks
    public static final long TC_TIMESTAMP_READ_MASK = 0x1FFFFFFFFFFL;
    public static final long TC_COUNTER_READ_MASK = 0x7FFFFE0000000000L;
    public static final long ID_TIMESTAMP_READ_MASK = 0xFFFFFFFFFF800000L;

    // Exponent for power of 2 lookup
    public static final int[] EXP_LOOKUP = {8192, 4096, 2048, 1024, 512, 256, 128};

    // Logical Node Bounding
    public static final int NODE_ID_LOWER_BOUND = 128;
    public static final int NODE_ID_UPPER_BOUND = 8192;

    // Default configuration values
    public static final int DEFAULT_MAX_LOGICAL_NODES_13_BITS = 8192;

    // Defined service name
    public static final String SERVICE_NAME = "noctarius::SequencerService";

    // User context lookup name
    public static final String USER_CONTEXT_LOOKUP_NAME = "noctarius::Snowcast::SequencerService";

    private SnowcastConstants() {
    }
}
