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

import java.io.InputStream;
import java.util.Properties;

public final class SnowcastConstants {

    // Retry timeout for increment function of the counter
    public static final int INCREMENT_RETRY_TIMEOUT_NANOS = 100;

    // Shifting values
    public static final int SHIFT_COUNTER = 41;
    public static final int SHIFT_TIMESTAMP = 23;

    // Read masks
    public static final long TC_TIMESTAMP_READ_MASK = 0x1FFFFFFFFFFL;
    public static final long TC_COUNTER_READ_MASK = 0x7FFFFE0000000000L;
    public static final long ID_TIMESTAMP_READ_MASK = 0xFFFFFFFFFF800000L;

    // Exponent for power of 2 lookup
    public static final int MAX_LOGICAL_NODE_COUNT_8192 = 8191;
    public static final int MAX_LOGICAL_NODE_COUNT_4096 = 4095;
    public static final int MAX_LOGICAL_NODE_COUNT_2048 = 2047;
    public static final int MAX_LOGICAL_NODE_COUNT_1024 = 1023;
    public static final int MAX_LOGICAL_NODE_COUNT_512 = 511;
    public static final int MAX_LOGICAL_NODE_COUNT_256 = 255;
    public static final int MAX_LOGICAL_NODE_COUNT_128 = 127;
    public static final int SHIFT_LOGICAL_NODE_ID_8192 = 10;
    public static final int SHIFT_LOGICAL_NODE_ID_4096 = 11;
    public static final int SHIFT_LOGICAL_NODE_ID_2048 = 12;
    public static final int SHIFT_LOGICAL_NODE_ID_1024 = 13;
    public static final int SHIFT_LOGICAL_NODE_ID_512 = 14;
    public static final int SHIFT_LOGICAL_NODE_ID_256 = 15;
    public static final int SHIFT_LOGICAL_NODE_ID_128 = 16;

    // Logical Node Bounding
    public static final int NODE_ID_LOWER_BOUND = 128;
    public static final int NODE_ID_UPPER_BOUND = 8192;

    // Default configuration values
    public static final int DEFAULT_MAX_LOGICAL_NODES_13_BITS = 8192;

    // Defined service name
    public static final String SERVICE_NAME = "noctarius::SequencerService";

    // User context lookup name
    public static final String USER_CONTEXT_LOOKUP_NAME = "noctarius::Snowcast::SequencerService";

    // snowcast ascii logo
    public static final String SNOWCAST_ASCII_LOGO = "                                             __ \n" +
            "   _________  ____ _      ___________ ______/ /_\n" +
            "  / ___/ __ \\/ __ \\ | /| / / ___/ __ `/ ___/ __/\n" +
            " (__  ) / / / /_/ / |/ |/ / /__/ /_/ (__  ) /_  \n" +
            "/____/_/ /_/\\____/|__/|__/\\___/\\__,_/____/\\__/  \n";

    // snowcast version
    public static final String VERSION;

    // snowcast build date
    public static final String BUILD_DATE;

    // System Property to prevent lazy configuration
    public static final String PROPERTY_PREVENT_LAZY_CONFIGURATION = "com.noctarius.snowcast.prevent.lazy.configuration";

    // File name for properties file containing build information
    private static final String SNOWCAST_VERSION_FILE = "snowcast-version.properties";

    // Property keys from build information file
    private static final String SNOWCAST_VERSION_PROPERTY = "snowcast-version";
    private static final String SNOWCAST_BUILD_DATE_PROPERTY = "snowcast-build-date";

    static {
        String version = "Unknown version";
        String buildDate = "Unknown build-date";
        try {
            ClassLoader classLoader = SnowcastConstants.class.getClassLoader();
            InputStream versionFile = classLoader.getResourceAsStream(SNOWCAST_VERSION_FILE);
            Properties properties = new Properties();
            properties.load(versionFile);

            version = properties.getProperty(SNOWCAST_VERSION_PROPERTY);
            buildDate = properties.getProperty(SNOWCAST_BUILD_DATE_PROPERTY);
        } catch (Exception e) {
            // We really want to ignore this, should never fail but if it does
            // there is no reason to prevent startup!
        }
        VERSION = version;
        BUILD_DATE = buildDate;
    }

    private SnowcastConstants() {
    }

    public static boolean preventLazyConfiguration() {
        String property = System.getProperty(PROPERTY_PREVENT_LAZY_CONFIGURATION);
        return property != null;
    }
}
