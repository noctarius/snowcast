/*
 * Copyright (c) 2015, Christoph Engelbert (aka noctarius) and
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.logging.Level;

public final class InternalLoggerUtils {

    private static final Level TRACE_LEVEL = Level.FINEST;
    private static final Level DEBUG_LEVEL = Level.FINE;
    private static final Level INFO_LEVEL = Level.INFO;
    private static final Level WARNING_LEVEL = Level.WARNING;

    private static final Object[] EMPTY_ARGS = new Object[0];

    private InternalLoggerUtils() {
    }

    public static InternalLogger logger(Class<?> type) {
        ILogger logger = Logger.getLogger(type);
        return new InternalLoggerImpl(logger);
    }

    private static void log(ILogger logger, Level level, String message, Throwable throwable) {
        log(logger, level, message, throwable, EMPTY_ARGS);
    }

    private static void log(ILogger logger, Level level, String message, Throwable throwable, Object arg1) {
        log(logger, level, message, throwable, new Object[]{arg1});
    }

    private static void log(ILogger logger, Level level, String message, Throwable throwable, Object arg1, Object arg2) {
        log(logger, level, message, throwable, new Object[]{arg1, arg2});
    }

    private static void log(ILogger logger, Level level, String message, Throwable throwable, Object arg1, Object arg2,
                            Object arg3) {

        log(logger, level, message, throwable, new Object[]{arg1, arg2, arg3});
    }

    private static void log(ILogger logger, Level level, String message, Throwable throwable, Object arg1, Object arg2,
                            Object arg3, Object... args) {

        Object[] temp = new Object[args.length + 3];
        temp[0] = arg1;
        temp[1] = arg2;
        temp[2] = arg3;
        System.arraycopy(args, 0, temp, 3, args.length);
        log(logger, level, message, throwable, temp);
    }

    private static void log(ILogger logger, Level level, String message, Throwable throwable, Object... args) {
        if (args.length > 0) {
            message = String.format(message, args);
        }
        logger.log(level, message, throwable);
    }

    private static boolean loggingEnabled(ILogger logger, Level level) {
        return SnowcastConstants.LOGGING_ENABLED && logger.isLoggable(level);
    }

    private static boolean isTracing(ILogger logger) {
        return loggingEnabled(logger, TRACE_LEVEL);
    }

    private static boolean isDebugging(ILogger logger) {
        return loggingEnabled(logger, DEBUG_LEVEL);
    }

    private static boolean isInfo(ILogger logger) {
        return loggingEnabled(logger, INFO_LEVEL);
    }

    private static boolean isWarning(ILogger logger) {
        return loggingEnabled(logger, WARNING_LEVEL);
    }

    private static class InternalLoggerImpl
            implements InternalLogger {

        private final ILogger logger;

        private InternalLoggerImpl(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void trace(String message) {
            if (!isTracing(logger)) {
                return;
            }
            trace(message, (Throwable) null);
        }

        @Override
        public void trace(String message, Object arg1) {
            if (!isTracing(logger)) {
                return;
            }
            trace(message, (Throwable) null, arg1);
        }

        @Override
        public void trace(String message, Object arg1, Object arg2) {
            if (!isTracing(logger)) {
                return;
            }
            trace(message, (Throwable) null, arg1, arg2);
        }

        @Override
        public void trace(String message, Object arg1, Object arg2, Object arg3) {
            if (!isTracing(logger)) {
                return;
            }
            trace(message, (Throwable) null, arg1, arg2, arg3);
        }

        @Override
        public void trace(String message, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isTracing(logger)) {
                return;
            }
            trace(message, (Throwable) null, arg1, arg2, arg3, args);
        }

        @Override
        public void debug(String message) {
            if (!isDebugging(logger)) {
                return;
            }
            debug(message, (Throwable) null);
        }

        @Override
        public void debug(String message, Object arg1) {
            if (!isDebugging(logger)) {
                return;
            }
            debug(message, (Throwable) null, arg1);
        }

        @Override
        public void debug(String message, Object arg1, Object arg2) {
            if (!isDebugging(logger)) {
                return;
            }
            debug(message, (Throwable) null, arg1, arg2);
        }

        @Override
        public void debug(String message, Object arg1, Object arg2, Object arg3) {
            if (!isDebugging(logger)) {
                return;
            }
            debug(message, (Throwable) null, arg1, arg2, arg3);
        }

        @Override
        public void debug(String message, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isDebugging(logger)) {
                return;
            }
            debug(message, (Throwable) null, arg1, arg2, arg3, args);
        }

        @Override
        public void info(String message) {
            if (!isInfo(logger)) {
                return;
            }
            info(message, (Throwable) null);
        }

        @Override
        public void info(String message, Object arg1) {
            if (!isInfo(logger)) {
                return;
            }
            info(message, (Throwable) null, arg1);
        }

        @Override
        public void info(String message, Object arg1, Object arg2) {
            if (!isInfo(logger)) {
                return;
            }
            info(message, (Throwable) null, arg1, arg2);
        }

        @Override
        public void info(String message, Object arg1, Object arg2, Object arg3) {
            if (!isInfo(logger)) {
                return;
            }
            info(message, (Throwable) null, arg1, arg2, arg3);
        }

        @Override
        public void info(String message, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isInfo(logger)) {
                return;
            }
            info(message, (Throwable) null, arg1, arg2, arg3, args);
        }

        @Override
        public void warning(String message) {
            if (!isWarning(logger)) {
                return;
            }
            warning(message, (Throwable) null);
        }

        @Override
        public void warning(String message, Object arg1) {
            if (!isWarning(logger)) {
                return;
            }
            warning(message, (Throwable) null, arg1);
        }

        @Override
        public void warning(String message, Object arg1, Object arg2) {
            if (!isWarning(logger)) {
                return;
            }
            warning(message, (Throwable) null, arg1, arg2);
        }

        @Override
        public void warning(String message, Object arg1, Object arg2, Object arg3) {
            if (!isWarning(logger)) {
                return;
            }
            warning(message, (Throwable) null, arg1, arg2, arg3);
        }

        @Override
        public void warning(String message, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isWarning(logger)) {
                return;
            }
            warning(message, (Throwable) null, arg1, arg2, arg3, args);
        }

        // Implementation
        @Override
        public void trace(String message, Throwable throwable) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, TRACE_LEVEL, message, throwable);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, TRACE_LEVEL, message, throwable, arg1);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, TRACE_LEVEL, message, throwable, arg1, arg2);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, TRACE_LEVEL, message, throwable, arg1, arg2, arg3);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, TRACE_LEVEL, message, throwable, arg1, arg2, arg3, args);
        }

        @Override
        public void debug(String message, Throwable throwable) {
            if (!isDebugging(logger)) {
                return;
            }
            log(logger, DEBUG_LEVEL, message, throwable);
        }

        @Override
        public void debug(String message, Throwable throwable, Object arg1) {
            if (!isDebugging(logger)) {
                return;
            }
            log(logger, DEBUG_LEVEL, message, throwable, arg1);
        }

        @Override
        public void debug(String message, Throwable throwable, Object arg1, Object arg2) {
            if (!isDebugging(logger)) {
                return;
            }
            log(logger, DEBUG_LEVEL, message, throwable, arg1, arg2);
        }

        @Override
        public void debug(String message, Throwable throwable, Object arg1, Object arg2, Object arg3) {
            if (!isDebugging(logger)) {
                return;
            }
            log(logger, DEBUG_LEVEL, message, throwable, arg1, arg2, arg3);
        }

        @Override
        public void debug(String message, Throwable throwable, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isDebugging(logger)) {
                return;
            }
            log(logger, DEBUG_LEVEL, message, throwable, arg1, arg2, arg3, args);
        }

        @Override
        public void info(String message, Throwable throwable) {
            if (!isInfo(logger)) {
                return;
            }
            log(logger, INFO_LEVEL, message, throwable);
        }

        @Override
        public void info(String message, Throwable throwable, Object arg1) {
            if (!isInfo(logger)) {
                return;
            }
            log(logger, INFO_LEVEL, message, throwable, arg1);
        }

        @Override
        public void info(String message, Throwable throwable, Object arg1, Object arg2) {
            if (!isInfo(logger)) {
                return;
            }
            log(logger, INFO_LEVEL, message, throwable, arg1, arg2);
        }

        @Override
        public void info(String message, Throwable throwable, Object arg1, Object arg2, Object arg3) {
            if (!isInfo(logger)) {
                return;
            }
            log(logger, INFO_LEVEL, message, throwable, arg1, arg2, arg3);
        }

        @Override
        public void info(String message, Throwable throwable, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isInfo(logger)) {
                return;
            }
            log(logger, INFO_LEVEL, message, throwable, arg1, arg2, arg3, args);
        }

        @Override
        public void warning(String message, Throwable throwable) {
            if (!isWarning(logger)) {
                return;
            }
            log(logger, WARNING_LEVEL, message, throwable);
        }

        @Override
        public void warning(String message, Throwable throwable, Object arg1) {
            if (!isWarning(logger)) {
                return;
            }
            log(logger, WARNING_LEVEL, message, throwable, arg1);
        }

        @Override
        public void warning(String message, Throwable throwable, Object arg1, Object arg2) {
            if (!isWarning(logger)) {
                return;
            }
            log(logger, WARNING_LEVEL, message, throwable, arg1, arg2);
        }

        @Override
        public void warning(String message, Throwable throwable, Object arg1, Object arg2, Object arg3) {
            if (!isWarning(logger)) {
                return;
            }
            log(logger, WARNING_LEVEL, message, throwable, arg1, arg2, arg3);
        }

        @Override
        public void warning(String message, Throwable throwable, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isWarning(logger)) {
                return;
            }
            log(logger, WARNING_LEVEL, message, throwable, arg1, arg2, arg3, args);
        }
    }
}
