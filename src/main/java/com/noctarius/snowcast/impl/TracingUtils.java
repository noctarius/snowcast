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

public final class TracingUtils {

    private static final Level TRACE_LEVEL = Level.FINEST;

    private static final Object[] EMPTY_ARGS = new Object[0];

    private static final Tracer NO_OP_TRACER = new NoOpTracerImpl();

    private TracingUtils() {
    }

    public static Tracer tracer(Class<?> type) {
        if (!SnowcastConstants.LOGGING_ENABLED) {
            return NO_OP_TRACER;
        }
        ILogger logger = Logger.getLogger(type);
        return new TracerImpl(logger);
    }

    private static void log(ILogger logger, String message, Throwable throwable) {
        log(logger, TRACE_LEVEL, message, throwable, EMPTY_ARGS);
    }

    private static void log(ILogger logger, String message, Throwable throwable, Object arg1) {
        log(logger, TRACE_LEVEL, message, throwable, new Object[]{arg1});
    }

    private static void log(ILogger logger, String message, Throwable throwable, Object arg1, Object arg2) {
        log(logger, TRACE_LEVEL, message, throwable, new Object[]{arg1, arg2});
    }

    private static void log(ILogger logger, String message, Throwable throwable, Object arg1, Object arg2, Object arg3) {

        log(logger, TRACE_LEVEL, message, throwable, new Object[]{arg1, arg2, arg3});
    }

    private static void log(ILogger logger, String message, Throwable throwable, Object arg1, Object arg2, Object arg3,
                            Object... args) {

        Object[] temp = new Object[args.length + 3];
        temp[0] = arg1;
        temp[1] = arg2;
        temp[2] = arg3;
        System.arraycopy(args, 0, temp, 3, args.length);
        log(logger, TRACE_LEVEL, message, throwable, temp);
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

    private static final class TracerImpl
            implements Tracer {

        private final ILogger logger;

        private TracerImpl(ILogger logger) {
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

        // Implementation
        @Override
        public void trace(String message, Throwable throwable) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, message, throwable);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, message, throwable, arg1);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, message, throwable, arg1, arg2);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, message, throwable, arg1, arg2, arg3);
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3, Object... args) {
            if (!isTracing(logger)) {
                return;
            }
            log(logger, message, throwable, arg1, arg2, arg3, args);
        }
    }

    private static final class NoOpTracerImpl
            implements Tracer {

        @Override
        public void trace(String message) {
        }

        @Override
        public void trace(String message, Object arg1) {
        }

        @Override
        public void trace(String message, Object arg1, Object arg2) {
        }

        @Override
        public void trace(String message, Object arg1, Object arg2, Object arg3) {
        }

        @Override
        public void trace(String message, Object arg1, Object arg2, Object arg3, Object... args) {
        }

        @Override
        public void trace(String message, Throwable throwable) {
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1) {
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2) {
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3) {
        }

        @Override
        public void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3, Object... args) {
        }
    }
}
