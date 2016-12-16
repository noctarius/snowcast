/*
 * Copyright (c) 2014-2017, Christoph Engelbert (aka noctarius) and
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

public interface Tracer {

    void trace(String message);

    void trace(String message, Object arg1);

    void trace(String message, Object arg1, Object arg2);

    void trace(String message, Object arg1, Object arg2, Object arg3);

    void trace(String message, Object arg1, Object arg2, Object arg3, Object... args);

    void trace(String message, Throwable throwable);

    void trace(String message, Throwable throwable, Object arg1);

    void trace(String message, Throwable throwable, Object arg1, Object arg2);

    void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3);

    void trace(String message, Throwable throwable, Object arg1, Object arg2, Object arg3, Object... args);
}
