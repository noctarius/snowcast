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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.snowcast.SnowcastException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

final class Hazelcast35ClientInvocator
        implements ClientInvocator {

    private final HazelcastClientInstanceImpl client;
    private final Constructor<?> clientInvocationConstructor;
    private final Method invokeMethod;

    Hazelcast35ClientInvocator(@Nonnull HazelcastClientInstanceImpl client) {
        this.client = client;
        this.clientInvocationConstructor = findClientInvocationConstructor();
        this.invokeMethod = findInvokeMethod();
    }

    @Nonnull
    @Override
    public <T> ICompletableFuture<T> invoke(@Nonnegative int partitionId, @Nonnull ClientRequest request) {
        try {
            Object invocation = clientInvocationConstructor.newInstance(client, request, partitionId);
            return (ICompletableFuture<T>) invokeMethod.invoke(invocation);

        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Nonnull
    private Constructor<?> findClientInvocationConstructor() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        try {
            Class<?> clazz = Class.forName("com.hazelcast.client.spi.impl.ClientInvocation");
            return clazz.getConstructor(HazelcastClientInstanceImpl.class, ClientRequest.class, int.class);

        } catch (Exception e) {
            String message = ExceptionMessages.INTERNAL_SETUP_FAILED.buildMessage(buildInfo.getVersion());
            throw new SnowcastException(message, e);
        }
    }

    @Nonnull
    private Method findInvokeMethod() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        try {
            Class<?> clazz = Class.forName("com.hazelcast.client.spi.impl.ClientInvocation");
            return clazz.getMethod("invoke");

        } catch (Exception e) {
            String message = ExceptionMessages.INTERNAL_SETUP_FAILED.buildMessage(buildInfo.getVersion());
            throw new SnowcastException(message, e);
        }
    }
}
