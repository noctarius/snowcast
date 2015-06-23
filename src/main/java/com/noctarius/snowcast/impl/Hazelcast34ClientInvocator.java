package com.noctarius.snowcast.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.snowcast.SnowcastException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.lang.reflect.Method;

final class Hazelcast34ClientInvocator
        implements ClientInvocator {

    private final ClientInvocationService clientInvocationService;
    private final Method invokeOnPartitionOwnerMethod;

    Hazelcast34ClientInvocator(@Nonnull HazelcastClientInstanceImpl client) {
        this.clientInvocationService = client.getInvocationService();
        this.invokeOnPartitionOwnerMethod = findInvokeOnPartitionOwnerMethod();
    }

    @Nonnull
    @Override
    public <T> ICompletableFuture<T> invoke(@Nonnegative int partitionId, @Nonnull ClientRequest request) {
        try {
            return (ICompletableFuture<T>) invokeOnPartitionOwnerMethod.invoke(clientInvocationService, request, partitionId);

        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Nonnull
    private Method findInvokeOnPartitionOwnerMethod() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        try {
            return ClientInvocationService.class.getMethod("invokeOnPartitionOwner", ClientRequest.class, int.class);

        } catch (Exception e) {
            String message = ExceptionMessages.INTERNAL_SETUP_FAILED.buildMessage(buildInfo.getVersion());
            throw new SnowcastException(message, e);
        }
    }
}
