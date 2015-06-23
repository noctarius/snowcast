package com.noctarius.snowcast.impl;

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.core.ICompletableFuture;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

interface ClientInvocator {

    @Nonnull
    <T> ICompletableFuture<T> invoke(@Nonnegative int partitionId, @Nonnull ClientRequest request);

}
