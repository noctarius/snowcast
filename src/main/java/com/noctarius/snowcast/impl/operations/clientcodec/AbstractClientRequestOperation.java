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
package com.noctarius.snowcast.impl.operations.clientcodec;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

abstract class AbstractClientRequestOperation
        extends Operation
        implements PartitionAwareOperation {

    private final String sequencerName;
    private final MessageChannel messageChannel;

    AbstractClientRequestOperation(String sequencerName, MessageChannel messageChannel) {
        this.sequencerName = sequencerName;
        this.messageChannel = messageChannel;
    }

    @Override
    public void beforeRun()
            throws Exception {
    }

    @Override
    public void afterRun()
            throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
    }

    String getSequencerName() {
        return sequencerName;
    }

    MessageChannel getMessageChannel() {
        return messageChannel;
    }
}
