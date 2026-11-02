/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.transport.ProtobufPayload;
import org.opensearch.core.transport.TransportMessage;
import org.opensearch.tasks.TaskAwareRequest;

import java.io.IOException;

/**
 * A transport request
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class TransportRequest extends TransportMessage implements TaskAwareRequest {
    /**
     * Empty transport request
     *
     * @opensearch.internal
     */
    public static class Empty extends TransportRequest {
        public static final Empty INSTANCE = new Empty();

        public Empty() {}

        public Empty(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * Parent of this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "no parent".
     */
    private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

    public TransportRequest() {}

    public TransportRequest(StreamInput in) throws IOException {
        parentTaskId = TaskId.readFromStream(in);
    }

    /**
     * Set a reference to task that created this request.
     */
    @Override
    public void setParentTask(TaskId taskId) {
        this.parentTaskId = taskId;
    }

    /**
     * Get a reference to the task that created this request. Defaults to {@link TaskId#EMPTY_TASK_ID}, meaning "there is no parent".
     */
    @Override
    public TaskId getParentTask() {
        return parentTaskId;
    }

    /**
     * Writes the request as header fields followed by the payload.
     * <p>
     * Every request is <em>header fields</em> (task, routing and timeout metadata written by the base classes via
     * {@link #writeHeaderFields(StreamOutput)}) followed by a <em>payload</em> (the action-specific fields). For a
     * request implementing {@link ProtobufPayload} the payload is the protobuf message for a new enough peer and
     * {@link #writeLegacy(StreamOutput)} otherwise. Requests that still hand-write their payload override this
     * method and call {@code super.writeTo(out)} first, exactly as before.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeHeaderFields(out);
        if (this instanceof ProtobufPayload) {
            ProtobufPayload.writePayload((ProtobufPayload) this, out, this::writeLegacy);
        } else {
            writeLegacy(out);
        }
    }

    /**
     * Writes the fields owned by the base classes, in hierarchy order. Intermediate base classes override this
     * (calling {@code super.writeHeaderFields(out)} first) instead of {@link #writeTo(StreamOutput)}, so that the
     * payload is always written after every header field.
     */
    protected void writeHeaderFields(StreamOutput out) throws IOException {
        parentTaskId.writeTo(out);
    }

    /**
     * The pre-protobuf layout of the payload. Only called for requests implementing {@link ProtobufPayload}, and
     * only for peers older than {@link ProtobufPayload#protobufSince()}. Frozen: must emit exactly what the leaf's
     * {@code writeTo} emitted (after {@code super.writeTo}) before the request became protobuf-backed.
     */
    protected void writeLegacy(StreamOutput out) throws IOException {
        // requests that hand-write their payload override writeTo and never get here
    }
}
