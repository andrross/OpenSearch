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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support.replication;

import org.opensearch.Version;
import org.opensearch.action.support.replication.proto.ReplicationTaskStatusProto;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.transport.ProtobufPayload;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Task that tracks replication actions.
 *
 * @opensearch.internal
 */
public class ReplicationTask extends Task {
    private volatile String phase = "starting";

    public ReplicationTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    /**
     * Set the current phase of the task.
     */
    public void setPhase(String phase) {
        this.phase = phase;
    }

    /**
     * Get the current phase of the task.
     */
    public String getPhase() {
        return phase;
    }

    @Override
    public Status getStatus() {
        return new Status(phase);
    }

    /**
     * Status of the replication task.
     * <p>
     * A {@link Task.Status} that has opted in to protobuf. The frame that carries it (see
     * {@code NamedWriteableFrames}) does not know; the status applies the encoding rule in its own {@code writeTo},
     * exactly as a transport message's root class would.
     *
     * @opensearch.internal
     */
    public static class Status implements Task.Status, ProtobufPayload {
        public static final String NAME = "replication";

        /** First version that encodes this status as protobuf on the wire. */
        public static final Version PROTO_SINCE = Version.V_3_10_0;

        private final ReplicationTaskStatusProto.ReplicationTaskStatus message;

        public Status(String phase) {
            this(
                ReplicationTaskStatusProto.ReplicationTaskStatus.newBuilder()
                    .setPhase(requireNonNull(phase, "Phase cannot be null"))
                    .build()
            );
        }

        private Status(ReplicationTaskStatusProto.ReplicationTaskStatus message) {
            this.message = message;
        }

        public Status(StreamInput in) throws IOException {
            this(
                ProtobufPayload.<ReplicationTaskStatusProto.ReplicationTaskStatus>readPayload(
                    in,
                    PROTO_SINCE,
                    ReplicationTaskStatusProto.ReplicationTaskStatus::parseFrom,
                    i -> ReplicationTaskStatusProto.ReplicationTaskStatus.newBuilder().setPhase(i.readString()).build()
                )
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Version protobufSince() {
            return PROTO_SINCE;
        }

        @Override
        public byte[] protobufBytes() {
            return message.toByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ProtobufPayload.writePayload(this, out, o -> o.writeString(phase()));
        }

        private String phase() {
            return message.getPhase();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("phase", phase());
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }

        // Implements equals and hashcode for testing
        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != ReplicationTask.Status.class) {
                return false;
            }
            ReplicationTask.Status other = (Status) obj;
            return message.equals(other.message);
        }

        @Override
        public int hashCode() {
            return message.hashCode();
        }
    }
}
