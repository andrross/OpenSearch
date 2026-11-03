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

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.transport.ProtobufPayload;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ObjectParserHelper;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tasks.proto.TaskInfoProto;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Information about a currently running task.
 * <p>
 * Tasks are used for communication with transport actions. As a result, they can contain callback
 * references as well as mutable state. That makes it impractical to send tasks over transport channels
 * and use in APIs. Instead, immutable and writeable TaskInfo objects are used to represent
 * snapshot information about currently running tasks.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class TaskInfo implements Writeable, ToXContentFragment, ProtobufPayload {

    /** First version that encodes task info as protobuf on the wire. */
    public static final Version PROTO_SINCE = Version.V_3_10_0;

    /**
     * Every field except the status. The status is a {@link Task.Status}, an extension point chosen from the
     * {@code NamedWriteableRegistry} at read time, whose bytes depend on the peer's version; it is framed into the
     * message when the message is written (see {@link TaskInfoProtoConverters}).
     */
    private final TaskInfoProto.TaskInfo message;

    private final Task.Status status;

    public TaskInfo(
        TaskId taskId,
        String type,
        String action,
        String description,
        Task.Status status,
        long startTime,
        long runningTimeNanos,
        boolean cancellable,
        boolean cancelled,
        TaskId parentTaskId,
        Map<String, String> headers,
        TaskResourceStats resourceStats
    ) {
        this(
            taskId,
            type,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            resourceStats,
            null
        );
    }

    public TaskInfo(
        TaskId taskId,
        String type,
        String action,
        String description,
        Task.Status status,
        long startTime,
        long runningTimeNanos,
        boolean cancellable,
        boolean cancelled,
        TaskId parentTaskId,
        Map<String, String> headers,
        TaskResourceStats resourceStats,
        Long cancellationStartTime
    ) {
        this(
            TaskInfoProtoConverters.toProto(
                taskId,
                type,
                action,
                description,
                startTime,
                runningTimeNanos,
                cancellable,
                cancelled,
                parentTaskId,
                headers,
                resourceStats,
                cancellationStartTime
            ),
            status
        );
    }

    /**
     * Wrap a message (without a status frame) and its decoded status. Package-private: callers outside this package
     * use the public constructors and {@link TaskInfoProtoConverters}.
     */
    TaskInfo(TaskInfoProto.TaskInfo message, Task.Status status) {
        Objects.requireNonNull(message, "message");
        if (message.getCancellable() == false && message.getCancelled()) {
            throw new IllegalArgumentException("task cannot be cancelled");
        }
        this.message = message;
        this.status = status;
    }

    public TaskInfo(StreamInput in) throws IOException {
        this(
            ProtobufPayload.readPayload(
                in,
                PROTO_SINCE,
                bytes -> TaskInfoProtoConverters.fromProto(TaskInfoProto.TaskInfo.parseFrom(bytes), in),
                TaskInfo::readLegacy
            )
        );
    }

    private TaskInfo(TaskInfo decoded) {
        this(decoded.message, decoded.status);
    }

    /**
     * {@link TaskInfo} is a {@code Writeable} carried inside other messages ({@code NodeTasksResponse},
     * {@code TaskResult}), not a transport message, so no root class chooses its encoding; it applies the same rule
     * itself.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ProtobufPayload.writePayload(this, out, this::writeLegacy);
    }

    @Override
    public Version protobufSince() {
        return PROTO_SINCE;
    }

    @Override
    public byte[] protobufBytes() {
        return protobufBytes(Version.CURRENT);
    }

    @Override
    public byte[] protobufBytes(Version peerVersion) {
        try {
            return TaskInfoProtoConverters.toProto(this, peerVersion).toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The message without its status frame. Package-private: code outside this package reads the accessors. */
    TaskInfoProto.TaskInfo message() {
        return message;
    }

    // ----- frozen legacy layout -----

    private static TaskInfo readLegacy(StreamInput in) throws IOException {
        final TaskId taskId = TaskId.readFromStream(in);
        final String type = in.readString();
        final String action = in.readString();
        final String description = in.readOptionalString();
        final Task.Status status = in.readOptionalNamedWriteable(Task.Status.class);
        final long startTime = in.readLong();
        final long runningTimeNanos = in.readLong();
        final boolean cancellable = in.readBoolean();
        final boolean cancelled;
        if (in.getVersion().onOrAfter(Version.V_2_0_0)) {
            cancelled = in.readBoolean();
        } else {
            cancelled = false;
        }
        final TaskId parentTaskId = TaskId.readFromStream(in);
        final Map<String, String> headers = in.readMap(StreamInput::readString, StreamInput::readString);
        final TaskResourceStats resourceStats;
        if (in.getVersion().onOrAfter(Version.V_2_1_0)) {
            resourceStats = in.readOptionalWriteable(TaskResourceStats::new);
        } else {
            resourceStats = null;
        }
        final Long cancellationStartTime;
        if (in.getVersion().onOrAfter(Version.V_2_8_0)) {
            cancellationStartTime = in.readOptionalLong();
        } else {
            cancellationStartTime = null;
        }
        return new TaskInfo(
            taskId,
            type,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            resourceStats,
            cancellationStartTime
        );
    }

    private void writeLegacy(StreamOutput out) throws IOException {
        getTaskId().writeTo(out);
        out.writeString(getType());
        out.writeString(getAction());
        out.writeOptionalString(getDescription());
        out.writeOptionalNamedWriteable(status);
        out.writeLong(getStartTime());
        out.writeLong(getRunningTimeNanos());
        out.writeBoolean(isCancellable());
        if (out.getVersion().onOrAfter(Version.V_2_0_0)) {
            out.writeBoolean(isCancelled());
        }
        getParentTaskId().writeTo(out);
        out.writeMap(getHeaders(), StreamOutput::writeString, StreamOutput::writeString);
        if (out.getVersion().onOrAfter(Version.V_2_1_0)) {
            out.writeOptionalWriteable(getResourceStats());
        }
        if (out.getVersion().onOrAfter(Version.V_2_8_0)) {
            out.writeOptionalLong(getCancellationStartTime());
        }
    }

    // ----- public API (unchanged) -----

    public TaskId getTaskId() {
        return TaskInfoProtoConverters.fromProto(message.getTaskId(), message.hasTaskId());
    }

    public long getId() {
        return getTaskId().getId();
    }

    public String getType() {
        return message.hasType() ? message.getType() : null;
    }

    public String getAction() {
        return message.hasAction() ? message.getAction() : null;
    }

    public String getDescription() {
        return message.hasDescription() ? message.getDescription() : null;
    }

    /**
     * The status of the running task. Only available if TaskInfos were build
     * with the detailed flag.
     */
    public Task.Status getStatus() {
        return status;
    }

    /**
     * Returns the task start time
     */
    public long getStartTime() {
        return message.getStartTime();
    }

    /**
     * Returns the task running time
     */
    public long getRunningTimeNanos() {
        return message.getRunningTimeNanos();
    }

    /**
     * Returns true if the task supports cancellation
     */
    public boolean isCancellable() {
        return message.getCancellable();
    }

    /**
     * Returns true if the task has been cancelled
     */
    public boolean isCancelled() {
        return message.getCancelled();
    }

    public Long getCancellationStartTime() {
        return message.hasCancellationStartTime() ? message.getCancellationStartTime() : null;
    }

    /**
     * Returns the parent task id
     */
    public TaskId getParentTaskId() {
        return TaskInfoProtoConverters.fromProto(message.getParentTaskId(), message.hasParentTaskId());
    }

    /**
     * Returns the task headers
     */
    public Map<String, String> getHeaders() {
        return message.getHeadersMap();
    }

    /**
     * Returns the task resource information
     */
    public TaskResourceStats getResourceStats() {
        return message.hasResourceStats() ? TaskInfoProtoConverters.fromProto(message.getResourceStats()) : null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final TaskId taskId = getTaskId();
        builder.field("node", taskId.getNodeId());
        builder.field("id", taskId.getId());
        builder.field("type", getType());
        builder.field("action", getAction());
        if (status != null) {
            builder.field("status", status, params);
        }
        final String description = getDescription();
        if (description != null) {
            builder.field("description", description);
        }
        builder.timeField("start_time_in_millis", "start_time", getStartTime());
        if (builder.humanReadable()) {
            builder.field("running_time", new TimeValue(getRunningTimeNanos(), TimeUnit.NANOSECONDS).toString());
        }
        builder.field("running_time_in_nanos", getRunningTimeNanos());
        builder.field("cancellable", isCancellable());
        builder.field("cancelled", isCancelled());
        final TaskId parentTaskId = getParentTaskId();
        if (parentTaskId.isSet()) {
            builder.field("parent_task_id", parentTaskId.toString());
        }
        builder.startObject("headers");
        for (Map.Entry<String, String> attribute : getHeaders().entrySet()) {
            builder.field(attribute.getKey(), attribute.getValue());
        }
        builder.endObject();
        final TaskResourceStats resourceStats = getResourceStats();
        if (resourceStats != null) {
            builder.startObject("resource_stats");
            resourceStats.toXContent(builder, params);
            builder.endObject();
        }
        final Long cancellationStartTime = getCancellationStartTime();
        if (cancellationStartTime != null) {
            builder.humanReadableField("cancellation_time_millis", "cancellation_time", new TimeValue(cancellationStartTime));
        }
        return builder;
    }

    public static TaskInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static final ConstructingObjectParser<TaskInfo, Void> PARSER = new ConstructingObjectParser<>("task_info", true, a -> {
        int i = 0;
        TaskId id = new TaskId((String) a[i++], (Long) a[i++]);
        String type = (String) a[i++];
        String action = (String) a[i++];
        String description = (String) a[i++];
        BytesReference statusBytes = (BytesReference) a[i++];
        long startTime = (Long) a[i++];
        long runningTimeNanos = (Long) a[i++];
        boolean cancellable = (Boolean) a[i++];
        boolean cancelled = a[i++] == Boolean.TRUE;
        String parentTaskIdString = (String) a[i++];
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) a[i++];
        if (headers == null) {
            // This might happen if we are reading an old version of task info
            headers = Collections.emptyMap();
        }
        @SuppressWarnings("unchecked")
        TaskResourceStats resourceStats = (TaskResourceStats) a[i++];
        Long cancellationStartTime = (Long) a[i++];
        RawTaskStatus status = statusBytes == null ? null : new RawTaskStatus(statusBytes);
        TaskId parentTaskId = parentTaskIdString == null ? TaskId.EMPTY_TASK_ID : new TaskId(parentTaskIdString);
        return new TaskInfo(
            id,
            type,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            resourceStats,
            cancellationStartTime
        );
    });
    static {
        // Note for the future: this has to be backwards and forwards compatible with all changes to the task storage format
        PARSER.declareString(constructorArg(), new ParseField("node"));
        PARSER.declareLong(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("type"));
        PARSER.declareString(constructorArg(), new ParseField("action"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
        ObjectParserHelper<TaskInfo, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(PARSER, optionalConstructorArg(), new ParseField("status"));
        PARSER.declareLong(constructorArg(), new ParseField("start_time_in_millis"));
        PARSER.declareLong(constructorArg(), new ParseField("running_time_in_nanos"));
        PARSER.declareBoolean(constructorArg(), new ParseField("cancellable"));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField("cancelled"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("parent_task_id"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), new ParseField("headers"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> TaskResourceStats.fromXContent(p), new ParseField("resource_stats"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("cancellation_time_millis"));
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }

    // Implements equals and hashCode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskInfo.class) {
            return false;
        }
        TaskInfo other = (TaskInfo) obj;
        return message.equals(other.message) && Objects.equals(status, other.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, status);
    }
}
