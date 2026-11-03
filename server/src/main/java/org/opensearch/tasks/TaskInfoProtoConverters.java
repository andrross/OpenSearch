/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.io.stream.NamedWriteableFrames;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.tasks.resourcetracker.TaskThreadUsage;
import org.opensearch.tasks.proto.TaskIdProto;
import org.opensearch.tasks.proto.TaskInfoProto;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Conversions between {@link TaskInfo} and its generated protobuf form, for containers in other packages
 * ({@code ListTasksResponse}) that embed task info in their own messages.
 * <p>
 * {@link TaskInfo} holds its message without the status; the status is a {@link Task.Status} chosen from the
 * {@code NamedWriteableRegistry}, framed here per peer version (see {@link NamedWriteableFrames}).
 *
 * @opensearch.internal
 */
@InternalApi
public final class TaskInfoProtoConverters {

    private TaskInfoProtoConverters() {}

    /** The full message for {@code peerVersion}, status frame included. */
    public static TaskInfoProto.TaskInfo toProto(TaskInfo info, Version peerVersion) throws IOException {
        if (info.getStatus() == null) {
            return info.message();
        }
        return info.message().toBuilder().setStatus(NamedWriteableFrames.toProto(info.getStatus(), peerVersion)).build();
    }

    /**
     * Decode a full message, resolving the status frame against the registry and version of {@code context}, the
     * stream the message was read from.
     */
    public static TaskInfo fromProto(TaskInfoProto.TaskInfo message, StreamInput context) throws IOException {
        final Task.Status status = message.hasStatus()
            ? NamedWriteableFrames.fromProto(message.getStatus(), Task.Status.class, context)
            : null;
        return new TaskInfo(message.hasStatus() ? message.toBuilder().clearStatus().build() : message, status);
    }

    static TaskInfoProto.TaskInfo toProto(
        TaskId taskId,
        String type,
        String action,
        String description,
        long startTime,
        long runningTimeNanos,
        boolean cancellable,
        boolean cancelled,
        TaskId parentTaskId,
        Map<String, String> headers,
        TaskResourceStats resourceStats,
        Long cancellationStartTime
    ) {
        final TaskInfoProto.TaskInfo.Builder b = TaskInfoProto.TaskInfo.newBuilder();
        if (isEmpty(taskId) == false) {
            b.setTaskId(toProto(taskId));
        }
        // type and action are never null for a real task, but TaskInfo always accepted a null in memory (mocked tasks
        // produce one); it only failed if the info reached the legacy wire, and writeLegacy still does
        if (type != null) {
            b.setType(type);
        }
        if (action != null) {
            b.setAction(action);
        }
        if (description != null) {
            b.setDescription(description);
        }
        b.setStartTime(startTime);
        b.setRunningTimeNanos(runningTimeNanos);
        b.setCancellable(cancellable);
        b.setCancelled(cancelled);
        if (cancellationStartTime != null) {
            b.setCancellationStartTime(cancellationStartTime);
        }
        if (isEmpty(parentTaskId) == false) {
            b.setParentTaskId(toProto(parentTaskId));
        }
        if (headers != null) {
            b.putAllHeaders(headers);
        }
        if (resourceStats != null) {
            b.setResourceStats(toProto(resourceStats));
        }
        return b.build();
    }

    /**
     * The legacy stream's own criterion for {@link TaskId#EMPTY_TASK_ID}: an empty node id. {@link TaskId#isSet()}
     * is a different predicate (id != -1) and a task id with a node and id -1, odd as it is, is not the empty one.
     */
    private static boolean isEmpty(TaskId taskId) {
        return taskId == null || taskId.getNodeId().isEmpty();
    }

    static TaskIdProto.TaskId toProto(TaskId taskId) {
        return TaskIdProto.TaskId.newBuilder().setNodeId(taskId.getNodeId()).setId(taskId.getId()).build();
    }

    /** An absent message is {@link TaskId#EMPTY_TASK_ID}, exactly as an empty node id is on the legacy stream. */
    static TaskId fromProto(TaskIdProto.TaskId taskId, boolean present) {
        return present ? new TaskId(taskId.getNodeId(), taskId.getId()) : TaskId.EMPTY_TASK_ID;
    }

    static TaskInfoProto.TaskResourceStats toProto(TaskResourceStats stats) {
        final TaskInfoProto.TaskResourceStats.Builder b = TaskInfoProto.TaskResourceStats.newBuilder();
        for (Map.Entry<String, TaskResourceUsage> e : stats.getResourceUsageInfo().entrySet()) {
            b.putResourceUsage(
                e.getKey(),
                TaskInfoProto.TaskResourceUsage.newBuilder()
                    .setCpuTimeInNanos(e.getValue().getCpuTimeInNanos())
                    .setMemoryInBytes(e.getValue().getMemoryInBytes())
                    .build()
            );
        }
        final TaskThreadUsage threads = stats.getThreadUsage();
        b.setThreadUsage(
            TaskInfoProto.TaskThreadUsage.newBuilder()
                .setThreadExecutions(threads.getThreadExecutions())
                .setActiveThreads(threads.getActiveThreads())
                .build()
        );
        return b.build();
    }

    static TaskResourceStats fromProto(TaskInfoProto.TaskResourceStats stats) {
        final Map<String, TaskResourceUsage> usage = new LinkedHashMap<>();
        for (Map.Entry<String, TaskInfoProto.TaskResourceUsage> e : stats.getResourceUsageMap().entrySet()) {
            usage.put(e.getKey(), new TaskResourceUsage(e.getValue().getCpuTimeInNanos(), e.getValue().getMemoryInBytes()));
        }
        return new TaskResourceStats(
            usage,
            new TaskThreadUsage(stats.getThreadUsage().getThreadExecutions(), stats.getThreadUsage().getActiveThreads())
        );
    }
}
