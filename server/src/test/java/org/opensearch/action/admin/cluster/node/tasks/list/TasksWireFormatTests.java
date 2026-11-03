/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.list;

import com.google.protobuf.UnknownFieldSet;
import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.action.support.replication.ReplicationTask;
import org.opensearch.action.support.replication.proto.ReplicationTaskStatusProto;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableFrames;
import org.opensearch.common.io.stream.proto.NamedWriteableProto;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.TaskResourceStats;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.tasks.resourcetracker.TaskThreadUsage;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.RawTaskStatus;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Pins the two encodings of {@link ListTasksRequest}, {@link ListTasksResponse} and {@link TaskInfo}, and exercises
 * the {@code NamedWriteable} frame that carries a {@link Task.Status} inside a protobuf message.
 * <p>
 * The {@code GOLDEN_*} constants were produced by serialising the same instances with the released
 * {@code org.opensearch:opensearch:3.9.0} artifact at stream version 3.9.0 (see the generator in the PR
 * description). They are what a real 3.9 peer emits and expects, so the legacy layouts must match them byte for
 * byte.
 */
public class TasksWireFormatTests extends OpenSearchTestCase {

    private static final Version LEGACY = Version.V_3_9_0;
    /** A peer newer than this build; only its id matters. */
    private static final Version FUTURE = Version.fromString("3.11.0");

    private static final String GOLDEN_REQUEST_FULL =
        "0b636f6f7264696e61746f72000000000000000900026e31000000000000000702026e31026e320214696e6469"
            + "6365733a646174612f77726974652f2a1b636c75737465723a6d6f6e69746f722f7461736b732f6c69737473010a030101";
    private static final String GOLDEN_REQUEST_DEFAULT = "0000000000000000";
    private static final String GOLDEN_TASK_INFO_FULL =
        "026e31000000000000002a097472616e73706f72741a696e64696365733a646174612f77726974652f62"
            + "756c6b5b735d011772657175657374735b315d2c20696e6465785b6964785d010b7265706c69636174696f6e0866696e6973686564000001"
            + "8bcfe5680000000000075bcd150101026e310000000000000005010b582d4f70617175652d496403616263010105746f74616ce807801000"
            + "00000300000001010000018bcfe56b09";
    private static final String GOLDEN_TASK_INFO_MINIMAL =
        "026e320000000000000001066469726563741e636c75737465723a6d6f6e69746f722f7461736b73"
            + "2f6c697374735b6e5d000000000000000000010000000000000002000000000000";
    private static final String GOLDEN_TASK_INFO_RAW_STATUS = "026e320000000000000002097472616e73706f72741a696e64696365733a646174612f777269"
        + "74652f7265696e6465780001037261770d7b22746f74616c223a31307d00000000000000030000000000000004010000000000";
    private static final String GOLDEN_RESPONSE =
        "01026e31000000000000000b010e0104626f6f6d00000015494e5445524e414c5f5345525645525f4552524f52"
            + "0101096e6f646520646f776e000000000001026e3303"
            + GOLDEN_TASK_INFO_FULL
            + GOLDEN_TASK_INFO_MINIMAL
            + GOLDEN_TASK_INFO_RAW_STATUS;

    // ----- request: legacy layout vs. released 3.9.0 -----

    public void testRequestLegacyBytesMatchReleasedArtifact() throws IOException {
        assertThat(hex(goldenFullRequest(), LEGACY), equalTo(GOLDEN_REQUEST_FULL));
        assertThat(hex(new ListTasksRequest(), LEGACY), equalTo(GOLDEN_REQUEST_DEFAULT));
    }

    public void testRequestReadsReleasedArtifactBytes() throws IOException {
        final ListTasksRequest full = read(GOLDEN_REQUEST_FULL, LEGACY, ListTasksRequest::new);
        assertThat(full.getParentTask(), equalTo(new TaskId("coordinator", 9)));
        assertArrayEquals(new String[] { "n1", "n2" }, full.getNodes());
        assertArrayEquals(new String[] { "indices:data/write/*", "cluster:monitor/tasks/lists" }, full.getActions());
        assertThat(full.getParentTaskId(), equalTo(new TaskId("n1", 7)));
        assertThat(full.getTimeout(), equalTo(TimeValue.timeValueSeconds(5)));
        assertThat(full.getDetailed(), equalTo(true));
        assertThat(full.getWaitForCompletion(), equalTo(true));

        final ListTasksRequest dflt = read(GOLDEN_REQUEST_DEFAULT, LEGACY, ListTasksRequest::new);
        assertThat(dflt.getDetailed(), equalTo(false));
        assertThat(dflt.getWaitForCompletion(), equalTo(false));
    }

    public void testRequestRoundTripsOnBothEncodings() throws IOException {
        for (Version v : List.of(LEGACY, ListTasksRequest.PROTO_SINCE, Version.CURRENT)) {
            final ListTasksRequest original = randomRequest();
            final ListTasksRequest copy = roundTrip(original, v, ListTasksRequest::new);
            assertThat("version " + v, copy.message(), equalTo(original.message()));
            assertThat(copy.getParentTask(), equalTo(original.getParentTask()));
            assertThat(copy.getTaskId(), equalTo(original.getTaskId()));
            assertThat(copy.getParentTaskId(), equalTo(original.getParentTaskId()));
            assertArrayEquals(original.getNodes(), copy.getNodes());
            assertArrayEquals(original.getActions(), copy.getActions());
            assertThat(copy.getTimeout(), equalTo(original.getTimeout()));
        }
    }

    public void testUnknownRequestFieldsSurviveProtoRoundTrip() throws IOException {
        final UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
            .addField(200, UnknownFieldSet.Field.newBuilder().addVarint(42).build())
            .build();
        final ListTasksRequest original = new ListTasksRequest(randomRequest().message().toBuilder().setUnknownFields(unknown).build());
        final ListTasksRequest copy = roundTrip(original, Version.CURRENT, ListTasksRequest::new);
        assertThat(copy.message().getUnknownFields().getField(200).getVarintList(), equalTo(List.of(42L)));
        final ListTasksRequest legacyCopy = roundTrip(original, LEGACY, ListTasksRequest::new);
        assertThat(legacyCopy.message().getUnknownFields().asMap().isEmpty(), equalTo(true));
    }

    // ----- task info: legacy layout vs. released 3.9.0 -----

    public void testTaskInfoLegacyBytesMatchReleasedArtifact() throws IOException {
        assertThat(hex(goldenFullTaskInfo(), LEGACY), equalTo(GOLDEN_TASK_INFO_FULL));
        assertThat(hex(goldenMinimalTaskInfo(), LEGACY), equalTo(GOLDEN_TASK_INFO_MINIMAL));
        assertThat(hex(goldenRawStatusTaskInfo(), LEGACY), equalTo(GOLDEN_TASK_INFO_RAW_STATUS));
    }

    public void testTaskInfoReadsReleasedArtifactBytes() throws IOException {
        assertThat(read(GOLDEN_TASK_INFO_FULL, LEGACY, TaskInfo::new), equalTo(goldenFullTaskInfo()));
        assertThat(read(GOLDEN_TASK_INFO_MINIMAL, LEGACY, TaskInfo::new), equalTo(goldenMinimalTaskInfo()));
        assertThat(read(GOLDEN_TASK_INFO_RAW_STATUS, LEGACY, TaskInfo::new), equalTo(goldenRawStatusTaskInfo()));

        final TaskInfo full = read(GOLDEN_TASK_INFO_FULL, LEGACY, TaskInfo::new);
        assertThat(full.getStatus(), equalTo(new ReplicationTask.Status("finished")));
        assertThat(full.getHeaders(), equalTo(Map.of("X-Opaque-Id", "abc")));
        assertThat(full.getResourceStats().getThreadUsage().getThreadExecutions(), equalTo(3));
        assertThat(full.getCancellationStartTime(), equalTo(1700000000777L));

        final TaskInfo minimal = read(GOLDEN_TASK_INFO_MINIMAL, LEGACY, TaskInfo::new);
        assertThat(minimal.getStatus(), nullValue());
        assertThat(minimal.getDescription(), nullValue());
        assertThat(minimal.getResourceStats(), nullValue());
        assertThat(minimal.getCancellationStartTime(), nullValue());
        assertThat(minimal.getParentTaskId(), equalTo(TaskId.EMPTY_TASK_ID));
    }

    public void testTaskInfoRoundTripsOnBothEncodings() throws IOException {
        for (Version v : List.of(LEGACY, TaskInfo.PROTO_SINCE, Version.CURRENT)) {
            for (TaskInfo original : List.of(goldenFullTaskInfo(), goldenMinimalTaskInfo(), goldenRawStatusTaskInfo())) {
                final TaskInfo copy = roundTrip(original, v, TaskInfo::new);
                assertThat("version " + v, copy, equalTo(original));
                assertThat(copy.toString(), equalTo(original.toString()));
            }
            // random instances may carry several headers; the legacy layout does not preserve map order (it never did),
            // so compare by equality only
            final TaskInfo original = randomTaskInfo();
            assertThat("version " + v, roundTrip(original, v, TaskInfo::new), equalTo(original));
        }
    }

    // ----- response: legacy layout vs. released 3.9.0 -----

    public void testResponseLegacyBytesMatchReleasedArtifact() throws IOException {
        assertThat(hex(goldenResponse(), LEGACY), equalTo(GOLDEN_RESPONSE));
    }

    public void testResponseReadsReleasedArtifactBytes() throws IOException {
        final ListTasksResponse response = read(GOLDEN_RESPONSE, LEGACY, ListTasksResponse::new);
        assertThat(response.getTasks(), equalTo(goldenResponse().getTasks()));
        assertThat(response.getTaskFailures(), hasSize(1));
        assertThat(response.getTaskFailures().get(0).getNodeId(), equalTo("n1"));
        assertThat(response.getTaskFailures().get(0).getTaskId(), equalTo(11L));
        assertThat(response.getTaskFailures().get(0).getCause(), instanceOf(IllegalStateException.class));
        assertThat(response.getTaskFailures().get(0).getCause().getMessage(), equalTo("boom"));
        assertThat(response.getNodeFailures(), hasSize(1));
        assertThat(response.getNodeFailures().get(0), instanceOf(FailedNodeException.class));
        assertThat(((FailedNodeException) response.getNodeFailures().get(0)).nodeId(), equalTo("n3"));
        assertThat(response.getNodeFailures().get(0).getMessage(), equalTo("node down"));
    }

    public void testResponseRoundTripsOnBothEncodings() throws IOException {
        for (Version v : List.of(LEGACY, ListTasksResponse.PROTO_SINCE, Version.CURRENT)) {
            final ListTasksResponse original = goldenResponse();
            final ListTasksResponse copy = roundTrip(original, v, ListTasksResponse::new);
            assertThat("version " + v, copy.getTasks(), equalTo(original.getTasks()));
            assertThat(copy.getTaskFailures(), hasSize(1));
            assertThat(copy.getNodeFailures(), hasSize(1));
            assertThat(copy.toString(), equalTo(original.toString()));
        }
    }

    /** On the proto encoding, the response payload is exactly the message built for that peer. */
    public void testProtoEncodingIsTheMessage() throws IOException {
        final ListTasksResponse response = goldenResponse();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.CURRENT);
            response.writeTo(out);
            final byte[] all = BytesReference.toBytes(out.bytes());
            final byte[] payload = response.message(Version.CURRENT).toByteArray();
            final byte[] tail = new byte[payload.length];
            System.arraycopy(all, all.length - payload.length, tail, 0, payload.length);
            assertArrayEquals(payload, tail);
        }
    }

    // ----- the NamedWriteable frame -----

    /**
     * An unmodified status (legacy bytes inside the frame) keeps its own version rules: the frame is built for the
     * peer, so a field this status only writes to newer peers is omitted for an older one. This is what lets a plugin's
     * registrant work in a protobuf container without changing.
     */
    public void testUnmodifiedStatusKeepsItsVersionRulesInsideTheFrame() throws IOException {
        final GuardedStatus status = new GuardedStatus("base", "only-for-newer-peers");
        final TaskInfo original = taskInfoWithStatus(status);

        // to a peer at this version, the guarded field is not written, so it reads back as absent
        final TaskInfo currentCopy = roundTrip(original, Version.CURRENT, TaskInfo::new);
        assertThat(currentCopy.getStatus(), equalTo(new GuardedStatus("base", null)));

        // to a peer that understands the guarded field it is present
        final TaskInfo futureCopy = roundTrip(original, FUTURE, TaskInfo::new);
        assertThat(futureCopy.getStatus(), equalTo(status));
    }

    /**
     * An opted-in status writes its protobuf message inside the frame for a new enough peer and its legacy layout
     * otherwise, without the frame knowing which.
     */
    public void testOptedInStatusChoosesItsOwnEncodingInsideTheFrame() throws IOException {
        final ReplicationTask.Status status = new ReplicationTask.Status("finished");

        final NamedWriteableProto.NamedWriteable protoFrame = NamedWriteableFrames.toProto(status, Version.CURRENT);
        assertThat(protoFrame.getName(), equalTo(ReplicationTask.Status.NAME));
        try (StreamInput payload = StreamInput.wrap(protoFrame.getPayload().toByteArray())) {
            payload.setVersion(Version.CURRENT);
            final ReplicationTaskStatusProto.ReplicationTaskStatus message = ReplicationTaskStatusProto.ReplicationTaskStatus.parseFrom(
                payload.readByteArray()
            );
            assertThat(message.getPhase(), equalTo("finished"));
            assertThat(payload.available(), equalTo(0));
        }

        final NamedWriteableProto.NamedWriteable legacyFrame = NamedWriteableFrames.toProto(status, LEGACY);
        try (StreamInput payload = StreamInput.wrap(legacyFrame.getPayload().toByteArray())) {
            payload.setVersion(LEGACY);
            assertThat(payload.readString(), equalTo("finished"));
            assertThat(payload.available(), equalTo(0));
        }
    }

    public void testFrameWithUnknownNameFailsAtTheFrame() throws IOException {
        final NamedWriteableProto.NamedWriteable frame = NamedWriteableFrames.toProto(
            new RawTaskStatus(new BytesArray("{}")),
            Version.CURRENT
        ).toBuilder().setName("no-such-status").build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> NamedWriteableFrames.fromProto(frame, Task.Status.class, registryInput(new byte[0], Version.CURRENT))
        );
        assertThat(e.getMessage(), containsString("no-such-status"));
    }

    public void testFrameWithTrailingBytesFailsAtTheFrame() throws IOException {
        final NamedWriteableProto.NamedWriteable good = NamedWriteableFrames.toProto(new ReplicationTask.Status("x"), Version.CURRENT);
        final NamedWriteableProto.NamedWriteable bad = good.toBuilder().setPayload(good.getPayload().concat(good.getPayload())).build();
        final IOException e = expectThrows(
            IOException.class,
            () -> NamedWriteableFrames.fromProto(bad, Task.Status.class, registryInput(new byte[0], Version.CURRENT))
        );
        assertThat(e.getMessage(), containsString("unread bytes"));
    }

    // ----- fixtures (must stay identical to the generator that produced the golden bytes) -----

    static ListTasksRequest goldenFullRequest() {
        final ListTasksRequest request = new ListTasksRequest();
        request.setParentTask(new TaskId("coordinator", 9));
        request.setNodes("n1", "n2");
        request.setActions("indices:data/write/*", "cluster:monitor/tasks/lists");
        request.setParentTaskId(new TaskId("n1", 7));
        request.setTimeout(TimeValue.timeValueSeconds(5));
        request.setDetailed(true);
        request.setWaitForCompletion(true);
        return request;
    }

    static TaskInfo goldenFullTaskInfo() {
        final Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Opaque-Id", "abc");
        final Map<String, TaskResourceUsage> usage = new LinkedHashMap<>();
        usage.put("total", new TaskResourceUsage(1000, 2048));
        return new TaskInfo(
            new TaskId("n1", 42),
            "transport",
            "indices:data/write/bulk[s]",
            "requests[1], index[idx]",
            new ReplicationTask.Status("finished"),
            1700000000000L,
            123456789L,
            true,
            true,
            new TaskId("n1", 5),
            headers,
            new TaskResourceStats(usage, new TaskThreadUsage(3, 1)),
            1700000000777L
        );
    }

    static TaskInfo goldenMinimalTaskInfo() {
        return new TaskInfo(
            new TaskId("n2", 1),
            "direct",
            "cluster:monitor/tasks/lists[n]",
            null,
            null,
            1L,
            2L,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            null,
            null
        );
    }

    static TaskInfo goldenRawStatusTaskInfo() {
        return new TaskInfo(
            new TaskId("n2", 2),
            "transport",
            "indices:data/write/reindex",
            null,
            new RawTaskStatus(new BytesArray("{\"total\":10}")),
            3L,
            4L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            null,
            null
        );
    }

    static ListTasksResponse goldenResponse() {
        final IllegalStateException reason = new IllegalStateException("boom");
        reason.setStackTrace(new StackTraceElement[0]);
        final FailedNodeException nodeFailure = new FailedNodeException("n3", "node down", null);
        nodeFailure.setStackTrace(new StackTraceElement[0]);
        return new ListTasksResponse(
            List.of(goldenFullTaskInfo(), goldenMinimalTaskInfo(), goldenRawStatusTaskInfo()),
            List.of(new TaskOperationFailure("n1", 11, reason)),
            List.of(nodeFailure)
        );
    }

    // ----- random instances -----

    static ListTasksRequest randomRequest() {
        final ListTasksRequest request = new ListTasksRequest();
        if (randomBoolean()) {
            request.setParentTask(new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setTaskId(new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()));
        } else {
            request.setNodes(generateRandomStringArray(3, 5, false, true));
        }
        request.setActions(generateRandomStringArray(3, 8, false, true));
        if (randomBoolean()) {
            request.setParentTaskId(new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setTimeout(TimeValue.timeValueMillis(randomIntBetween(1, 100000)));
        }
        request.setDetailed(randomBoolean());
        request.setWaitForCompletion(randomBoolean());
        return request;
    }

    static TaskInfo randomTaskInfo() {
        final boolean cancellable = randomBoolean();
        final Map<String, String> headers = new LinkedHashMap<>();
        for (int i = randomIntBetween(0, 3); i > 0; i--) {
            headers.put(randomAlphaOfLength(4), randomAlphaOfLength(6));
        }
        TaskResourceStats stats = null;
        if (randomBoolean()) {
            final Map<String, TaskResourceUsage> usage = new LinkedHashMap<>();
            for (int i = randomIntBetween(0, 2); i > 0; i--) {
                usage.put(randomAlphaOfLength(5), new TaskResourceUsage(randomNonNegativeLong(), randomNonNegativeLong()));
            }
            stats = new TaskResourceStats(usage, new TaskThreadUsage(randomIntBetween(0, 10), randomIntBetween(0, 10)));
        }
        return new TaskInfo(
            new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()),
            randomAlphaOfLength(5),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(20),
            randomStatus(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            cancellable,
            cancellable && randomBoolean(),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()),
            headers,
            stats,
            randomBoolean() ? null : randomNonNegativeLong()
        );
    }

    private static Task.Status randomStatus() {
        switch (randomIntBetween(0, 3)) {
            case 0:
                return null;
            case 1:
                return new ReplicationTask.Status(randomAlphaOfLength(6));
            case 2:
                return new RawTaskStatus(new BytesArray("{\"n\":" + randomInt() + "}"));
            default:
                return new GuardedStatus(randomAlphaOfLength(4), null);
        }
    }

    private static TaskInfo taskInfoWithStatus(Task.Status status) {
        return new TaskInfo(
            new TaskId("n1", 1),
            "transport",
            "test",
            null,
            status,
            1,
            2,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            null
        );
    }

    /**
     * A status that only writes its second field to peers at or after {@link #FUTURE}; stands in for a plugin's
     * registrant with a version guard and no knowledge of protobuf.
     */
    static final class GuardedStatus implements Task.Status {
        static final String NAME = "guarded";

        private final String base;
        private final String newer;

        GuardedStatus(String base, String newer) {
            this.base = base;
            this.newer = newer;
        }

        GuardedStatus(StreamInput in) throws IOException {
            base = in.readString();
            newer = in.getVersion().onOrAfter(FUTURE) ? in.readOptionalString() : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(base);
            if (out.getVersion().onOrAfter(FUTURE)) {
                out.writeOptionalString(newer);
            }
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("base", base);
            if (newer != null) {
                builder.field("newer", newer);
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof GuardedStatus
                && Objects.equals(base, ((GuardedStatus) o).base)
                && Objects.equals(newer, ((GuardedStatus) o).newer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(base, newer);
        }

        @Override
        public String toString() {
            return "GuardedStatus{" + base + "," + newer + "}";
        }
    }

    // ----- helpers -----

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        List.of(
            new NamedWriteableRegistry.Entry(Task.Status.class, ReplicationTask.Status.NAME, ReplicationTask.Status::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, GuardedStatus.NAME, GuardedStatus::new)
        )
    );

    private static StreamInput registryInput(byte[] bytes, Version version) {
        final StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(bytes), REGISTRY);
        in.setVersion(version);
        return in;
    }

    private static String hex(Writeable w, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            w.writeTo(out);
            return HexFormat.of().formatHex(BytesReference.toBytes(out.bytes()));
        }
    }

    private static <T> T read(String hex, Version version, Writeable.Reader<T> reader) throws IOException {
        try (StreamInput in = registryInput(HexFormat.of().parseHex(hex), version)) {
            final T value = reader.read(in);
            assertThat("unread bytes", in.available(), equalTo(0));
            return value;
        }
    }

    private static <T extends Writeable> T roundTrip(T original, Version version, Writeable.Reader<T> reader) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            original.writeTo(out);
            try (StreamInput in = registryInput(BytesReference.toBytes(out.bytes()), version)) {
                final T value = reader.read(in);
                assertThat("unread bytes", in.available(), equalTo(0));
                return value;
            }
        }
    }
}
