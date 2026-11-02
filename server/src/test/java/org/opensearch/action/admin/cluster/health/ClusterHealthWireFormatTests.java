/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.health;

import com.google.protobuf.UnknownFieldSet;
import org.opensearch.Version;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.Priority;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HexFormat;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Pins the two encodings of {@link ClusterHealthRequest} and {@link ClusterHealthResponse}.
 * <p>
 * The {@code GOLDEN_*} constants were produced by serialising the same instances with the released
 * {@code org.opensearch:opensearch:3.9.0} artifact at stream version 3.9.0 (see the generator in the PR
 * description). They are what a real 3.9 peer emits and expects, so {@code writeLegacy}/{@code readLegacy}
 * must match them byte for byte.
 */
public class ClusterHealthWireFormatTests extends OpenSearchTestCase {

    private static final Version LEGACY = Version.V_3_9_0;

    private static final String GOLDEN_REQUEST_FULL =
        "0052030102056964782d61056964782d62220301010100000003033e3d32010500040001030402000100020001";
    private static final String GOLDEN_REQUEST_AWARENESS = "003c0300003c030000000000000000000200020300010201047a6f6e65030000";
    private static final String GOLDEN_REQUEST_DEFAULT = "003c0300003c030000000000000000000200020300010200000000";
    private static final String GOLDEN_RESPONSE_SHARDS =
        "06676f6c64656e0101020000020202010101056964782d610201010200000201020000020000000101010000000200404900000000000000000005010000000200000001a4130200";
    private static final String GOLDEN_RESPONSE_SHARDS_JSON = "{\"cluster_name\":\"golden\",\"status\":\"yellow\",\"timed_out\":true,"
        + "\"number_of_nodes\":2,\"number_of_data_nodes\":2,\"discovered_master\":true,\"discovered_cluster_manager\":true,"
        + "\"active_primary_shards\":1,\"active_shards\":2,\"relocating_shards\":0,\"initializing_shards\":0,\"unassigned_shards\":2,"
        + "\"delayed_unassigned_shards\":1,\"number_of_pending_tasks\":5,\"number_of_in_flight_fetch\":2,"
        + "\"task_max_waiting_in_queue_millis\":1234,\"active_shards_percent_as_number\":50.0}";
    private static final String GOLDEN_RESPONSE_RED =
        "06676f6c64656e0200000000000202010000405900000000000000000000000000000000000000000200";

    // ----- request: legacy layout vs. released 3.9.0 -----

    public void testRequestLegacyBytesMatchReleasedSerializer() throws IOException {
        assertThat(hex(goldenFullRequest(), LEGACY), equalTo(GOLDEN_REQUEST_FULL));
        assertThat(hex(goldenAwarenessRequest(), LEGACY), equalTo(GOLDEN_REQUEST_AWARENESS));
        assertThat(hex(new ClusterHealthRequest(), LEGACY), equalTo(GOLDEN_REQUEST_DEFAULT));
    }

    public void testRequestReadsBytesFromReleasedSerializer() throws IOException {
        final ClusterHealthRequest full = read(GOLDEN_REQUEST_FULL, LEGACY, ClusterHealthRequest::new);
        assertArrayEquals(new String[] { "idx-a", "idx-b" }, full.indices());
        assertThat(full.indicesOptions(), equalTo(IndicesOptions.fromOptions(true, false, true, true, false, false, true, true, false)));
        assertThat(full.timeout(), equalTo(TimeValue.timeValueSeconds(17)));
        assertThat(full.waitForStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(full.waitForNoRelocatingShards(), equalTo(true));
        assertThat(full.waitForNoInitializingShards(), equalTo(false));
        assertThat(full.waitForActiveShards(), equalTo(ActiveShardCount.from(3)));
        assertThat(full.waitForNodes(), equalTo(">=2"));
        assertThat(full.waitForEvents(), equalTo(Priority.LANGUID));
        assertThat(full.ensureNodeWeighedIn(), equalTo(false));
        assertThat(full.level(), equalTo(ClusterHealthRequest.Level.SHARDS));
        assertThat(full.isApplyLevelAtTransportLayer(), equalTo(true));
        assertThat(full.getAwarenessAttribute(), nullValue());
        assertThat(full.local(), equalTo(true));
        assertThat(full.clusterManagerNodeTimeout(), equalTo(TimeValue.timeValueSeconds(41)));

        final ClusterHealthRequest awareness = read(GOLDEN_REQUEST_AWARENESS, LEGACY, ClusterHealthRequest::new);
        assertThat(awareness.level(), equalTo(ClusterHealthRequest.Level.AWARENESS_ATTRIBUTES));
        assertThat(awareness.getAwarenessAttribute(), equalTo("zone"));
        assertThat(awareness.indices().length, equalTo(0));

        final ClusterHealthRequest dflt = read(GOLDEN_REQUEST_DEFAULT, LEGACY, ClusterHealthRequest::new);
        final ClusterHealthRequest fresh = new ClusterHealthRequest();
        assertArrayEquals(fresh.indices(), dflt.indices());
        assertThat(dflt.indicesOptions(), equalTo(fresh.indicesOptions()));
        assertThat(dflt.timeout(), equalTo(fresh.timeout()));
        assertThat(dflt.waitForStatus(), equalTo(fresh.waitForStatus()));
        assertThat(dflt.waitForActiveShards(), equalTo(fresh.waitForActiveShards()));
        assertThat(dflt.waitForNodes(), equalTo(fresh.waitForNodes()));
        assertThat(dflt.waitForEvents(), equalTo(fresh.waitForEvents()));
        assertThat(dflt.level(), equalTo(fresh.level()));
        assertThat(dflt.getAwarenessAttribute(), equalTo(fresh.getAwarenessAttribute()));
        assertThat(dflt.local(), equalTo(fresh.local()));
        assertThat(dflt.clusterManagerNodeTimeout(), equalTo(fresh.clusterManagerNodeTimeout()));
    }

    // ----- response: legacy layout vs. released 3.9.0 -----

    public void testResponseLegacyBytesMatchReleasedSerializer() throws IOException {
        assertThat(hex(goldenShardsResponse(), LEGACY), equalTo(GOLDEN_RESPONSE_SHARDS));
        assertThat(hex(goldenRedResponse(), LEGACY), equalTo(GOLDEN_RESPONSE_RED));
    }

    public void testResponseReadsBytesFromReleasedSerializer() throws IOException {
        final ClusterHealthResponse shards = read(GOLDEN_RESPONSE_SHARDS, LEGACY, ClusterHealthResponse::new);
        assertThat(shards.toString(), equalTo(GOLDEN_RESPONSE_SHARDS_JSON));
        assertThat(shards.getIndices().get("idx-a").getShards().size(), equalTo(2));
        assertThat(shards.getIndices().get("idx-a").getShards().get(1).getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(shards.getIndices().get("idx-a").getShards().get(1).isPrimaryActive(), equalTo(false));

        final ClusterHealthResponse red = read(GOLDEN_RESPONSE_RED, LEGACY, ClusterHealthResponse::new);
        assertThat(red.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(red.getClusterStateHealth().getStatus(), equalTo(ClusterHealthStatus.GREEN)); // override is separate from derived status
        assertThat(red.getIndices().isEmpty(), equalTo(true));
    }

    public void testResponseJsonMatchesReleasedSerializer() {
        assertThat(goldenShardsResponse().toString(), equalTo(GOLDEN_RESPONSE_SHARDS_JSON));
    }

    // ----- both encodings round trip -----

    public void testRequestRoundTripsOnBothEncodings() throws IOException {
        for (Version v : List.of(LEGACY, Version.CURRENT, ClusterHealthRequest.PROTO_SINCE)) {
            final ClusterHealthRequest original = randomRequest();
            final ClusterHealthRequest copy = roundTrip(original, v, ClusterHealthRequest::new);
            assertThat("version " + v, copy.message(), equalTo(original.message()));
            assertThat(copy.local(), equalTo(original.local()));
            assertThat(copy.clusterManagerNodeTimeout(), equalTo(original.clusterManagerNodeTimeout()));
            assertThat(copy.getParentTask(), equalTo(original.getParentTask()));
        }
    }

    public void testResponseRoundTripsOnBothEncodings() throws IOException {
        for (Version v : List.of(LEGACY, Version.CURRENT)) {
            for (ClusterHealthRequest.Level level : ClusterHealthRequest.Level.values()) {
                final ClusterHealthResponse original = randomResponse(level);
                final ClusterHealthResponse copy = roundTrip(original, v, ClusterHealthResponse::new);
                assertThat("version " + v + " level " + level, copy, equalTo(original));
                assertThat(copy.toString(), equalTo(original.toString()));
            }
        }
    }

    /** On the proto encoding, the action-specific payload is exactly the serialized message. */
    public void testProtoEncodingIsTheMessage() throws IOException {
        final ClusterHealthRequest request = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.CURRENT);
            request.writeTo(out);
            final byte[] all = BytesReference.toBytes(out.bytes());
            final byte[] payload = request.message().toByteArray();
            final byte[] tail = new byte[payload.length];
            System.arraycopy(all, all.length - payload.length, tail, 0, payload.length);
            assertArrayEquals(payload, tail);
        }
    }

    /**
     * Fields a newer node adds to the schema survive a hop through this node unchanged. The legacy stream cannot
     * offer this; it is the reason for the whole exercise.
     */
    public void testUnknownFieldsSurviveProtoRoundTrip() throws IOException {
        final UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
            .addField(200, UnknownFieldSet.Field.newBuilder().addVarint(42).build())
            .build();
        final ClusterHealthRequest original = new ClusterHealthRequest(
            randomRequest().message().toBuilder().setUnknownFields(unknown).build()
        );
        final ClusterHealthRequest copy = roundTrip(original, Version.CURRENT, ClusterHealthRequest::new);
        assertThat(copy.message().getUnknownFields().getField(200).getVarintList(), equalTo(List.of(42L)));
        // and are dropped, not corrupted, when the peer only speaks legacy
        final ClusterHealthRequest legacyCopy = roundTrip(original, LEGACY, ClusterHealthRequest::new);
        assertThat(legacyCopy.message().getUnknownFields().asMap().isEmpty(), equalTo(true));
    }

    // ----- fixtures (must stay identical to the generator that produced the golden bytes) -----

    static ClusterHealthRequest goldenFullRequest() {
        final ClusterHealthRequest req = new ClusterHealthRequest("idx-a", "idx-b");
        req.indicesOptions(IndicesOptions.fromOptions(true, false, true, true, false, false, true, true, false));
        req.timeout(TimeValue.timeValueSeconds(17));
        req.waitForStatus(ClusterHealthStatus.YELLOW);
        req.waitForNoRelocatingShards(true);
        req.waitForNoInitializingShards(false);
        req.waitForActiveShards(ActiveShardCount.from(3));
        req.waitForNodes(">=2");
        req.waitForEvents(Priority.LANGUID);
        req.ensureNodeWeighedIn(false);
        req.setLevel("shards");
        req.setApplyLevelAtTransportLayer(true);
        req.local(true);
        req.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(41));
        return req;
    }

    static ClusterHealthRequest goldenAwarenessRequest() {
        final ClusterHealthRequest req = new ClusterHealthRequest();
        req.setLevel("awareness_attributes");
        req.setAwarenessAttribute("zone");
        return req;
    }

    static ClusterState goldenState() {
        final DiscoveryNode node = new DiscoveryNode("n1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), LEGACY);
        final DiscoveryNode node2 = new DiscoveryNode("n2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301), LEGACY);
        final IndexMetadata im = IndexMetadata.builder("idx-a")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, LEGACY).put(IndexMetadata.SETTING_INDEX_UUID, "uuid-a"))
            .numberOfShards(2)
            .numberOfReplicas(1)
            .build();
        final IndexRoutingTable.Builder rt = new IndexRoutingTable.Builder(im.getIndex());
        final ShardId s0 = new ShardId(im.getIndex(), 0);
        final ShardId s1 = new ShardId(im.getIndex(), 1);
        final UnassignedInfo ui = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "golden");
        rt.addShard(
            ShardRouting.newUnassigned(s0, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE, ui)
                .initialize("n1", null, 0)
                .moveToStarted()
        );
        rt.addShard(
            ShardRouting.newUnassigned(s0, false, RecoverySource.PeerRecoverySource.INSTANCE, ui).initialize("n2", null, 0).moveToStarted()
        );
        rt.addShard(ShardRouting.newUnassigned(s1, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE, ui));
        rt.addShard(ShardRouting.newUnassigned(s1, false, RecoverySource.PeerRecoverySource.INSTANCE, ui));
        return ClusterState.builder(new ClusterName("golden"))
            .nodes(DiscoveryNodes.builder().add(node).add(node2).localNodeId("n1").clusterManagerNodeId("n1"))
            .metadata(Metadata.builder().put(im, false))
            .routingTable(RoutingTable.builder().add(rt.build()).build())
            .build();
    }

    static ClusterHealthResponse goldenShardsResponse() {
        final ClusterHealthResponse resp = new ClusterHealthResponse(
            "golden",
            new String[] { "idx-a" },
            goldenState(),
            5,
            2,
            1,
            TimeValue.timeValueMillis(1234)
        );
        resp.setTimedOut(true);
        return resp;
    }

    static ClusterHealthResponse goldenRedResponse() {
        final ClusterHealthResponse red = new ClusterHealthResponse("golden", new String[0], goldenState(), 0, 0, 0, TimeValue.ZERO);
        red.setStatus(ClusterHealthStatus.RED);
        return red;
    }

    static ClusterHealthRequest randomRequest() {
        final ClusterHealthRequest request = new ClusterHealthRequest(generateRandomStringArray(5, 8, false, false));
        request.indicesOptions(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            )
        );
        request.timeout(TimeValue.parseTimeValue(randomTimeValue(), "test"));
        if (randomBoolean()) {
            request.waitForStatus(randomFrom(ClusterHealthStatus.values()));
        }
        request.waitForNoRelocatingShards(randomBoolean());
        request.waitForNoInitializingShards(randomBoolean());
        request.waitForActiveShards(randomFrom(ActiveShardCount.ALL, ActiveShardCount.NONE, ActiveShardCount.from(randomIntBetween(1, 9))));
        request.waitForNodes(randomBoolean() ? "" : randomFrom(">=", "<=", ">", "<", "") + randomIntBetween(1, 9));
        if (randomBoolean()) {
            request.waitForEvents(randomFrom(Priority.values()));
        }
        request.ensureNodeWeighedIn(randomBoolean());
        request.level(randomFrom(ClusterHealthRequest.Level.values()));
        request.setApplyLevelAtTransportLayer(randomBoolean());
        if (randomBoolean()) {
            request.setAwarenessAttribute(randomAlphaOfLength(5));
        }
        request.local(randomBoolean());
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(randomIntBetween(1, 100)));
        return request;
    }

    static ClusterHealthResponse randomResponse(ClusterHealthRequest.Level level) {
        final ClusterHealthResponse response = new ClusterHealthResponsesTests().createTestInstanceForLevel(level);
        if (randomBoolean()) {
            response.setStatus(randomFrom(ClusterHealthStatus.values()));
        }
        return response;
    }

    // ----- helpers -----

    private static String hex(Writeable w, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            w.writeTo(out);
            return HexFormat.of().formatHex(BytesReference.toBytes(out.bytes()));
        }
    }

    private static <T> T read(String hex, Version version, Writeable.Reader<T> reader) throws IOException {
        try (StreamInput in = StreamInput.wrap(HexFormat.of().parseHex(hex))) {
            in.setVersion(version);
            return reader.read(in);
        }
    }

    private static <T extends Writeable> T roundTrip(T original, Version version, Writeable.Reader<T> reader) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                return reader.read(in);
            }
        }
    }
}
