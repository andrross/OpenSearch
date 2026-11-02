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

package org.opensearch.action.admin.cluster.health;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.cluster.health.proto.ClusterHealthRequestProto;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.action.support.protoaction.ProtoConverters;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.ProtobufPayload;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for requesting cluster health.
 * <p>
 * State is held in a {@link ClusterHealthRequestProto.ClusterHealthRequest} message. Between nodes at or after
 * {@link #PROTO_SINCE} the message is the wire payload (see {@link ProtobufPayload}); older peers receive the legacy
 * stream layout, which is frozen in {@link #writeLegacy}/{@link #readLegacy}. The public API of this class is
 * unchanged.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterHealthRequest extends ClusterManagerNodeReadRequest<ClusterHealthRequest>
    implements
        IndicesRequest.Replaceable,
        ProtobufPayload {

    /** First version that encodes this request as protobuf on the wire. */
    public static final Version PROTO_SINCE = Version.V_3_10_0;

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.lenientExpandHidden();

    private ClusterHealthRequestProto.ClusterHealthRequest message;

    public ClusterHealthRequest() {
        this(ClusterHealthRequestProto.ClusterHealthRequest.getDefaultInstance());
    }

    public ClusterHealthRequest(String... indices) {
        this(ClusterHealthRequestProto.ClusterHealthRequest.newBuilder().addAllIndices(Arrays.asList(indices)).build());
    }

    /** Wrap an existing message. Package-private: callers outside this package use the public constructors and setters. */
    ClusterHealthRequest(ClusterHealthRequestProto.ClusterHealthRequest message) {
        this.message = Objects.requireNonNull(message, "message");
    }

    public ClusterHealthRequest(StreamInput in) throws IOException {
        super(in); // header fields
        this.message = ProtobufPayload.readPayload(
            in,
            PROTO_SINCE,
            ClusterHealthRequestProto.ClusterHealthRequest::parseFrom,
            ClusterHealthRequest::readLegacy
        );
    }

    // header fields and payload are written by TransportRequest#writeTo; kept here for binary compatibility
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public Version protobufSince() {
        return PROTO_SINCE;
    }

    @Override
    public byte[] protobufBytes() {
        return message.toByteArray();
    }

    /**
     * The current message. Immutable; setters replace it. Package-private: code outside this class reads the request
     * through its accessors, which apply defaults and convert to domain types.
     */
    ClusterHealthRequestProto.ClusterHealthRequest message() {
        return message;
    }

    private ClusterHealthRequest update(UnaryOperator<ClusterHealthRequestProto.ClusterHealthRequest> change) {
        this.message = Objects.requireNonNull(change.apply(message), "message");
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + message + "}";
    }

    // ----- frozen legacy layout -----

    private static ClusterHealthRequestProto.ClusterHealthRequest readLegacy(StreamInput in) throws IOException {
        final ClusterHealthRequestProto.ClusterHealthRequest.Builder b = ClusterHealthRequestProto.ClusterHealthRequest.newBuilder();
        b.addAllIndices(Arrays.asList(in.readStringArray()));
        b.setTimeout(ProtoConverters.toProto(in.readTimeValue()));
        if (in.readBoolean()) {
            b.setWaitForStatus(ProtoConverters.toProto(ClusterHealthStatus.fromValue(in.readByte())));
        }
        b.setWaitForNoRelocatingShards(in.readBoolean());
        b.setWaitForActiveShards(ProtoConverters.toProto(ActiveShardCount.readFrom(in)));
        b.setWaitForNodes(in.readString());
        if (in.readBoolean()) {
            b.setWaitForEvents(ProtoConverters.toProto(Priority.readFrom(in)));
        }
        b.setWaitForNoInitializingShards(in.readBoolean());
        b.setIndicesOptions(ProtoConverters.toProto(IndicesOptions.readIndicesOptions(in)));
        if (in.getVersion().onOrAfter(Version.V_2_5_0)) {
            final String awarenessAttribute = in.readOptionalString();
            if (awarenessAttribute != null) {
                b.setAwarenessAttribute(awarenessAttribute);
            }
            b.setLevel(toProto(in.readEnum(Level.class)));
        }
        if (in.getVersion().onOrAfter(Version.V_2_6_0)) {
            b.setEnsureNodeWeighedIn(in.readBoolean());
        }
        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            b.setApplyLevelAtTransportLayer(in.readBoolean());
        }
        return b.build();
    }

    @Override
    protected void writeLegacy(StreamOutput out) throws IOException {
        final ClusterHealthRequestProto.ClusterHealthRequest m = message;
        out.writeStringArray(m.getIndicesList().toArray(new String[0]));
        out.writeTimeValue(timeout());
        if (m.hasWaitForStatus()) {
            out.writeBoolean(true);
            out.writeByte(ProtoConverters.fromProto(m.getWaitForStatus()).value());
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(m.getWaitForNoRelocatingShards());
        waitForActiveShards().writeTo(out);
        out.writeString(m.getWaitForNodes());
        if (m.hasWaitForEvents()) {
            out.writeBoolean(true);
            Priority.writeTo(ProtoConverters.fromProto(m.getWaitForEvents()), out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(m.getWaitForNoInitializingShards());
        indicesOptions().writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(Version.V_2_5_0)) {
            out.writeOptionalString(m.hasAwarenessAttribute() ? m.getAwarenessAttribute() : null);
            out.writeEnum(level());
        }
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeBoolean(m.getEnsureNodeWeighedIn());
        }
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeBoolean(m.getApplyLevelAtTransportLayer());
        }
    }

    // ----- public API (unchanged) -----

    @Override
    public String[] indices() {
        return message().getIndicesList().toArray(new String[0]);
    }

    @Override
    public ClusterHealthRequest indices(String... indices) {
        return update(m -> m.toBuilder().clearIndices().addAllIndices(Arrays.asList(indices)).build());
    }

    @Override
    public IndicesOptions indicesOptions() {
        return message().hasIndicesOptions() ? ProtoConverters.fromProto(message().getIndicesOptions()) : DEFAULT_INDICES_OPTIONS;
    }

    public ClusterHealthRequest indicesOptions(final IndicesOptions indicesOptions) {
        return update(m -> m.toBuilder().setIndicesOptions(ProtoConverters.toProto(indicesOptions)).build());
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public TimeValue timeout() {
        return message().hasTimeout() ? ProtoConverters.fromProto(message().getTimeout()) : DEFAULT_TIMEOUT;
    }

    public ClusterHealthRequest timeout(TimeValue timeout) {
        update(m -> m.toBuilder().setTimeout(ProtoConverters.toProto(timeout)).build());
        if (clusterManagerNodeTimeout == DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT) {
            clusterManagerNodeTimeout = timeout;
        }
        return this;
    }

    public ClusterHealthRequest timeout(String timeout) {
        return this.timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public ClusterHealthStatus waitForStatus() {
        return message().hasWaitForStatus() ? ProtoConverters.fromProto(message().getWaitForStatus()) : null;
    }

    public ClusterHealthRequest waitForStatus(ClusterHealthStatus waitForStatus) {
        return update(m -> {
            final ClusterHealthRequestProto.ClusterHealthRequest.Builder b = m.toBuilder();
            if (waitForStatus == null) {
                b.clearWaitForStatus();
            } else {
                b.setWaitForStatus(ProtoConverters.toProto(waitForStatus));
            }
            return b.build();
        });
    }

    public ClusterHealthRequest waitForGreenStatus() {
        return waitForStatus(ClusterHealthStatus.GREEN);
    }

    public ClusterHealthRequest waitForYellowStatus() {
        return waitForStatus(ClusterHealthStatus.YELLOW);
    }

    public boolean waitForNoRelocatingShards() {
        return message().getWaitForNoRelocatingShards();
    }

    /**
     * Sets whether the request should wait for there to be no relocating shards before
     * retrieving the cluster health status.  Defaults to {@code false}, meaning the
     * operation does not wait on there being no more relocating shards.  Set to <code>true</code>
     * to wait until the number of relocating shards in the cluster is 0.
     */
    public ClusterHealthRequest waitForNoRelocatingShards(boolean waitForNoRelocatingShards) {
        return update(m -> m.toBuilder().setWaitForNoRelocatingShards(waitForNoRelocatingShards).build());
    }

    public boolean waitForNoInitializingShards() {
        return message().getWaitForNoInitializingShards();
    }

    /**
     * Sets whether the request should wait for there to be no initializing shards before
     * retrieving the cluster health status.  Defaults to {@code false}, meaning the
     * operation does not wait on there being no more initializing shards.  Set to <code>true</code>
     * to wait until the number of initializing shards in the cluster is 0.
     */
    public ClusterHealthRequest waitForNoInitializingShards(boolean waitForNoInitializingShards) {
        return update(m -> m.toBuilder().setWaitForNoInitializingShards(waitForNoInitializingShards).build());
    }

    public ActiveShardCount waitForActiveShards() {
        if (message().hasWaitForActiveShards() == false) {
            return ActiveShardCount.NONE;
        }
        final ActiveShardCount count = ProtoConverters.fromProto(message().getWaitForActiveShards());
        // the default for cluster health request is 0, not 1
        return count.equals(ActiveShardCount.DEFAULT) ? ActiveShardCount.NONE : count;
    }

    /**
     * Sets the number of shard copies that must be active across all indices before getting the
     * health status. Defaults to {@link ActiveShardCount#NONE}, meaning we don't wait on any active shards.
     * Set this value to {@link ActiveShardCount#ALL} to wait for all shards (primary and
     * all replicas) to be active across all indices in the cluster. Otherwise, use
     * {@link ActiveShardCount#from(int)} to set this value to any non-negative integer, up to the
     * total number of shard copies to wait for.
     */
    public ClusterHealthRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        // the default for cluster health request is 0, not 1
        final ActiveShardCount effective = waitForActiveShards.equals(ActiveShardCount.DEFAULT)
            ? ActiveShardCount.NONE
            : waitForActiveShards;
        return update(m -> m.toBuilder().setWaitForActiveShards(ProtoConverters.toProto(effective)).build());
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public ClusterHealthRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public String waitForNodes() {
        return message().getWaitForNodes();
    }

    /**
     * Waits for N number of nodes. Use "12" for exact mapping, "&gt;12" and "&lt;12" for range.
     */
    public ClusterHealthRequest waitForNodes(String waitForNodes) {
        return update(m -> m.toBuilder().setWaitForNodes(waitForNodes).build());
    }

    public ClusterHealthRequest waitForEvents(Priority waitForEvents) {
        return update(m -> {
            final ClusterHealthRequestProto.ClusterHealthRequest.Builder b = m.toBuilder();
            if (waitForEvents == null) {
                b.clearWaitForEvents();
            } else {
                b.setWaitForEvents(ProtoConverters.toProto(waitForEvents));
            }
            return b.build();
        });
    }

    public Priority waitForEvents() {
        return message().hasWaitForEvents() ? ProtoConverters.fromProto(message().getWaitForEvents()) : null;
    }

    /**
     * Set the level of detail for the health information to be returned.
     * Only used by the high-level REST Client.
     */
    public void level(Level level) {
        Objects.requireNonNull(level, "level must not be null");
        update(m -> m.toBuilder().setLevel(toProto(level)).build());
    }

    public void setLevel(String level) {
        switch (level) {
            case "indices":
                level(ClusterHealthRequest.Level.INDICES);
                break;
            case "shards":
                level(ClusterHealthRequest.Level.SHARDS);
                break;
            case "awareness_attributes":
                level(ClusterHealthRequest.Level.AWARENESS_ATTRIBUTES);
                break;
            default:
                level(ClusterHealthRequest.Level.CLUSTER);
        }
    }

    /**
     * Get the level of detail for the health information to be returned.
     * Only used by the high-level REST Client.
     */
    public Level level() {
        switch (message().getLevel()) {
            case LEVEL_INDICES:
                return Level.INDICES;
            case LEVEL_SHARDS:
                return Level.SHARDS;
            case LEVEL_AWARENESS_ATTRIBUTES:
                return Level.AWARENESS_ATTRIBUTES;
            default:
                return Level.CLUSTER;
        }
    }

    public ClusterHealthRequest setAwarenessAttribute(String awarenessAttribute) {
        return update(m -> {
            final ClusterHealthRequestProto.ClusterHealthRequest.Builder b = m.toBuilder();
            if (awarenessAttribute == null) {
                b.clearAwarenessAttribute();
            } else {
                b.setAwarenessAttribute(awarenessAttribute);
            }
            return b.build();
        });
    }

    public String getAwarenessAttribute() {
        return message().hasAwarenessAttribute() ? message().getAwarenessAttribute() : null;
    }

    public final ClusterHealthRequest ensureNodeWeighedIn(boolean ensureNodeWeighedIn) {
        return update(m -> m.toBuilder().setEnsureNodeWeighedIn(ensureNodeWeighedIn).build());
    }

    /**
     * For a given local request, checks if the local node is commissioned or not (default: false).
     * @return <code>true</code> if local information is to be returned only when local node is also commissioned
     * <code>false</code> to not check local node if commissioned or not for a local request
     */
    public final boolean ensureNodeWeighedIn() {
        return message().getEnsureNodeWeighedIn();
    }

    public boolean isApplyLevelAtTransportLayer() {
        return message().getApplyLevelAtTransportLayer();
    }

    public void setApplyLevelAtTransportLayer(boolean applyLevelAtTransportLayer) {
        update(m -> m.toBuilder().setApplyLevelAtTransportLayer(applyLevelAtTransportLayer).build());
    }

    @Override
    public ActionRequestValidationException validate() {
        final Level level = level();
        if (level == Level.AWARENESS_ATTRIBUTES && message().getIndicesCount() > 0) {
            return addValidationError("awareness_attribute is not a supported parameter with index health", null);
        } else if (level != Level.AWARENESS_ATTRIBUTES && message().hasAwarenessAttribute()) {
            return addValidationError("level=awareness_attributes is required with awareness_attribute parameter", null);
        }
        if (ensureNodeWeighedIn() && local() == false) {
            return addValidationError("not a local request to ensure local node commissioned or weighed in", null);
        }
        return null;
    }

    private static ClusterHealthRequestProto.ClusterHealthRequest.Level toProto(Level level) {
        switch (level) {
            case INDICES:
                return ClusterHealthRequestProto.ClusterHealthRequest.Level.LEVEL_INDICES;
            case SHARDS:
                return ClusterHealthRequestProto.ClusterHealthRequest.Level.LEVEL_SHARDS;
            case AWARENESS_ATTRIBUTES:
                return ClusterHealthRequestProto.ClusterHealthRequest.Level.LEVEL_AWARENESS_ATTRIBUTES;
            default:
                return ClusterHealthRequestProto.ClusterHealthRequest.Level.LEVEL_CLUSTER;
        }
    }

    /**
     * The level of the health request.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Level {
        CLUSTER,
        INDICES,
        SHARDS,
        AWARENESS_ATTRIBUTES
    }
}
