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
import org.opensearch.action.admin.cluster.health.proto.ClusterHealthResponseProto;
import org.opensearch.action.support.protoaction.ProtoConverters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.awarenesshealth.ClusterAwarenessHealth;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.health.proto.ClusterStateHealthProto;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.transport.ProtobufPayload;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Transport response for Cluster Health.
 * <p>
 * State is held in a {@link ClusterHealthResponseProto.ClusterHealthResponse} message. Between nodes at or after
 * {@link #PROTO_SINCE} the message is the wire payload (see {@link ProtobufPayload}); older peers receive the legacy
 * stream layout, which is frozen in {@link #writeLegacy}/{@link #readLegacy}. The public API of this class is unchanged.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterHealthResponse extends ActionResponse implements StatusToXContentObject, ProtobufPayload {

    /** First version that encodes this response as protobuf on the wire. */
    public static final Version PROTO_SINCE = Version.V_3_10_0;

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String STATUS = "status";
    private static final String TIMED_OUT = "timed_out";
    private static final String NUMBER_OF_NODES = "number_of_nodes";
    private static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    @Deprecated
    private static final String DISCOVERED_MASTER = "discovered_master";
    private static final String DISCOVERED_CLUSTER_MANAGER = "discovered_cluster_manager";
    private static final String NUMBER_OF_PENDING_TASKS = "number_of_pending_tasks";
    private static final String NUMBER_OF_IN_FLIGHT_FETCH = "number_of_in_flight_fetch";
    private static final String DELAYED_UNASSIGNED_SHARDS = "delayed_unassigned_shards";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE = "task_max_waiting_in_queue";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS = "task_max_waiting_in_queue_millis";
    private static final String ACTIVE_SHARDS_PERCENT_AS_NUMBER = "active_shards_percent_as_number";
    private static final String ACTIVE_SHARDS_PERCENT = "active_shards_percent";
    private static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String INDICES = "indices";

    private static final ConstructingObjectParser<ClusterHealthResponse, Void> PARSER = new ConstructingObjectParser<>(
        "cluster_health_response",
        true,
        parsedObjects -> {
            int i = 0;
            // ClusterStateHealth fields
            int numberOfNodes = (int) parsedObjects[i++];
            int numberOfDataNodes = (int) parsedObjects[i++];
            boolean hasDiscoveredMaster = Boolean.TRUE.equals(parsedObjects[i++]);
            boolean hasDiscoveredClusterManager = Boolean.TRUE.equals(parsedObjects[i++]);
            int activeShards = (int) parsedObjects[i++];
            int relocatingShards = (int) parsedObjects[i++];
            int activePrimaryShards = (int) parsedObjects[i++];
            int initializingShards = (int) parsedObjects[i++];
            int unassignedShards = (int) parsedObjects[i++];
            double activeShardsPercent = (double) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i++];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            @SuppressWarnings("unchecked")
            List<ClusterIndexHealth> indexList = (List<ClusterIndexHealth>) parsedObjects[i++];
            final Map<String, ClusterIndexHealth> indices;
            if (indexList == null || indexList.isEmpty()) {
                indices = emptyMap();
            } else {
                indices = new HashMap<>(indexList.size());
                for (ClusterIndexHealth indexHealth : indexList) {
                    indices.put(indexHealth.getIndex(), indexHealth);
                }
            }
            ClusterStateHealth stateHealth = new ClusterStateHealth(
                activePrimaryShards,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                numberOfNodes,
                numberOfDataNodes,
                hasDiscoveredClusterManager || hasDiscoveredMaster,
                activeShardsPercent,
                status,
                indices
            );
            // ClusterHealthResponse fields
            String clusterName = (String) parsedObjects[i++];
            int numberOfPendingTasks = (int) parsedObjects[i++];
            int numberOfInFlightFetch = (int) parsedObjects[i++];
            int delayedUnassignedShards = (int) parsedObjects[i++];
            long taskMaxWaitingTimeMillis = (long) parsedObjects[i++];
            boolean timedOut = (boolean) parsedObjects[i];
            return new ClusterHealthResponse(
                clusterName,
                numberOfPendingTasks,
                numberOfInFlightFetch,
                delayedUnassignedShards,
                TimeValue.timeValueMillis(taskMaxWaitingTimeMillis),
                timedOut,
                stateHealth
            );
        }
    );

    private static final ObjectParser.NamedObjectParser<ClusterIndexHealth, Void> INDEX_PARSER = (
        XContentParser parser,
        Void context,
        String index) -> ClusterIndexHealth.innerFromXContent(parser, index);

    static {
        // ClusterStateHealth fields
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_NODES));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_DATA_NODES));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(DISCOVERED_MASTER));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(DISCOVERED_CLUSTER_MANAGER));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_PRIMARY_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareDouble(constructorArg(), new ParseField(ACTIVE_SHARDS_PERCENT_AS_NUMBER));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
        // Can be absent if LEVEL == 'cluster'
        PARSER.declareNamedObjects(optionalConstructorArg(), INDEX_PARSER, new ParseField(INDICES));

        // ClusterHealthResponse fields
        PARSER.declareString(constructorArg(), new ParseField(CLUSTER_NAME));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_PENDING_TASKS));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_IN_FLIGHT_FETCH));
        PARSER.declareInt(constructorArg(), new ParseField(DELAYED_UNASSIGNED_SHARDS));
        PARSER.declareLong(constructorArg(), new ParseField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS));
        PARSER.declareBoolean(constructorArg(), new ParseField(TIMED_OUT));
    }

    private ClusterHealthResponseProto.ClusterHealthResponse message;
    // Domain views of the message, converted on first use.
    private ClusterStateHealth clusterStateHealth;
    private ClusterAwarenessHealth clusterAwarenessHealth;

    public ClusterHealthResponse() {
        this(ClusterHealthResponseProto.ClusterHealthResponse.getDefaultInstance());
    }

    /** Wrap an existing message. Package-private: callers outside this package use the public constructors. */
    ClusterHealthResponse(ClusterHealthResponseProto.ClusterHealthResponse message) {
        this.message = Objects.requireNonNull(message, "message");
    }

    public ClusterHealthResponse(StreamInput in) throws IOException {
        super(in);
        this.message = ProtobufPayload.readPayload(
            in,
            PROTO_SINCE,
            ClusterHealthResponseProto.ClusterHealthResponse::parseFrom,
            ClusterHealthResponse::readLegacy
        );
    }

    /** needed for plugins BWC */
    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState) {
        this(clusterName, concreteIndices, clusterState, -1, -1, -1, TimeValue.timeValueHours(0));
    }

    public ClusterHealthResponse(
        String clusterName,
        String[] concreteIndices,
        ClusterState clusterState,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime
    ) {
        this(
            clusterName,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            delayedUnassignedShards,
            taskMaxWaitingTime,
            false,
            new ClusterStateHealth(clusterState, concreteIndices)
        );
    }

    public ClusterHealthResponse(
        String clusterName,
        String[] concreteIndices,
        ClusterHealthRequest clusterHealthRequest,
        ClusterState clusterState,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        TimeValue taskMaxWaitingTime
    ) {
        this(
            clusterName,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            0,
            taskMaxWaitingTime,
            false,
            clusterHealthRequest.isApplyLevelAtTransportLayer()
                ? new ClusterStateHealth(clusterState, concreteIndices, clusterHealthRequest.level())
                : new ClusterStateHealth(clusterState, concreteIndices)
        );
        replace(message().toBuilder().setDelayedUnassignedShards(clusterStateHealth.getDelayedUnassignedShards()).build());
    }

    // Awareness Attribute health
    public ClusterHealthResponse(
        String clusterName,
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        String[] concreteIndices,
        String awarenessAttributeName,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime
    ) {
        this(
            clusterName,
            concreteIndices,
            clusterState,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            delayedUnassignedShards,
            taskMaxWaitingTime
        );
        setClusterAwarenessHealth(new ClusterAwarenessHealth(clusterState, clusterSettings, awarenessAttributeName));
    }

    public ClusterHealthResponse(
        String clusterName,
        ClusterHealthRequest clusterHealthRequest,
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        String[] concreteIndices,
        String awarenessAttributeName,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        TimeValue taskMaxWaitingTime
    ) {
        this(
            clusterName,
            concreteIndices,
            clusterHealthRequest,
            clusterState,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            taskMaxWaitingTime
        );
        setClusterAwarenessHealth(new ClusterAwarenessHealth(clusterState, clusterSettings, awarenessAttributeName));
    }

    /**
     * For XContent Parser and serialization tests
     */
    ClusterHealthResponse(
        String clusterName,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime,
        boolean timedOut,
        ClusterStateHealth clusterStateHealth
    ) {
        this(
            ClusterHealthResponseProto.ClusterHealthResponse.newBuilder()
                .setClusterName(clusterName)
                .setStatus(ProtoConverters.toProto(clusterStateHealth.getStatus()))
                .setClusterStateHealth(ClusterHealthProtoConverters.toProto(clusterStateHealth))
                .setNumberOfPendingTasks(numberOfPendingTasks)
                .setTimedOut(timedOut)
                .setNumberOfInFlightFetch(numberOfInFlightFetch)
                .setDelayedUnassignedShards(delayedUnassignedShards)
                .setTaskMaxWaitingTime(ProtoConverters.toProto(taskMaxWaitingTime))
                .build()
        );
        this.clusterStateHealth = clusterStateHealth;
    }

    // payload is written by TransportResponse#writeTo; kept here for binary compatibility
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
     * The current message. Package-private: code outside this class reads the response through its accessors, which
     * convert to domain types.
     */
    ClusterHealthResponseProto.ClusterHealthResponse message() {
        return message;
    }

    private void replace(ClusterHealthResponseProto.ClusterHealthResponse message) {
        this.message = Objects.requireNonNull(message, "message");
    }

    // ----- frozen legacy layout -----

    private static ClusterHealthResponseProto.ClusterHealthResponse readLegacy(StreamInput in) throws IOException {
        final ClusterHealthResponseProto.ClusterHealthResponse.Builder b = ClusterHealthResponseProto.ClusterHealthResponse.newBuilder();
        b.setClusterName(in.readString());
        b.setStatus(ProtoConverters.toProto(ClusterHealthStatus.fromValue(in.readByte())));
        b.setClusterStateHealth(ClusterHealthProtoConverters.toProto(new ClusterStateHealth(in)));
        b.setNumberOfPendingTasks(in.readInt());
        b.setTimedOut(in.readBoolean());
        b.setNumberOfInFlightFetch(in.readInt());
        b.setDelayedUnassignedShards(in.readInt());
        b.setTaskMaxWaitingTime(ProtoConverters.toProto(in.readTimeValue()));
        if (in.getVersion().onOrAfter(Version.V_2_5_0)) {
            if (in.readBoolean()) {
                b.setClusterAwarenessHealth(ClusterHealthProtoConverters.toProto(new ClusterAwarenessHealth(in)));
            }
        }
        return b.build();
    }

    @Override
    protected void writeLegacy(StreamOutput out) throws IOException {
        final ClusterHealthResponseProto.ClusterHealthResponse m = message;
        out.writeString(m.getClusterName());
        out.writeByte(ProtoConverters.fromProto(m.getStatus()).value());
        getClusterStateHealth().writeTo(out);
        out.writeInt(m.getNumberOfPendingTasks());
        out.writeBoolean(m.getTimedOut());
        out.writeInt(m.getNumberOfInFlightFetch());
        out.writeInt(m.getDelayedUnassignedShards());
        out.writeTimeValue(ProtoConverters.fromProto(m.getTaskMaxWaitingTime()));
        if (out.getVersion().onOrAfter(Version.V_2_5_0)) {
            if (m.hasClusterAwarenessHealth()) {
                out.writeBoolean(true);
                getClusterAwarenessHealth().writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    // ----- public API (unchanged) -----

    public String getClusterName() {
        return message().getClusterName();
    }

    // package private for testing
    ClusterStateHealth getClusterStateHealth() {
        if (clusterStateHealth == null) {
            clusterStateHealth = ClusterHealthProtoConverters.fromProto(message().getClusterStateHealth());
        }
        return clusterStateHealth;
    }

    private ClusterStateHealthProto.ClusterStateHealth state() {
        return message().getClusterStateHealth();
    }

    public int getActiveShards() {
        return state().getActiveShards();
    }

    public int getRelocatingShards() {
        return state().getRelocatingShards();
    }

    public int getActivePrimaryShards() {
        return state().getActivePrimaryShards();
    }

    public int getInitializingShards() {
        return state().getInitializingShards();
    }

    public int getUnassignedShards() {
        return state().getUnassignedShards();
    }

    public int getNumberOfNodes() {
        return state().getNumberOfNodes();
    }

    public int getNumberOfDataNodes() {
        return state().getNumberOfDataNodes();
    }

    public boolean hasDiscoveredClusterManager() {
        return state().getHasDiscoveredClusterManager();
    }

    public int getNumberOfPendingTasks() {
        return message().getNumberOfPendingTasks();
    }

    public int getNumberOfInFlightFetch() {
        return message().getNumberOfInFlightFetch();
    }

    /**
     * The number of unassigned shards that are currently being delayed (for example,
     * due to node leaving the cluster and waiting for a timeout for the node to come
     * back in order to allocate the shards back to it).
     */
    public int getDelayedUnassignedShards() {
        return message().getDelayedUnassignedShards();
    }

    /**
     * {@code true} if the waitForXXX has timeout out and did not match.
     */
    public boolean isTimedOut() {
        return message().getTimedOut();
    }

    public void setTimedOut(boolean timedOut) {
        replace(message().toBuilder().setTimedOut(timedOut).build());
    }

    public ClusterHealthStatus getStatus() {
        return ProtoConverters.fromProto(message().getStatus());
    }

    /**
     * Allows to explicitly override the derived cluster health status.
     *
     * @param status The override status. Must not be null.
     */
    public void setStatus(ClusterHealthStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("'status' must not be null");
        }
        replace(message().toBuilder().setStatus(ProtoConverters.toProto(status)).build());
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return getClusterStateHealth().getIndices();
    }

    /**
     *
     * @return The maximum wait time of all tasks in the queue
     */
    public TimeValue getTaskMaxWaitingTime() {
        return ProtoConverters.fromProto(message().getTaskMaxWaitingTime());
    }

    /**
     * The percentage of active shards, should be 100% in a green system
     */
    public double getActiveShardsPercent() {
        return state().getActiveShardsPercent();
    }

    public ClusterAwarenessHealth getClusterAwarenessHealth() {
        if (clusterAwarenessHealth == null && message().hasClusterAwarenessHealth()) {
            clusterAwarenessHealth = ClusterHealthProtoConverters.fromProto(message().getClusterAwarenessHealth());
        }
        return clusterAwarenessHealth;
    }

    private void setClusterAwarenessHealth(ClusterAwarenessHealth health) {
        this.clusterAwarenessHealth = health;
        replace(message().toBuilder().setClusterAwarenessHealth(ClusterHealthProtoConverters.toProto(health)).build());
    }

    public static ClusterHealthResponse readResponseFrom(StreamInput in) throws IOException {
        return new ClusterHealthResponse(in);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public RestStatus status() {
        return isTimedOut() ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLUSTER_NAME, getClusterName());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(TIMED_OUT, isTimedOut());
        builder.field(NUMBER_OF_NODES, getNumberOfNodes());
        builder.field(NUMBER_OF_DATA_NODES, getNumberOfDataNodes());
        builder.field(DISCOVERED_MASTER, hasDiscoveredClusterManager());  // the field will be removed in a future major version
        builder.field(DISCOVERED_CLUSTER_MANAGER, hasDiscoveredClusterManager());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(DELAYED_UNASSIGNED_SHARDS, getDelayedUnassignedShards());
        builder.field(NUMBER_OF_PENDING_TASKS, getNumberOfPendingTasks());
        builder.field(NUMBER_OF_IN_FLIGHT_FETCH, getNumberOfInFlightFetch());
        builder.humanReadableField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS, TASK_MAX_WAIT_TIME_IN_QUEUE, getTaskMaxWaitingTime());
        builder.percentageField(ACTIVE_SHARDS_PERCENT_AS_NUMBER, ACTIVE_SHARDS_PERCENT, getActiveShardsPercent());

        String level = params.param("level", "cluster");
        boolean outputIndices = "indices".equals(level) || "shards".equals(level);
        boolean outputAwarenessHealth = "awareness_attributes".equals(level);

        if (outputIndices) {
            builder.startObject(INDICES);
            for (ClusterIndexHealth indexHealth : getIndices().values()) {
                indexHealth.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (outputAwarenessHealth && getClusterAwarenessHealth() != null) {
            getClusterAwarenessHealth().toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    public static ClusterHealthResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        return o != null && getClass() == o.getClass() && message.equals(((ClusterHealthResponse) o).message);
    }

    @Override
    public int hashCode() {
        return message.hashCode();
    }
}
