/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.health;

import org.opensearch.action.support.protoaction.ProtoConverters;
import org.opensearch.cluster.awarenesshealth.ClusterAwarenessAttributeValueHealth;
import org.opensearch.cluster.awarenesshealth.ClusterAwarenessAttributesHealth;
import org.opensearch.cluster.awarenesshealth.ClusterAwarenessHealth;
import org.opensearch.cluster.awarenesshealth.proto.ClusterAwarenessHealthProto;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.health.ClusterShardHealth;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.health.proto.ClusterIndexHealthProto;
import org.opensearch.cluster.health.proto.ClusterShardHealthProto;
import org.opensearch.cluster.health.proto.ClusterStateHealthProto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Conversions from the health domain types (computed from {@link org.opensearch.cluster.ClusterState}) to their
 * generated protobuf forms and back. The domain classes still compute health from cluster state and still own the
 * frozen legacy stream layout, so both directions are needed for the compatibility window.
 *
 * @opensearch.internal
 */
final class ClusterHealthProtoConverters {

    private ClusterHealthProtoConverters() {}

    // ----- domain -> proto -----

    static ClusterShardHealthProto.ClusterShardHealth toProto(ClusterShardHealth shard) {
        return ClusterShardHealthProto.ClusterShardHealth.newBuilder()
            .setShardId(shard.getShardId())
            .setStatus(ProtoConverters.toProto(shard.getStatus()))
            .setActiveShards(shard.getActiveShards())
            .setRelocatingShards(shard.getRelocatingShards())
            .setInitializingShards(shard.getInitializingShards())
            .setUnassignedShards(shard.getUnassignedShards())
            .setDelayedUnassignedShards(shard.getDelayedUnassignedShards())
            .setPrimaryActive(shard.isPrimaryActive())
            .build();
    }

    static ClusterIndexHealthProto.ClusterIndexHealth toProto(ClusterIndexHealth index) {
        final ClusterIndexHealthProto.ClusterIndexHealth.Builder builder = ClusterIndexHealthProto.ClusterIndexHealth.newBuilder()
            .setIndex(index.getIndex())
            .setNumberOfShards(index.getNumberOfShards())
            .setNumberOfReplicas(index.getNumberOfReplicas())
            .setActivePrimaryShards(index.getActivePrimaryShards())
            .setActiveShards(index.getActiveShards())
            .setRelocatingShards(index.getRelocatingShards())
            .setInitializingShards(index.getInitializingShards())
            .setUnassignedShards(index.getUnassignedShards())
            .setDelayedUnassignedShards(index.getDelayedUnassignedShards())
            .setStatus(ProtoConverters.toProto(index.getStatus()));
        for (Map.Entry<Integer, ClusterShardHealth> e : index.getShards().entrySet()) {
            builder.putShards(e.getKey(), toProto(e.getValue()));
        }
        return builder.build();
    }

    static ClusterStateHealthProto.ClusterStateHealth toProto(ClusterStateHealth health) {
        final ClusterStateHealthProto.ClusterStateHealth.Builder builder = ClusterStateHealthProto.ClusterStateHealth.newBuilder()
            .setNumberOfNodes(health.getNumberOfNodes())
            .setNumberOfDataNodes(health.getNumberOfDataNodes())
            .setHasDiscoveredClusterManager(health.hasDiscoveredClusterManager())
            .setActiveShards(health.getActiveShards())
            .setRelocatingShards(health.getRelocatingShards())
            .setActivePrimaryShards(health.getActivePrimaryShards())
            .setInitializingShards(health.getInitializingShards())
            .setUnassignedShards(health.getUnassignedShards())
            .setDelayedUnassignedShards(health.getDelayedUnassignedShards())
            .setActiveShardsPercent(health.getActiveShardsPercent())
            .setStatus(ProtoConverters.toProto(health.getStatus()));
        for (Map.Entry<String, ClusterIndexHealth> e : health.getIndices().entrySet()) {
            builder.putIndices(e.getKey(), toProto(e.getValue()));
        }
        return builder.build();
    }

    static ClusterAwarenessHealthProto.ClusterAwarenessAttributeValueHealth toProto(ClusterAwarenessAttributeValueHealth value) {
        final ClusterAwarenessHealthProto.ClusterAwarenessAttributeValueHealth.Builder builder =
            ClusterAwarenessHealthProto.ClusterAwarenessAttributeValueHealth.newBuilder()
                .setName(value.getName())
                .setActiveShards(value.getActiveShards())
                .setUnassignedShards(value.getUnassignedShards())
                .setInitializingShards(value.getInitializingShards())
                .setRelocatingShards(value.getRelocatingShards())
                .setNodes(value.getNodes())
                .setWeight(value.getWeight());
        if (value.getNodeList() != null) {
            builder.addAllNodeList(value.getNodeList());
        }
        return builder.build();
    }

    static ClusterAwarenessHealthProto.ClusterAwarenessAttributesHealth toProto(ClusterAwarenessAttributesHealth attribute) {
        final ClusterAwarenessHealthProto.ClusterAwarenessAttributesHealth.Builder builder =
            ClusterAwarenessHealthProto.ClusterAwarenessAttributesHealth.newBuilder()
                .setAwarenessAttributeName(attribute.getAwarenessAttributeName());
        for (Map.Entry<String, ClusterAwarenessAttributeValueHealth> e : attribute.getAwarenessAttributeHealthMap().entrySet()) {
            builder.putValues(e.getKey(), toProto(e.getValue()));
        }
        return builder.build();
    }

    static ClusterAwarenessHealthProto.ClusterAwarenessHealth toProto(ClusterAwarenessHealth health) {
        final ClusterAwarenessHealthProto.ClusterAwarenessHealth.Builder builder = ClusterAwarenessHealthProto.ClusterAwarenessHealth
            .newBuilder();
        for (Map.Entry<String, ClusterAwarenessAttributesHealth> e : health.getClusterAwarenessAttributesHealthMap().entrySet()) {
            builder.putAttributes(e.getKey(), toProto(e.getValue()));
        }
        return builder.build();
    }

    // ----- proto -> domain -----

    static ClusterStateHealth fromProto(ClusterStateHealthProto.ClusterStateHealth health) {
        final Map<String, ClusterIndexHealth> indices = new HashMap<>(health.getIndicesCount());
        for (Map.Entry<String, ClusterIndexHealthProto.ClusterIndexHealth> e : health.getIndicesMap().entrySet()) {
            indices.put(e.getKey(), fromProto(e.getValue()));
        }
        return new ClusterStateHealth(
            health.getActivePrimaryShards(),
            health.getActiveShards(),
            health.getRelocatingShards(),
            health.getInitializingShards(),
            health.getUnassignedShards(),
            health.getNumberOfNodes(),
            health.getNumberOfDataNodes(),
            health.getHasDiscoveredClusterManager(),
            health.getActiveShardsPercent(),
            ProtoConverters.fromProto(health.getStatus()),
            indices
        );
    }

    static ClusterIndexHealth fromProto(ClusterIndexHealthProto.ClusterIndexHealth index) {
        final Map<Integer, ClusterShardHealth> shards = new HashMap<>(index.getShardsCount());
        for (Map.Entry<Integer, ClusterShardHealthProto.ClusterShardHealth> e : index.getShardsMap().entrySet()) {
            shards.put(e.getKey(), fromProto(e.getValue()));
        }
        return new ClusterIndexHealth(
            index.getIndex(),
            index.getNumberOfShards(),
            index.getNumberOfReplicas(),
            index.getActiveShards(),
            index.getRelocatingShards(),
            index.getInitializingShards(),
            index.getUnassignedShards(),
            index.getActivePrimaryShards(),
            ProtoConverters.fromProto(index.getStatus()),
            shards
        );
    }

    static ClusterShardHealth fromProto(ClusterShardHealthProto.ClusterShardHealth shard) {
        return new ClusterShardHealth(
            shard.getShardId(),
            ProtoConverters.fromProto(shard.getStatus()),
            shard.getActiveShards(),
            shard.getRelocatingShards(),
            shard.getInitializingShards(),
            shard.getUnassignedShards(),
            shard.getPrimaryActive()
        );
    }

    static ClusterAwarenessHealth fromProto(ClusterAwarenessHealthProto.ClusterAwarenessHealth health) {
        final Map<String, ClusterAwarenessAttributesHealth> attributes = new HashMap<>(health.getAttributesCount());
        for (Map.Entry<String, ClusterAwarenessHealthProto.ClusterAwarenessAttributesHealth> e : health.getAttributesMap().entrySet()) {
            final Map<String, ClusterAwarenessAttributeValueHealth> values = new HashMap<>(e.getValue().getValuesCount());
            for (Map.Entry<String, ClusterAwarenessHealthProto.ClusterAwarenessAttributeValueHealth> v : e.getValue()
                .getValuesMap()
                .entrySet()) {
                final ClusterAwarenessHealthProto.ClusterAwarenessAttributeValueHealth p = v.getValue();
                final ClusterAwarenessAttributeValueHealth value = new ClusterAwarenessAttributeValueHealth(
                    p.getName(),
                    p.getActiveShards(),
                    p.getInitializingShards(),
                    p.getRelocatingShards(),
                    p.getUnassignedShards(),
                    p.getNodes(),
                    p.getWeight()
                );
                value.setNodeList(new ArrayList<>(p.getNodeListList()));
                values.put(v.getKey(), value);
            }
            attributes.put(e.getKey(), new ClusterAwarenessAttributesHealth(e.getValue().getAwarenessAttributeName(), values));
        }
        return new ClusterAwarenessHealth(attributes);
    }
}
