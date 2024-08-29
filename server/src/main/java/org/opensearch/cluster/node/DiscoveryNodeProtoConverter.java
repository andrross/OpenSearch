/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.node;

public class DiscoveryNodeProtoConverter {
    public static DiscoveryNode fromProto(DiscoveryNodeProto.DiscoveryNode proto) {
        return new DiscoveryNode(proto);
    }

    public static DiscoveryNodeProto.DiscoveryNode toProto(DiscoveryNode node) {
        return node.proto;
    }
}
