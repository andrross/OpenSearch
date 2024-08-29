/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.node;

import org.opensearch.Version;

public class DiscoveryNodeRoleProtoConverter {
    static DiscoveryNodeRole fromProto(DiscoveryNodeProto.DiscoveryNode.Role role) {
        return DiscoveryNode.buildRole(role.getName(), role.getAbbreviation(), role.getCanContainData(), Version.CURRENT);
    }

    static DiscoveryNodeProto.DiscoveryNode.Role toProto(DiscoveryNodeRole role) {
        return DiscoveryNodeProto.DiscoveryNode.Role.newBuilder()
            .setName(role.roleName())
            .setAbbreviation(role.roleNameAbbreviation())
            .setCanContainData(role.canContainData())
            .build();
    }
}
