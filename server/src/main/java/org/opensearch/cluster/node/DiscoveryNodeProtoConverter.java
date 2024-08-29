/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.opensearch.Version;
import org.opensearch.core.common.transport.TransportAddress;

import com.google.protobuf.ByteString;

public class DiscoveryNodeProtoConverter {
    public static DiscoveryNode fromProto(DiscoveryNodeProto.DiscoveryNode proto) throws IOException {
        final Version version = Version.fromId(proto.getVersion());
        final Set<DiscoveryNodeRole> roles = new HashSet<>();
        for (DiscoveryNodeProto.DiscoveryNode.Role protoRole : proto.getRolesList()) {
            roles.add(DiscoveryNode.buildRole(protoRole.getName(), protoRole.getAbbreviation(), protoRole.getCanContainData(), version));
        }
        return new DiscoveryNode(
            proto.getNodeName(),
            proto.getNodeId(),
            proto.getEphemeralId(),
            proto.getHostName(),
            proto.getHostAddress(),
            new TransportAddress(new InetSocketAddress(
                InetAddress.getByAddress(proto.getAddress().getHost(), proto.getAddress().getAddr().toByteArray()), proto.getAddress().getPort())),
            proto.getAttributesMap(),
            roles,
            Version.fromId(proto.getVersion()));
    }

    public static DiscoveryNodeProto.DiscoveryNode toProto(DiscoveryNode node) {
        final DiscoveryNodeProto.DiscoveryNode.Builder builder = DiscoveryNodeProto.DiscoveryNode.newBuilder();
        builder.setNodeName(node.getName());
        builder.setNodeId(node.getId());
        builder.setEphemeralId(node.getEphemeralId());
        builder.setHostName(node.getHostName());
        builder.setHostAddress(node.getHostAddress());
        builder.getAddressBuilder()
            .setAddr(ByteString.copyFrom(node.getAddress().address().getAddress().getAddress()))
            .setHost(node.getAddress().address().getHostString())
            .setPort(node.getAddress().getPort());
        builder.putAllAttributes(node.getAttributes());
        for (DiscoveryNodeRole role : node.getRoles()) {
            builder.addRolesBuilder()
                .setName(role.roleName())
                .setAbbreviation(role.roleNameAbbreviation())
                .setCanContainData(role.canContainData());
        }
        builder.setVersion(node.getVersion().id);
        return builder.build();
    }
}
