/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.cluster.coordination;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.Version;
import org.opensearch.cluster.coordination.FollowersChecker;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.core.common.transport.TransportAddress;

@Warmup(iterations = 2)
@Measurement(iterations = 2)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Threads(1)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class FollowsCheckerSerializationBenchmark {
    static {
        LogConfigurator.setNodeName("benchmark");
    }

    private final BytesStreamOutput output = new BytesStreamOutput(1024);

    @Benchmark
    public void get(Parameters parameters, Blackhole blackhole) throws IOException {
        final FollowersChecker.FollowerCheckRequest request = new FollowersChecker.FollowerCheckRequest(1, createNode(parameters.version()));
        output.setVersion(parameters.version());
        request.writeTo(output);
        blackhole.consume(output.bytes());
        output.reset();
    }

    private static DiscoveryNode createNode(Version version) {
        return new DiscoveryNode(
            "nodeName",
            "nodeId",
            "ephemeralId",
            "hostName",
            "hostAddress",
            new TransportAddress(InetAddress.getLoopbackAddress(), 12345),
            Map.of("key", "value"),
            Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.INGEST_ROLE),
            version
        );
    }

    @State(Scope.Benchmark)
    public static class Parameters {
        @Param({ "3.0.0", "2.16.0" })
        String versionString;

        @Setup
        public void setup() {
        }

        Version version() {
            return Version.fromString(versionString);
        }
    }
}
