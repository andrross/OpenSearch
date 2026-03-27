## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Features

* Introduce `AdditionalCodecs` and `EnginePlugin::getAdditionalCodecs` hook to allow additional Codec registration ([#20411](https://github.com/opensearch-project/OpenSearch/pull/20411))
* Add `hll` field type for storing pre-aggregated HyperLogLog++ cardinality data for rollup operations ([#20129](https://github.com/opensearch-project/OpenSearch/pull/20129))
* Add support for fields containing dots in their name as literals via `disable_objects` mapping parameter ([#19958](https://github.com/opensearch-project/OpenSearch/pull/19958))
* Add HTTP/3 server-side support ([#20017](https://github.com/opensearch-project/OpenSearch/pull/20017))

### Enhancements

* Add `X-Request-Id` header to uniquely identify search requests and include it in search slow logs ([#19798](https://github.com/opensearch-project/OpenSearch/pull/19798))
* Add `Alt-Svc` header support to advertise HTTP/3 availability ([#20434](https://github.com/opensearch-project/OpenSearch/pull/20434))
* Add circuit breaker support for gRPC transport ([#20203](https://github.com/opensearch-project/OpenSearch/pull/20203))
* Add index-level encryption support for snapshots and remote store ([#20095](https://github.com/opensearch-project/OpenSearch/pull/20095))
* Add public getter functions for source field mapper includes and excludes ([#20290](https://github.com/opensearch-project/OpenSearch/pull/20290)), ([#20205](https://github.com/opensearch-project/OpenSearch/pull/20205))
* Add `BigInteger` support for `unsigned_long` fields in gRPC transport ([#20346](https://github.com/opensearch-project/OpenSearch/pull/20346))
* Enable intra-segment search with configurable partitioning strategies ([#19704](https://github.com/opensearch-project/OpenSearch/pull/19704))
* Introduce concurrent translog recovery to accelerate segment replication primary promotion ([#20251](https://github.com/opensearch-project/OpenSearch/pull/20251))
* Make crypto store settings immutable after index creation ([#20123](https://github.com/opensearch-project/OpenSearch/pull/20123))
* Use compact object headers with JDK 25+ ([#20392](https://github.com/opensearch-project/OpenSearch/pull/20392))
* Add `cluster.initial_cluster_manager_nodes` to testClusters overridable settings for multi-node test clusters with search-only nodes ([#20348](https://github.com/opensearch-project/OpenSearch/pull/20348))
* Refactor streaming aggregation query phase planning with support for nested terms, cardinality, max, min, and sum sub-aggregations ([#20471](https://github.com/opensearch-project/OpenSearch/pull/20471))
* Add TopN selection for streaming terms aggregations to reduce data transfer and coordinator memory usage ([#20481](https://github.com/opensearch-project/OpenSearch/pull/20481))
* Stream transport refactor with async `FlightClientChannel`, virtual threads, and more efficient serialization ([#20359](https://github.com/opensearch-project/OpenSearch/pull/20359))
* Set TLS SNI in transport-netty4 module when `server_name` is configured for remote clusters ([#20321](https://github.com/opensearch-project/OpenSearch/pull/20321))
* Install demo security configuration automatically when running with the security plugin via Gradle ([#20372](https://github.com/opensearch-project/OpenSearch/pull/20372))
* Ensure all modules are included in INTEG_TEST testcluster distribution ([#20241](https://github.com/opensearch-project/OpenSearch/pull/20241))
* Remove `endpointOverride` in repository-s3 plugin and let AWS SDK V2 determine the S3 URL based on bucket name or ARN ([#20345](https://github.com/opensearch-project/OpenSearch/pull/20345))
* Introduce `libs/netty4` module to share common implementation between netty-based transport plugins ([#20447](https://github.com/opensearch-project/OpenSearch/pull/20447))

### Bug Fixes

* Fix tracing support for `StreamingRestChannel` to ensure proper context handling during streaming operations ([#20361](https://github.com/opensearch-project/OpenSearch/pull/20361))
* Allow removing a plugin that is optionally extended by another installed plugin ([#20417](https://github.com/opensearch-project/OpenSearch/pull/20417))
* Fix `SearchPhaseExecutionException` to properly initialize root cause for improved error reporting ([#20320](https://github.com/opensearch-project/OpenSearch/pull/20320))
* Fix `X-Opaque-Id` header propagation and other response headers for streaming Reactor Netty 4 transport ([#20371](https://github.com/opensearch-project/OpenSearch/pull/20371))
* Fix node local term/version log truncation with long host provider addresses during cluster formation ([#20432](https://github.com/opensearch-project/OpenSearch/pull/20432))
* Fix segment replication failure during rolling restart when replica has newer checkpoint than restarted primary ([#20422](https://github.com/opensearch-project/OpenSearch/pull/20422))
* Fix stats aggregation returning zero results with `size:0` ([#20427](https://github.com/opensearch-project/OpenSearch/pull/20427))
* Fix indexing regression and bug fixes for grouping criteria in composite index writer ([#20145](https://github.com/opensearch-project/OpenSearch/pull/20145))
* Preserve `SubReaderWrappers` on `LeafReader` when `IndexWriter` encounters a non-aborting exception ([#20193](https://github.com/opensearch-project/OpenSearch/pull/20193))
* Relax jar hell check when extended plugins share transitive dependencies ([#20103](https://github.com/opensearch-project/OpenSearch/pull/20103))
* Fix snapshot restore crash caused by unbounded string expansion in rename replacement field ([#20465](https://github.com/opensearch-project/OpenSearch/pull/20465))
* Fix restore with index sort ([#20284](https://github.com/opensearch-project/OpenSearch/pull/20284))
* Remove child-level directory on refresh for `CompositeIndexWriter` to prevent orphaned directories ([#20326](https://github.com/opensearch-project/OpenSearch/pull/20326))

### Maintenance

* Bump Apache HttpClient5 to 5.6 and Apache HttpCore5 to 5.4 ([#20358](https://github.com/opensearch-project/OpenSearch/pull/20358))
* Bump OpenTelemetry to 1.58.0 ([#20441](https://github.com/opensearch-project/OpenSearch/pull/20441))
* Bump netty to 4.2.9.Final ([#20230](https://github.com/opensearch-project/OpenSearch/pull/20230))
* Bump reactor-netty to 1.3.2 and reactor to 3.8.2 ([#20419](https://github.com/opensearch-project/OpenSearch/pull/20419))
* Update Jackson to 2.20.1 ([#20343](https://github.com/opensearch-project/OpenSearch/pull/20343))
* Bump log4j from 2.21.0 to 2.25.3 ([#20308](https://github.com/opensearch-project/OpenSearch/pull/20308))
* Bump asm from 9.7 to 9.9.1 ([#20330](https://github.com/opensearch-project/OpenSearch/pull/20330))
* Bump opensearch-protobufs from 1.1.0 to 1.2.0 ([#20480](https://github.com/opensearch-project/OpenSearch/pull/20480))
* Bump `com.google.api.grpc:proto-google-iam-v1` from 1.57.0 to 1.58.2 ([#20302](https://github.com/opensearch-project/OpenSearch/pull/20302))
* Bump `org.checkerframework:checker-qual` from 3.49.0 to 3.52.1 ([#20234](https://github.com/opensearch-project/OpenSearch/pull/20234))
* Bump `org.jsoup:jsoup` from 1.21.2 to 1.22.1 ([#20368](https://github.com/opensearch-project/OpenSearch/pull/20368))
* Bump `com.netflix.nebula:nebula-publishing-plugin` from 21.1.0 to 23.0.0 ([#20477](https://github.com/opensearch-project/OpenSearch/pull/20477))
* Bump `com.netflix.nebula.ospackage-base` from 12.1.1 to 12.2.0 ([#20439](https://github.com/opensearch-project/OpenSearch/pull/20439))
* Bump `org.apache.maven:maven-model` from 3.9.6 to 3.9.12 ([#20438](https://github.com/opensearch-project/OpenSearch/pull/20438))
* Remove identity-shiro plugin from plugins folder ([#20305](https://github.com/opensearch-project/OpenSearch/pull/20305))
* Upgrade gRPC protobufs to 1.0.0 and 1.1.0 ([#20335](https://github.com/opensearch-project/OpenSearch/pull/20335)), ([#20396](https://github.com/opensearch-project/OpenSearch/pull/20396))
