# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Constant Keyword Field ([#12285](https://github.com/opensearch-project/OpenSearch/pull/12285))
- Convert ingest processor supports ip type ([#12818](https://github.com/opensearch-project/OpenSearch/pull/12818))
- Add a counter to node stat api to track shard going from idle to non-idle ([#12768](https://github.com/opensearch-project/OpenSearch/pull/12768))
- Allow setting KEYSTORE_PASSWORD through env variable ([#12865](https://github.com/opensearch-project/OpenSearch/pull/12865))
- [Concurrent Segment Search] Perform buildAggregation concurrently and support Composite Aggregations ([#12697](https://github.com/opensearch-project/OpenSearch/pull/12697))
- [Concurrent Segment Search] Disable concurrent segment search for system indices and throttled requests ([#12954](https://github.com/opensearch-project/OpenSearch/pull/12954))
- Derived fields support to derive field values at query time without indexing ([#12569](https://github.com/opensearch-project/OpenSearch/pull/12569))
- Detect breaking changes on pull requests ([#9044](https://github.com/opensearch-project/OpenSearch/pull/9044))
- Add cluster primary balance contraint for rebalancing with buffer ([#12656](https://github.com/opensearch-project/OpenSearch/pull/12656))

### Dependencies
- Bump `org.apache.commons:commons-configuration2` from 2.10.0 to 2.10.1 ([#12896](https://github.com/opensearch-project/OpenSearch/pull/12896))
- Bump `asm` from 9.6 to 9.7 ([#12908](https://github.com/opensearch-project/OpenSearch/pull/12908))
- Bump `net.minidev:json-smart` from 2.5.0 to 2.5.1 ([#12893](https://github.com/opensearch-project/OpenSearch/pull/12893))
- Bump `netty` from 4.1.107.Final to 4.1.108.Final ([#12924](https://github.com/opensearch-project/OpenSearch/pull/12924))
- Bump `commons-io:commons-io` from 2.15.1 to 2.16.0 ([#12996](https://github.com/opensearch-project/OpenSearch/pull/12996), [#12998](https://github.com/opensearch-project/OpenSearch/pull/12998), [#12999](https://github.com/opensearch-project/OpenSearch/pull/12999))

### Changed
- [BWC and API enforcement] Enforcing the presence of API annotations at build time ([#12872](https://github.com/opensearch-project/OpenSearch/pull/12872))
- Improve built-in secure transports support ([#12907](https://github.com/opensearch-project/OpenSearch/pull/12907))

### Deprecated

### Removed

### Fixed
- Fix issue with feature flags where default value may not be honored ([#12849](https://github.com/opensearch-project/OpenSearch/pull/12849))
- Fix UOE While building Exists query for nested search_as_you_type field ([#12048](https://github.com/opensearch-project/OpenSearch/pull/12048))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.13...2.x
