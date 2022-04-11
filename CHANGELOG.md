# Changelog

## [Unreleased]

### Added

- Support for HBase 2.4.9 ([#133]).
- Support for HBase 2.4.11 ([#148]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#137]).

## Changed

- `operator-rs` `0.12.0` -> `0.15.0` ([#137], [#153]).
- Now using HDFS discovery config map instead of hdfs name node config map ([#153])
- BREAKING: Consolidated CRD - discovery config maps now top level, removed several `HbaseConfig` options ([#162]). 

[#133]: https://github.com/stackabletech/hbase-operator/pull/133
[#137]: https://github.com/stackabletech/hbase-operator/pull/137
[#148]: https://github.com/stackabletech/hbase-operator/pull/148
[#153]: https://github.com/stackabletech/hbase-operator/pull/153
[#162]: https://github.com/stackabletech/hbase-operator/pull/162

## [0.2.0] - 2022-02-14

### Added

- Reconciliation errors are now reported as Kubernetes events ([#127]).

### Changed

- `operator-rs` `0.10.0` -> `0.12.0` ([#127]).

[#127]: https://github.com/stackabletech/hbase-operator/pull/127

### Changed

- Migrated to StatefulSet rather than direct Pod management ([#110]).

[#110]: https://github.com/stackabletech/hbase-operator/pull/110

## [0.1.0] - 2021-10-28

### Changed

- `operator-rs`: `0.3.0` ([#18])

[#18]: https://github.com/stackabletech/hdfs-operator/pull/18
