# Changelog

## [Unreleased]

### Changed

- Updated stackable image versions ([#275]).
- `operator-rs` `0.24.0` -> `0.27.1` ([#277]).

[#275]: https://github.com/stackabletech/hbase-operator/pull/275
[#277]: https://github.com/stackabletech/hbase-operator/pull/277

## [0.5.0] - 2022-11-07

### Added

- Cpu and memory limits are now configurable ([#245]).
- Fix for Phoenix tests ([#261])

[#245]: https://github.com/stackabletech/hbase-operator/pull/245
[#261]: https://github.com/stackabletech/hbase-operator/pull/261

## [0.4.0] - 2022-09-06

### Changed

- Startup probe created and thresholds in liveness and readiness probes fine-tuned ([#193]).
- Include chart name when installing with a custom release name ([#209], [#210]).
- Orphaned resources are deleted ([#215]).
- Fix HBase-shell start failure ([#218]).
- Add integration tests and usage documentation for Phoenix ([#221]).
- Added OpenShift compatibility ([#232])

[#193]: https://github.com/stackabletech/hbase-operator/pull/193
[#209]: https://github.com/stackabletech/hbase-operator/pull/209
[#210]: https://github.com/stackabletech/hbase-operator/pull/210
[#215]: https://github.com/stackabletech/hbase-operator/pull/215
[#218]: https://github.com/stackabletech/hbase-operator/pull/218
[#221]: https://github.com/stackabletech/hbase-operator/pull/221
[#232]: https://github.com/stackabletech/hbase-operator/pull/232

## [0.3.0] - 2022-06-30

### Added

- Support for HBase 2.4.9 ([#133]).
- Support for HBase 2.4.11 ([#148]).
- Support for HBase 2.4.12 ([#197]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#137]).
- Writing a discovery config map containing `hbase-site.xml` with the `hbase.zookeeper.quorum` property ([#163]).

### Changed

- `operator-rs` `0.12.0` -> `0.15.0` ([#137], [#153]).
- Now using HDFS discovery config map instead of hdfs name node config map ([#153])
- BREAKING: Consolidated CRD - discovery config maps now top level, removed several `HbaseConfig` options (can still be overridden) ([#162]):
  - `hbaseManagesZk`: defaults to false
  - `hbaseClusterDistributed`: defaults to true
- [BREAKING] Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so `version: 3.5.8` becomes (for example) `version: 3.5.8-stackable0.1.0` ([#179])

[#133]: https://github.com/stackabletech/hbase-operator/pull/133
[#137]: https://github.com/stackabletech/hbase-operator/pull/137
[#148]: https://github.com/stackabletech/hbase-operator/pull/148
[#153]: https://github.com/stackabletech/hbase-operator/pull/153
[#162]: https://github.com/stackabletech/hbase-operator/pull/162
[#163]: https://github.com/stackabletech/hbase-operator/pull/163
[#179]: https://github.com/stackabletech/hbase-operator/pull/179
[#197]: https://github.com/stackabletech/hbase-operator/pull/197

## [0.2.0] - 2022-02-14

### Added

- Reconciliation errors are now reported as Kubernetes events ([#127]).

### Changed

- `operator-rs` `0.10.0` -> `0.12.0` ([#127]).
- Migrated to StatefulSet rather than direct Pod management ([#110]).

[#127]: https://github.com/stackabletech/hbase-operator/pull/127
[#110]: https://github.com/stackabletech/hbase-operator/pull/110

## [0.1.0] - 2021-10-28

### Changed

- `operator-rs`: `0.3.0` ([#18])

[#18]: https://github.com/stackabletech/hdfs-operator/pull/18
