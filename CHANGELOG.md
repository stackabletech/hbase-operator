# Changelog

## [Unreleased]

### Added

- Generate OLM bundle for Release 23.4.0 ([#350]).
- Missing CRD defaults for `status.conditions` field ([#360]).

### Changed

- Operator-rs: `0.40.2` -> `0.41.0` ([#349]).
- Use 0.0.0-dev product images for tests and examples ([#351]).
- Use testing-tools 0.2.0 ([#351]).
- Run as root group ([#359]).
- Added kuttl test suites ([#369])

### Fixed

- Fix missing quoting of env variables. This caused problems when env vars (e.g. from envOverrides) contained a whitespace ([#356]).
- Fix `hbase.zookeeper.quorum` to not contain the znode path, instead pass it via `zookeeper.znode.parent` ([#357]).
- Add `hbase.zookeeper.property.clientPort` setting, because hbase sometimes tried to access zookeeper with the (wrong) default port ([#357]).
- Fix test assert by adding variable quoting ([#359]).

[#349]: https://github.com/stackabletech/hbase-operator/pull/349
[#350]: https://github.com/stackabletech/hbase-operator/pull/350
[#351]: https://github.com/stackabletech/hbase-operator/pull/351
[#356]: https://github.com/stackabletech/hbase-operator/pull/356
[#357]: https://github.com/stackabletech/hbase-operator/pull/357
[#359]: https://github.com/stackabletech/hbase-operator/pull/359
[#360]: https://github.com/stackabletech/hbase-operator/pull/360
[#369]: https://github.com/stackabletech/hbase-operator/pull/369

## [23.4.0] - 2023-04-17

### Added

- Deploy default and support custom affinities ([#322]).
- OLM bundle files ([#333]).
- Extend cluster resources for status and cluster operation (paused, stopped) ([#336]).
- Cluster status conditions ([#337]).

### Changed

- [BREAKING]: Consolidated top level configuration to `clusterConfig` ([#334]).
- [BREAKING] Support specifying Service type.
  This enables us to later switch non-breaking to using `ListenerClasses` for the exposure of Services.
  This change is breaking, because - for security reasons - we default to the `cluster-internal` `ListenerClass`.
  If you need your cluster to be accessible from outside of Kubernetes you need to set `clusterConfig.listenerClass`
  to `external-unstable` ([#338]).
- `operator-rs` `0.36.0` -> `0.40.2` ([#334], [#336], [#339], [#340]).
- Use `build_rbac_resources` from operator-rs. This renames the `hbase-sa` ServiceAccount to `hbase-serviceaccount` ([#340]).

### Fixed

- Avoid empty log events dated to 1970-01-01 and improve the precision of the
  log event timestamps ([#339]).

### Removed

- [BREAKING]: Removed top level role/role group config ([#334]).

[#322]: https://github.com/stackabletech/hbase-operator/pull/322
[#333]: https://github.com/stackabletech/hbase-operator/pull/333
[#334]: https://github.com/stackabletech/hbase-operator/pull/334
[#336]: https://github.com/stackabletech/hbase-operator/pull/336
[#337]: https://github.com/stackabletech/hbase-operator/pull/337
[#338]: https://github.com/stackabletech/hbase-operator/pull/338
[#339]: https://github.com/stackabletech/hbase-operator/pull/339
[#340]: https://github.com/stackabletech/hbase-operator/pull/340

## [23.1.0] - 2023-01-23

### Added

- Log aggregation added ([#294]).

### Changed

- [BREAKING] Use Product image selection instead of version. `spec.version` has been replaced by `spec.image` ([#282]).
- Updated stackable image versions ([#275]).
- `operator-rs` `0.24.0` -> `0.30.2` ([#277], [#293], [#294]).
- Set runAsGroup to 1000 rather than 0 ([#283]).
- Fixed: `selector` in role groups now works. It was not working before ([#293])

[#275]: https://github.com/stackabletech/hbase-operator/pull/275
[#277]: https://github.com/stackabletech/hbase-operator/pull/277
[#282]: https://github.com/stackabletech/hbase-operator/pull/282
[#283]: https://github.com/stackabletech/hbase-operator/pull/283
[#293]: https://github.com/stackabletech/hbase-operator/pull/293
[#294]: https://github.com/stackabletech/hbase-operator/pull/294

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
