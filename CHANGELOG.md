# Changelog

## [Unreleased]

### Added

- BREAKING: Add listener support for HBase ([#639]).
- Adds new telemetry CLI arguments and environment variables ([#652]).
  - Use `--file-log-max-files` (or `FILE_LOG_MAX_FILES`) to limit the number of log files kept.
  - Use `--file-log-rotation-period` (or `FILE_LOG_ROTATION_PERIOD`) to configure the frequency of rotation.
  - Use `--console-log-format` (or `CONSOLE_LOG_FORMAT`) to set the format to `plain` (default) or `json`.
- Added support for HBase 2.6.2 ([#659]):
- Add RBAC rule to helm template for automatic cluster domain detection ([#675]).

### Changed

- BREAKING: Replace stackable-operator `initialize_logging` with stackable-telemetry `Tracing` ([#640], [#648], [#652]).
  - The console log level was set by `HBASE_OPERATOR_LOG`, and is now set by `CONSOLE_LOG_LEVEL`.
  - The file log level was set by `HBASE_OPERATOR_LOG`, and is now set by `FILE_LOG_LEVEL`.
  - The file log directory was set by `HBASE_OPERATOR_LOG_DIRECTORY`, and is now set
    by `FILE_LOG_DIRECTORY` (or via `--file-log-directory <DIRECTORY>`).
  - Replace stackable-operator `print_startup_string` with `tracing::info!` with fields.
- BREAKING: Inject the vector aggregator address into the vector config using the env var `VECTOR_AGGREGATOR_ADDRESS` instead
    of having the operator write it to the vector config ([#645]).
- test: Bump to Vector `0.46.1` ([#657]).
- test: Bump OPA to `1.4.2` ([#661]).
- BREAKING: Previously this operator would hardcode the UID and GID of the Pods being created to 1000/0, this has changed now ([#660])
  - The `runAsUser` and `runAsGroup` fields will not be set anymore by the operator
  - The defaults from the docker images itself will now apply, which will be different from 1000/0 going forward
  - This is marked as breaking because tools and policies might exist, which require these fields to be set
- BREAKING: Bump stackable-operator to 0.94.0 and update other dependencies ([#675]).
  - The default Kubernetes cluster domain name is now fetched from the kubelet API unless explicitly configured.
  - This requires operators to have the RBAC permission to get nodes/proxy in the apiGroup "". The helm-chart takes care of this.
  - The CLI argument `--kubernetes-node-name` or env variable `KUBERNETES_NODE_NAME` needs to be set. The helm-chart takes care of this.

### Fixed

- Use `json` file extension for log files ([#647]).
- Fix a bug where changes to ConfigMaps that are referenced in the HbaseCluster spec didn't trigger a reconciliation ([#645]).
- Allow uppercase characters in domain names ([#675]).

### Removed

- test: ZooKeeper 3.9.2 removed ([#654]).
- test: Remove HDFS `3.3.4`, `3.3.6`, and `3.4.0` ([#655]).
- test: HBase 2.4.18 removed ([#659]):
- Remove operator support for HBase 2.4 including the JMX exporter ([#672]).
- Remove the `lastUpdateTime` field from the stacklet status ([#675]).
- Remove role binding to legacy service accounts ([#675]).

[#639]: https://github.com/stackabletech/hbase-operator/pull/639
[#640]: https://github.com/stackabletech/hbase-operator/pull/640
[#645]: https://github.com/stackabletech/hbase-operator/pull/645
[#647]: https://github.com/stackabletech/hbase-operator/pull/647
[#648]: https://github.com/stackabletech/hbase-operator/pull/648
[#652]: https://github.com/stackabletech/hbase-operator/pull/652
[#654]: https://github.com/stackabletech/hbase-operator/pull/654
[#655]: https://github.com/stackabletech/hbase-operator/pull/655
[#657]: https://github.com/stackabletech/hbase-operator/pull/657
[#659]: https://github.com/stackabletech/hbase-operator/pull/659
[#660]: https://github.com/stackabletech/hbase-operator/pull/660
[#661]: https://github.com/stackabletech/hbase-operator/pull/661
[#672]: https://github.com/stackabletech/hbase-operator/pull/672
[#675]: https://github.com/stackabletech/hbase-operator/pull/675

## [25.3.0] - 2025-03-21

### Added

- The lifetime of auto generated TLS certificates is now configurable with the role and roleGroup
  config property `requestedSecretLifetime`. This helps reducing frequent Pod restarts ([#598]).
- Run a `containerdebug` process in the background of each HBase container to collect debugging information ([#605]).
- Aggregate emitted Kubernetes events on the CustomResources ([#612]).
- Support configuring JVM arguments ([#620]).
- Added support for HBase 2.6.1 ([#627]):

### Removed

- BREAKING: The field `config.hbaseOpts` has been removed. Use JVM argument overrides instead to configure additional JVM arguments ([#620]).

### Changed

- Bump `stackable-operator` to 0.87.0 and `stackable-versioned` to 0.6.0 ([#634]).
- Support moving regions to other Pods during graceful shutdown of region servers ([#570]).
- Default to OCI for image metadata and product image selection ([#611]).

[#570]: https://github.com/stackabletech/hbase-operator/pull/570
[#598]: https://github.com/stackabletech/hbase-operator/pull/598
[#605]: https://github.com/stackabletech/hbase-operator/pull/605
[#611]: https://github.com/stackabletech/hbase-operator/pull/611
[#612]: https://github.com/stackabletech/hbase-operator/pull/612
[#620]: https://github.com/stackabletech/hbase-operator/pull/620
[#627]: https://github.com/stackabletech/hbase-operator/pull/627
[#634]: https://github.com/stackabletech/hbase-operator/pull/634

## [24.11.1] - 2025-01-09

### Fixed

- BREAKING: Use distinct ServiceAccounts for the Stacklets, so that multiple Stacklets can be
  deployed in one namespace. Existing Stacklets will use the newly created ServiceAccounts after
  restart ([#594]).

[#594]: https://github.com/stackabletech/hbase-operator/pull/594

## [24.11.0] - 2024-11-18

### Added

- The operator can now run on Kubernetes clusters using a non-default cluster domain.
  Use the env var `KUBERNETES_CLUSTER_DOMAIN` or the operator Helm chart property `kubernetesClusterDomain` to set a non-default cluster domain ([#574]).

### Changed

- Reduce CRD size from `1.4MB` to `96KB` by accepting arbitrary YAML input instead of the underlying schema for the following fields ([#548]):
  - `podOverrides`
  - `affinity`
- Fix bug where the configuration of the `hbaseRootdir` at the role level is ignored ([#584]).

### Fixed

- Implement `envOverrides` for HbaseCluster ([#550]).
- Omid test: use 1.1.2, update default port number and raise test timeout ([#556]).
- An invalid `HBaseCluster` doesn't cause the operator to stop functioning (#[575]).

### Removed

- test: Remove ZooKeeper 3.8.4 ([#558]).

[#548]: https://github.com/stackabletech/hbase-operator/pull/548
[#550]: https://github.com/stackabletech/hbase-operator/pull/550
[#556]: https://github.com/stackabletech/hbase-operator/pull/556
[#558]: https://github.com/stackabletech/hbase-operator/pull/558
[#574]: https://github.com/stackabletech/hbase-operator/pull/574
[#584]: https://github.com/stackabletech/hbase-operator/pull/584

## [24.7.0] - 2024-07-24

### Added

- Added support for HBase 2.6.0 with the following changes ([#506]):
  - Added `clusterConfig.authorization` property to support the OPA authorizer.
  - Configure log4j2 properties.
  - Use built-in prometheus metric exporter.
- Added support for HBase 2.4.18 ([#523]).

### Changed

- Bump `stackable-operator` from `0.64.0` to `0.70.0` ([#524]).
- Bump `product-config` from `0.6.0` to `0.7.0` ([#524]).
- Bump other dependencies ([#527]).
- Remove sed calls to replace KRB REALM in configuration. IMPORTANT: This means the operator will ONLY work
  with HBase versions that are compiled against Hadoop 3.x as of now ([#531])

[#506]: https://github.com/stackabletech/hbase-operator/pull/506
[#523]: https://github.com/stackabletech/hbase-operator/pull/523
[#524]: https://github.com/stackabletech/hbase-operator/pull/524
[#527]: https://github.com/stackabletech/hbase-operator/pull/527
[#531]: https://github.com/stackabletech/hbase-operator/pull/531

## [24.3.0] - 2024-03-20

### Added

- Support user authentication using Kerberos ([#436]).
- More CRD documentation ([#425]).
- Helm: support labels in values.yaml ([#441]).

### Changed

- `operator-rs` `0.56.1` -> `0.57.0` ([#425]).
- Use new label builders ([#438]).
- Use Snafu instead of thiserror ([#438]).

### Removed

- [BREAKING] Removed legacy node selector on roleGroups ([#438]).
- Remove support for version `2.4.12` ([#454]).

### Fixed

- Processing of corrupted log events fixed; If errors occur, the error
  messages are added to the log event ([#511]).

[#425]: https://github.com/stackabletech/hbase-operator/pull/425
[#436]: https://github.com/stackabletech/hbase-operator/pull/436
[#438]: https://github.com/stackabletech/hbase-operator/pull/438
[#441]: https://github.com/stackabletech/hbase-operator/pull/441
[#454]: https://github.com/stackabletech/hbase-operator/pull/454
[#511]: https://github.com/stackabletech/hbase-operator/pull/511

## [23.11.0] - 2023-11-24

### Added

- Default stackableVersion to operator version ([#385]).
- Configuration overrides for the JVM security properties, such as DNS caching ([#389]).
- Support PodDisruptionBudgets ([#399]).
- Support graceful shutdown ([#402]).
- Added support for version 2.4.17 ([#403]).

### Changed

- `vector` `0.26.0` -> `0.33.0` ([#382], [#403]).
- Use jmx_exporter soft link instead of hardcoded version ([#403]).

### Fixed

- Fix Zookeeper hbase.rootdir when users point to discovery ConfigMap of ZookeeperCluster rather than ZNode. Print a warning in that case ([#394]).
- Default `hbase.unsafe.regionserver.hostname.disable.master.reversedns` to
  `true`, to ensure the names of RegionServers are resolved to their hostnames
  instead of IP addresses ([#418]).

### Removed

- Removed support for 2.4.6, 2.4.8, 2.4.9, 2.4.11 ([#403]).

[#382]: https://github.com/stackabletech/hbase-operator/pull/382
[#385]: https://github.com/stackabletech/hbase-operator/pull/385
[#389]: https://github.com/stackabletech/hbase-operator/pull/389
[#394]: https://github.com/stackabletech/hbase-operator/pull/394
[#399]: https://github.com/stackabletech/hbase-operator/pull/399
[#402]: https://github.com/stackabletech/hbase-operator/pull/402
[#403]: https://github.com/stackabletech/hbase-operator/pull/403
[#418]: https://github.com/stackabletech/hbase-operator/pull/418

## [23.7.0] - 2023-07-14

### Added

- Generate OLM bundle for Release 23.4.0 ([#350]).
- Missing CRD defaults for `status.conditions` field ([#360]).
- Set explicit resources on all containers ([#366], [#378]).
- Support podOverrides ([#371], [#373]).

### Changed

- Operator-rs: `0.40.2` -> `0.44.0` ([#349], [#366], [#375]).
- Use 0.0.0-dev product images for tests and examples ([#351]).
- Use testing-tools 0.2.0 ([#351]).
- Run as root group ([#359]).
- Added kuttl test suites ([#369])

### Fixed

- Fix missing quoting of env variables. This caused problems when env vars (e.g. from envOverrides) contained a whitespace ([#356]).
- Fix `hbase.zookeeper.quorum` to not contain the znode path, instead pass it via `zookeeper.znode.parent` ([#357]).
- Add `hbase.zookeeper.property.clientPort` setting, because hbase sometimes tried to access zookeeper with the (wrong) default port ([#357]).
- Fix test assert by adding variable quoting ([#359]).
- Increase the size limit of the log volume ([#375]).

[#349]: https://github.com/stackabletech/hbase-operator/pull/349
[#350]: https://github.com/stackabletech/hbase-operator/pull/350
[#351]: https://github.com/stackabletech/hbase-operator/pull/351
[#356]: https://github.com/stackabletech/hbase-operator/pull/356
[#357]: https://github.com/stackabletech/hbase-operator/pull/357
[#359]: https://github.com/stackabletech/hbase-operator/pull/359
[#360]: https://github.com/stackabletech/hbase-operator/pull/360
[#366]: https://github.com/stackabletech/hbase-operator/pull/366
[#369]: https://github.com/stackabletech/hbase-operator/pull/369
[#371]: https://github.com/stackabletech/hbase-operator/pull/371
[#373]: https://github.com/stackabletech/hbase-operator/pull/373
[#375]: https://github.com/stackabletech/hbase-operator/pull/375
[#378]: https://github.com/stackabletech/hbase-operator/pull/378

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
