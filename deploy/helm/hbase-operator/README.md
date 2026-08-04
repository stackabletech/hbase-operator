<!-- markdownlint-disable MD034 -->
# Helm Chart for Stackable Operator for Apache HBase

Kubernetes operator for Apache HBase. Deploy and run HBase masters and region servers with the Stackable Data Platform (SDP).

## Requirements

- A running Kubernetes cluster
- [Helm](https://helm.sh/docs/intro/install/) 3.8 or newer, for OCI support

## Install

```bash
helm install hbase-operator oci://oci.stackable.tech/sdp-charts/hbase-operator
```

Add `--version` to pin a release, for example `--version 26.7.0`.
Released versions are listed in the [SDP release notes](https://docs.stackable.tech/home/stable/release-notes/) and on the [Stackable Hub](https://hub.stackable.tech/releases).

Since SDP 26.7 the chart is published to two registries:

- `oci://oci.stackable.tech/sdp-charts/hbase-operator`
- `oci://quay.io/stackable/sdp-charts/hbase-operator`

Both hold the same chart, but the registry you install from also decides where the operator pulls product images from.
Install from quay.io and the operator is configured to use product images from quay.io as well.

Operators are not usually installed on their own.
Most of them need the commons, secret and listener operators alongside them, and `stackablectl` installs a matching set in one step.
See the [documentation](https://docs.stackable.tech/home/stable/hbase/) for the full picture.

## Custom resources

This operator installs and manages its own CustomResourceDefinitions, so they are not part of this chart.
The resources it reconciles, and the configuration they accept, are described in the [documentation](https://docs.stackable.tech/home/stable/hbase/).Each CRD is also browsable on the [Stackable Hub](https://hub.stackable.tech/components/hbase), with its schema and the API versions served per SDP release.
## Links

- [Documentation](https://docs.stackable.tech/home/stable/hbase/)
- [Stackable Hub](https://hub.stackable.tech/)
- [Source](https://github.com/stackabletech/hbase-operator)
- [Report an issue](https://github.com/stackabletech/hbase-operator/issues)
- [Stackable Data Platform](https://stackable.tech/)

## License

[Open Software License version 3.0](https://github.com/stackabletech/hbase-operator/blob/main/LICENSE)
