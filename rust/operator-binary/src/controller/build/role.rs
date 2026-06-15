//! Build-side behaviour of [`HbaseRole`]: the `bin/hbase` CLI subcommand name and the
//! port name/number mapping. These are operator/product knowledge, kept out of the `crd` module
//! so it stays a pure API definition.

use crate::crd::{
    HBASE_MASTER_METRICS_PORT, HBASE_MASTER_PORT, HBASE_MASTER_UI_PORT,
    HBASE_REGIONSERVER_METRICS_PORT, HBASE_REGIONSERVER_PORT, HBASE_REGIONSERVER_UI_PORT,
    HBASE_REST_METRICS_PORT, HBASE_REST_PORT, HBASE_REST_UI_PORT, HbaseRole,
};

const HBASE_UI_PORT_NAME_HTTP: &str = "ui-http";
const HBASE_UI_PORT_NAME_HTTPS: &str = "ui-https";
const HBASE_REST_PORT_NAME_HTTP: &str = "rest-http";
const HBASE_REST_PORT_NAME_HTTPS: &str = "rest-https";
const HBASE_METRICS_PORT_NAME: &str = "metrics";

impl HbaseRole {
    /// Returns the name of the role as it is needed by the `bin/hbase {cli_role_name} start` command.
    pub fn cli_role_name(&self) -> String {
        match self {
            HbaseRole::Master | HbaseRole::RegionServer => self.to_string(),
            // Of course it is not called "restserver", so we need to have this match
            // instead of just letting the Display impl do it's thing ;P
            HbaseRole::RestServer => "rest".to_string(),
        }
    }

    /// Returns required port name and port number tuples depending on the role.
    ///
    /// Hbase versions 2.6.* will have two ports for each role. The metrics are available on the
    /// UI port.
    pub fn ports(&self, https_enabled: bool) -> Vec<(String, u16)> {
        vec![
            (self.data_port_name(https_enabled), self.data_port()),
            (
                Self::ui_port_name(https_enabled).to_string(),
                self.ui_port(),
            ),
        ]
    }

    pub fn data_port(&self) -> u16 {
        match self {
            HbaseRole::Master => HBASE_MASTER_PORT,
            HbaseRole::RegionServer => HBASE_REGIONSERVER_PORT,
            HbaseRole::RestServer => HBASE_REST_PORT,
        }
    }

    pub fn data_port_name(&self, https_enabled: bool) -> String {
        match self {
            HbaseRole::Master | HbaseRole::RegionServer => self.to_string(),
            HbaseRole::RestServer => {
                if https_enabled {
                    HBASE_REST_PORT_NAME_HTTPS.to_owned()
                } else {
                    HBASE_REST_PORT_NAME_HTTP.to_owned()
                }
            }
        }
    }

    pub fn ui_port(&self) -> u16 {
        match self {
            HbaseRole::Master => HBASE_MASTER_UI_PORT,
            HbaseRole::RegionServer => HBASE_REGIONSERVER_UI_PORT,
            HbaseRole::RestServer => HBASE_REST_UI_PORT,
        }
    }

    /// Name of the port used by the Web UI, which depends on HTTPS usage
    pub fn ui_port_name(has_https_enabled: bool) -> &'static str {
        if has_https_enabled {
            HBASE_UI_PORT_NAME_HTTPS
        } else {
            HBASE_UI_PORT_NAME_HTTP
        }
    }

    pub fn metrics_port(&self) -> u16 {
        match self {
            HbaseRole::Master => HBASE_MASTER_METRICS_PORT,
            HbaseRole::RegionServer => HBASE_REGIONSERVER_METRICS_PORT,
            HbaseRole::RestServer => HBASE_REST_METRICS_PORT,
        }
    }

    pub fn metrics_port_name() -> &'static str {
        HBASE_METRICS_PORT_NAME
    }
}
