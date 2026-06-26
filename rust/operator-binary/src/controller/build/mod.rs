//! Builders that turn a [`ValidatedCluster`](crate::controller::ValidatedCluster) into
//! Kubernetes resources.

use std::str::FromStr;

use stackable_operator::v2::types::operator::RoleGroupName;

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod opa;
pub mod properties;
pub mod region_mover;
pub mod resource;
pub mod role;
