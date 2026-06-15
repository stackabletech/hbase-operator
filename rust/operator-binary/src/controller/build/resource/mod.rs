//! Builders that turn a [`ValidatedCluster`](crate::controller::ValidatedCluster) into
//! Kubernetes resources.

pub mod config_map;
pub mod discovery;
pub mod pdb;
pub mod service;
pub mod statefulset;
