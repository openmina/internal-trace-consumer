// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::{path::Path, ObjectStore};
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::node::NodeIdentity;

#[derive(Deserialize)]
struct MetaToBeSaved {
    remote_addr: String,
    peer_id: String,
    submitter: String,
    graphql_control_port: u16,
}

pub struct NodeData {
    is_block_producer: bool,
    peer_id: String,
    submitted: String,
}

pub struct DiscoveryParams {
    pub offset_min: u64,
    pub limit: usize,
    pub only_block_producers: bool,
}

pub struct DiscoveryService {
    gcs: GoogleCloudStorage,
    node_data: HashMap<NodeIdentity, NodeData>,
}

fn offset_by_time(t: DateTime<Utc>) -> String {
    let t_str = t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let d_str = t.format("%Y-%m-%d");
    format!("submissions/{}/{}", d_str, t_str)
}

impl DiscoveryService {
    pub fn try_new() -> Result<Self> {
        let gcs = GoogleCloudStorageBuilder::from_env().build()?;
        let node_data = HashMap::new();
        Ok(Self { gcs, node_data })
    }

    pub async fn discover_participants(
        &mut self,
        params: DiscoveryParams,
    ) -> Result<Vec<NodeIdentity>> {
        let before = Utc::now() - chrono::Duration::minutes(params.offset_min as i64);
        let offset: Path = offset_by_time(before).try_into()?;
        let prefix: Path = "submissions".into();
        let it = self.gcs.list_with_offset(Some(&prefix), &offset).await?;
        let mut results = HashSet::new();
        // TODO: do something with errors (probably add logger and report them)
        let list_results: Vec<_> = it
            .filter_map(|result| async { result.ok() })
            .collect()
            .await;

        for object_meta in list_results {
            let bytes = self.gcs.get(&object_meta.location).await?.bytes().await?;
            let meta: MetaToBeSaved = serde_json::from_slice(&bytes)?;
            let colon_ix = meta.remote_addr.find(':').ok_or_else(|| {
                anyhow!(
                    "wrong remote address in submission {}: {}",
                    object_meta.location,
                    meta.remote_addr
                )
            })?;
            let addr = NodeIdentity {
                ip: meta.remote_addr[..colon_ix].to_string(),
                graphql_port: meta.graphql_control_port,
                submitter_pk: Some(meta.submitter),
            };
            if results.contains(&addr) {
                continue;
            }
            results.insert(addr.clone());
            if params.limit > 0 && results.len() >= params.limit {
                break;
            }
        }
        Ok(results.into_iter().collect())
    }
}