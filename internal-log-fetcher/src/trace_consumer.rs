// Copyright (c) Viable Systems
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use tokio::process::Command;

pub struct TraceConsumer {
    consumer_executable_path: PathBuf,
    main_trace_file_path: PathBuf,
    graphql_port: u16,
    // TODO: process handle here?
}

impl TraceConsumer {
    pub fn new(
        consumer_executable_path: PathBuf,
        main_trace_file_path: PathBuf,
        graphql_port: u16,
    ) -> Self {
        Self {
            consumer_executable_path,
            main_trace_file_path,
            graphql_port,
        }
    }

    pub async fn run(&mut self) -> tokio::io::Result<tokio::process::Child> {
        let child = Command::new(&self.consumer_executable_path)
            .arg("serve")
            .arg("--trace-file")
            .arg(&self.main_trace_file_path)
            .arg("--port")
            .arg(format!("{}", self.graphql_port))
            .stdout(std::process::Stdio::null()) // TODO: log to some file instead
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;

        tokio::io::Result::Ok(child)
    }
}