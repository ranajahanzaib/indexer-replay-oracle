use crate::{config::Consumer, database::Workspace, fixture::Event};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Hello {
        request_id: u64,
        version: u32,
    },
    Apply {
        request_id: u64,
        event: Box<Event>,
        fault: Option<String>,
    },
    Shutdown {
        request_id: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Response {
    Hello {
        request_id: u64,
        version: u32,
        capabilities: Vec<String>,
        schema: Option<String>,
    },
    Ack {
        request_id: u64,
    },
    Error {
        request_id: u64,
        code: String,
        message: String,
    },
}
#[derive(Debug, thiserror::Error)]
#[error("{classification}: {message}")]
pub struct Failure {
    pub classification: String,
    pub message: String,
}
pub fn failure(class: &str, msg: impl Into<String>) -> anyhow::Error {
    Failure {
        classification: class.into(),
        message: msg.into(),
    }
    .into()
}
pub fn classification(e: &anyhow::Error) -> String {
    e.downcast_ref::<Failure>()
        .map(|f| f.classification.clone())
        .unwrap_or_else(|| "HARNESS_ERROR".into())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub request_id: u64,
    pub action: String,
    pub outcome: String,
}
pub struct Process {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    config: Consumer,
    stderr: Arc<Mutex<Vec<u8>>>,
    drain: tokio::task::JoinHandle<()>,
    pub capabilities: Vec<String>,
    next: u64,
}
impl Process {
    pub async fn start(config: &Consumer, w: &Workspace, seed: u64, run: &str) -> Result<Self> {
        let mut cmd = Command::new(&config.command[0]);
        cmd.args(&config.command[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env("IRO_DATABASE_URL", &w.url)
            .env("IRO_RUN_SEED", seed.to_string())
            .env("IRO_RUN_ID", run)
            .env_remove("IRO_DATABASE_SCHEMA");
        if let Some(s) = &w.schema {
            cmd.env("IRO_DATABASE_SCHEMA", s);
        }
        let mut child = cmd
            .spawn()
            .map_err(|_| failure("HARNESS_ERROR", "cannot launch consumer executable"))?;
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut err = child.stderr.take().unwrap();
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let captured = stderr.clone();
        let limit = config.max_stderr_bytes;
        let drain = tokio::spawn(async move {
            let mut buf = [0; 4096];
            while let Ok(n) = err.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let mut s = captured.lock().unwrap();
                let take = n.min(limit.saturating_sub(s.len()));
                s.extend_from_slice(&buf[..take]);
            }
        });
        let mut p = Self {
            child,
            input,
            output,
            config: config.clone(),
            stderr,
            drain,
            capabilities: Vec::new(),
            next: 1,
        };
        let response = p
            .exchange(
                &Request::Hello {
                    request_id: 0,
                    version: 1,
                },
                config.startup_timeout_seconds,
            )
            .await?;
        match response {
            Response::Hello {
                request_id: 0,
                version: 1,
                capabilities,
                schema,
            } => {
                if schema != w.schema {
                    return Err(failure(
                        "PROTOCOL_ERROR",
                        "consumer did not attest the requested database schema",
                    ));
                }
                if !capabilities.iter().any(|c| c == "durable_ack") {
                    return Err(failure(
                        "PROTOCOL_ERROR",
                        "required durable_ack capability missing",
                    ));
                }
                if w.schema.is_some() && !capabilities.iter().any(|c| c == "schema_isolation") {
                    return Err(failure(
                        "PROTOCOL_ERROR",
                        "required schema_isolation capability missing",
                    ));
                }
                p.capabilities = capabilities;
            }
            _ => {
                return Err(failure(
                    "PROTOCOL_ERROR",
                    "invalid hello/version/request ID",
                ));
            }
        };
        Ok(p)
    }
    async fn exchange(&mut self, req: &Request, seconds: u64) -> Result<Response> {
        let limit = self.config.max_message_bytes;
        let data = serde_json::to_vec(req)?;
        if data.len() + 1 > limit {
            return Err(failure("PROTOCOL_ERROR", "request exceeds message limit"));
        }
        let task = async {
            self.input
                .write_all(&data)
                .await
                .map_err(|_| failure("CONSUMER_CRASH", "consumer input closed"))?;
            self.input
                .write_all(b"\n")
                .await
                .map_err(|_| failure("CONSUMER_CRASH", "consumer input closed"))?;
            self.input
                .flush()
                .await
                .map_err(|_| failure("CONSUMER_CRASH", "consumer input closed"))?;
            let mut bytes = Vec::new();
            loop {
                let available = self
                    .output
                    .fill_buf()
                    .await
                    .map_err(|_| failure("PROTOCOL_ERROR", "response read failed"))?;
                if available.is_empty() {
                    return Err(failure("CONSUMER_CRASH", "consumer exited before response"));
                }
                let n = available
                    .iter()
                    .position(|&c| c == b'\n')
                    .map(|n| n + 1)
                    .unwrap_or(available.len());
                if bytes.len() + n > limit {
                    return Err(failure("PROTOCOL_ERROR", "response exceeds message limit"));
                }
                let end = available[n - 1] == b'\n';
                bytes.extend_from_slice(&available[..n]);
                self.output.consume(n);
                if end {
                    break;
                }
            }
            serde_json::from_slice(&bytes)
                .map_err(|_| failure("PROTOCOL_ERROR", "malformed consumer response"))
        };
        tokio::time::timeout(Duration::from_secs(seconds), task)
            .await
            .map_err(|_| failure("TIMEOUT", "consumer request timed out"))?
    }
    pub async fn apply(&mut self, event: Event, crash: bool) -> Result<()> {
        let id = self.next;
        self.next += 1;
        if crash
            && !self
                .capabilities
                .iter()
                .any(|c| c == "fault.after_commit_before_ack")
        {
            return Err(failure(
                "UNSUPPORTED_FAULT",
                "required fault.after_commit_before_ack capability missing",
            ));
        }
        let r = self
            .exchange(
                &Request::Apply {
                    request_id: id,
                    event: Box::new(event),
                    fault: crash.then(|| "after_commit_before_ack".into()),
                },
                self.config.timeout_seconds,
            )
            .await;
        if crash {
            match r {
                Err(e) if classification(&e) == "CONSUMER_CRASH" => {
                    let status = tokio::time::timeout(
                        Duration::from_secs(self.config.timeout_seconds),
                        self.child.wait(),
                    )
                    .await
                    .map_err(|_| {
                        failure("TIMEOUT", "fault hook closed stdout without exiting")
                    })??;
                    if status.code() == Some(86) {
                        return Ok(());
                    }
                    return Err(failure(
                        "CONSUMER_CRASH",
                        format!("fault hook must exit with negotiated code 86; observed {status}"),
                    ));
                }
                Err(e) => return Err(e),
                Ok(_) => {
                    return Err(failure(
                        "PROTOCOL_ERROR",
                        "fault hook acknowledged instead of exiting",
                    ));
                }
            }
        }
        match r? {
            Response::Ack { request_id } if request_id == id => Ok(()),
            Response::Error {
                request_id, code, ..
            } if request_id == id => Err(failure(
                "HANDLER_ERROR",
                format!("consumer handler error code {code}"),
            )),
            _ => Err(failure(
                "PROTOCOL_ERROR",
                "unexpected response or mismatched request ID",
            )),
        }
    }
    pub fn next_id(&self) -> u64 {
        self.next
    }
    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }
    pub async fn terminate(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill().await?;
        }
        self.child.wait().await?;
        Ok(())
    }
    pub async fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        let id = self.next;
        self.next += 1;
        if self.capabilities.iter().any(|c| c == "shutdown") {
            match self
                .exchange(
                    &Request::Shutdown { request_id: id },
                    self.config.timeout_seconds,
                )
                .await?
            {
                Response::Ack { request_id } if request_id == id => {}
                _ => {
                    return Err(failure(
                        "PROTOCOL_ERROR",
                        "invalid shutdown acknowledgement",
                    ));
                }
            }
            let status = tokio::time::timeout(
                Duration::from_secs(self.config.timeout_seconds),
                self.child.wait(),
            )
            .await
            .map_err(|_| failure("TIMEOUT", "consumer did not exit after shutdown"))??;
            if !status.success() {
                return Err(failure("CONSUMER_CRASH", "consumer failed during shutdown"));
            }
        } else {
            self.child.kill().await?;
            self.child.wait().await?;
        }
        Ok(())
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.drain.abort();
    }
}
