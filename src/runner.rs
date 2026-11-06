use crate::{
    config::Manifest,
    database::Workspace,
    fixture::Event,
    protocol::{Observation, Process, classification},
    schedule::{Op, Schedule},
    snapshot::Snapshot,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Execution {
    pub snapshot: Option<Snapshot>,
    #[serde(default)]
    pub state_fingerprint: Option<String>,
    pub classification: String,
    pub message: Option<String>,
    pub capabilities: Vec<String>,
    pub observations: Vec<Observation>,
    pub stderr: String,
}
pub async fn execute(m: &Manifest, events: &[Event], s: &Schedule) -> Result<Execution> {
    execute_bounded(
        m,
        events,
        s,
        std::time::Duration::from_secs(m.testing.max_run_seconds),
    )
    .await
}
pub async fn execute_bounded(
    m: &Manifest,
    events: &[Event],
    s: &Schedule,
    budget: std::time::Duration,
) -> Result<Execution> {
    let started = std::time::Instant::now();
    crate::schedule::validate(s, events, m)?;
    let w = tokio::time::timeout(budget, Workspace::create(m))
        .await
        .map_err(|_| crate::protocol::failure("TIMEOUT", "workspace startup timed out"))??;
    let mut result = Execution {
        snapshot: None,
        state_fingerprint: None,
        classification: "PASS".into(),
        message: None,
        capabilities: vec![],
        observations: vec![],
        stderr: String::new(),
    };
    let mut process: Option<Process> = None;
    let run = async {
        process = Some(Process::start(&m.consumer, &w, s.seed, &s.id).await?);
        result.capabilities = process.as_ref().unwrap().capabilities.clone();
        for op in &s.operations {
            match op {
                Op::Restart => {
                    let p = process.as_mut().unwrap();
                    p.stop().await?;
                    result.stderr.push_str(&p.stderr_text());
                    process.take();
                    process = Some(Process::start(&m.consumer, &w, s.seed, &s.id).await?);
                    result.observations.push(Observation {
                        request_id: 0,
                        action: "restart".into(),
                        outcome: "relaunched".into(),
                    });
                }
                Op::Apply { event, crash } => {
                    let e = events.iter().find(|e| e.id() == *event).unwrap().clone();
                    let p = process.as_mut().unwrap();
                    let id = p.next_id();
                    let response = p.apply(e.clone(), *crash).await;
                    result.observations.push(Observation {
                        request_id: id,
                        action: format!("apply:{event}"),
                        outcome: if let Err(e) = &response {
                            classification(e)
                        } else {
                            if *crash {
                                "exit_86".into()
                            } else {
                                "ack".into()
                            }
                        },
                    });
                    response?;
                    if *crash {
                        result.stderr.push_str(&p.stderr_text());
                        process.take();
                        process = Some(Process::start(&m.consumer, &w, s.seed, &s.id).await?);
                        let p = process.as_mut().unwrap();
                        let id = p.next_id();
                        let response = p.apply(e, false).await;
                        result.observations.push(Observation {
                            request_id: id,
                            action: format!("redeliver:{event}"),
                            outcome: match &response {
                                Ok(()) => "ack".into(),
                                Err(e) => classification(e),
                            },
                        });
                        response?;
                    }
                }
            }
        }
        process.as_mut().unwrap().stop().await?;
        let snapshot = w.snapshot(&m.projection).await?;
        result.state_fingerprint = Some(snapshot.fingerprint());
        result.snapshot = Some(snapshot);
        Ok::<_, anyhow::Error>(())
    };
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(m.testing.max_run_seconds)
            .min(budget.saturating_sub(started.elapsed())),
        run,
    )
    .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            result.classification = classification(&e);
            result.message = Some(if result.classification == "HARNESS_ERROR" {
                "database inspection or harness operation failed".into()
            } else {
                e.to_string()
            });
        }
        Err(_) => {
            result.classification = "TIMEOUT".into();
            result.message = Some("scenario duration limit exceeded".into());
        }
    }
    if let Some(mut p) = process.take() {
        if result.classification != "PASS" {
            p.terminate().await?;
        }
        result.stderr.push_str(&p.stderr_text());
        drop(p);
    }
    result.stderr.truncate(
        result
            .stderr
            .floor_char_boundary(m.consumer.max_stderr_bytes.min(result.stderr.len())),
    );
    // Stderr is retained in memory only: arbitrary external diagnostics can contain credentials.
    result.stderr.clear();
    w.cleanup().await?;
    Ok(result)
}
