use crate::{
    fixture,
    report::{Bundle, Minimization, executable_fingerprint},
    runner,
    schedule::{Op, canonical},
    snapshot::differences,
};
use anyhow::{Result, ensure};
use std::time::{Duration, Instant};

pub async fn minimize(original: &Bundle) -> Result<Bundle> {
    ensure!(
        original.target_classification == "STATE_DIVERGENCE",
        "minimization supports stable replay-state divergences; golden and infrastructure failures must retain their oracle"
    );
    if let Some(hash) = &original.consumer_fingerprint {
        ensure!(
            executable_fingerprint(&original.manifest).as_ref() == Some(hash),
            "consumer executable changed"
        );
    }
    let m = &original.manifest;
    let start = Instant::now();
    let budget = m.testing.max_minimization_attempts;
    let deadline = Duration::from_secs(m.testing.minimization_seconds);
    let mut best = original.clone();
    let mut attempts = 0;
    for _ in 0..m.testing.confirmations {
        ensure!(
            start.elapsed() < deadline,
            "minimization deadline expired before stable reproduction was confirmed"
        );
        let e = runner::execute_bounded(
            m,
            &original.events,
            &original.schedule,
            deadline.saturating_sub(start.elapsed()),
        )
        .await?;
        ensure!(
            e.classification == "PASS" && e.snapshot != original.expected,
            "failure is intermittent or non-reproducible; minimization rejected"
        );
    }
    let mut complete = false;
    loop {
        // Descriptors stay small; clone only the candidate being executed.
        enum Reduction {
            Event(String),
            Operation(usize),
            Fault(usize),
        }
        let event_reductions = best
            .events
            .iter()
            .filter(|event| {
                best.events.len() > 1
                    && !best
                        .events
                        .iter()
                        .any(|e| e.dependencies.contains(&event.id()))
            })
            .map(|e| Reduction::Event(e.id()));
        let operation_reductions = (0..best.schedule.operations.len()).map(Reduction::Operation);
        let fault_reductions = best
            .schedule
            .operations
            .iter()
            .enumerate()
            .filter_map(|(i, op)| {
                matches!(op, Op::Apply { crash: true, .. }).then_some(Reduction::Fault(i))
            });
        let reductions: Vec<_> = event_reductions
            .chain(operation_reductions)
            .chain(fault_reductions)
            .collect();
        let mut improved = false;
        for reduction in reductions {
            if attempts >= budget || start.elapsed() >= deadline {
                break;
            }
            let mut c = best.clone();
            match reduction {
                Reduction::Event(id) => {
                    c.events.retain(|e| e.id() != id);
                    c.schedule
                        .operations
                        .retain(|o| !matches!(o,Op::Apply{event,..} if event==&id));
                }
                Reduction::Operation(i) => {
                    c.schedule.operations.remove(i);
                }
                Reduction::Fault(i) => {
                    if let Op::Apply { crash, .. } = &mut c.schedule.operations[i] {
                        *crash = false;
                    }
                }
            }
            if fixture::validate(&c.events, &c.manifest).is_err()
                || crate::schedule::validate(&c.schedule, &c.events, &c.manifest).is_err()
            {
                continue;
            }
            attempts += 1;
            let result = verify_candidate(c, start, deadline).await;
            let verified = match result {
                Ok(candidate) => candidate,
                Err(_) if start.elapsed() >= deadline => break,
                Err(e) => return Err(e),
            };
            if let Some(c) = verified {
                best = c;
                improved = true;
                break;
            }
        }
        if !improved {
            complete = attempts < budget && start.elapsed() < deadline;
            break;
        }
        if attempts >= budget || start.elapsed() >= deadline {
            break;
        }
    }
    best.minimization = Some(Minimization {
        attempts,
        original_events: original.events.len(),
        original_operations: original.schedule.operations.len(),
        confirmations: m.testing.confirmations,
        status: if complete {
            "stable_1_minimal"
        } else {
            "stable_budget_limited"
        }
        .into(),
    });
    Ok(best)
}
async fn verify_candidate(
    mut c: Bundle,
    start: Instant,
    deadline: Duration,
) -> Result<Option<Bundle>> {
    let clean = canonical(&c.events, c.schedule.seed);
    let baseline = runner::execute_bounded(
        &c.manifest,
        &c.events,
        &clean,
        deadline.saturating_sub(start.elapsed()),
    )
    .await?;
    if baseline.classification != "PASS" {
        return Ok(None);
    }
    let expected = baseline.snapshot.unwrap();
    let repeat = runner::execute_bounded(
        &c.manifest,
        &c.events,
        &clean,
        deadline.saturating_sub(start.elapsed()),
    )
    .await?;
    if repeat.classification != "PASS" || repeat.snapshot.as_ref() != Some(&expected) {
        return Ok(None);
    }
    for _ in 0..c.manifest.testing.confirmations {
        if start.elapsed() >= deadline {
            return Ok(None);
        }
        let mut e = runner::execute_bounded(
            &c.manifest,
            &c.events,
            &c.schedule,
            deadline.saturating_sub(start.elapsed()),
        )
        .await?;
        if e.classification != "PASS" || e.snapshot.as_ref() == Some(&expected) {
            return Ok(None);
        }
        e.classification = "STATE_DIVERGENCE".into();
        c.execution = e;
    }
    c.expected_fingerprint = Some(expected.fingerprint());
    c.expected = Some(expected);
    c.differences = differences(
        c.expected.as_ref().unwrap(),
        c.execution.snapshot.as_ref().unwrap(),
    );
    c.fixture_fingerprint = fixture::fingerprint(&c.events);
    Ok(Some(c))
}
