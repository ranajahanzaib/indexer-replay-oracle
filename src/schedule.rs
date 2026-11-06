use crate::{config::Manifest, fixture::Event};
use anyhow::{Result, ensure};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Op {
    Apply {
        event: String,
        #[serde(default)]
        crash: bool,
    },
    Restart,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    pub id: String,
    pub seed: u64,
    pub operations: Vec<Op>,
}
pub fn edges(events: &[Event], mode: &str) -> Result<Vec<BTreeSet<usize>>> {
    let ids: BTreeMap<_, _> = events
        .iter()
        .enumerate()
        .map(|(i, e)| (e.id(), i))
        .collect();
    let mut parents = vec![BTreeSet::new(); events.len()];
    let mut last = BTreeMap::new();
    for (i, e) in events.iter().enumerate() {
        if mode == "strict" && i > 0 {
            parents[i].insert(i - 1);
        }
        for key in &e.conflict_keys {
            if let Some(j) = last.insert(key, i) {
                parents[i].insert(j);
            }
        }
        for d in &e.dependencies {
            let j = *ids
                .get(d)
                .ok_or_else(|| anyhow::anyhow!("event {}: nonexistent dependency {d}", i + 1))?;
            parents[i].insert(j);
        }
    }
    let mut done = BTreeSet::new();
    while done.len() < events.len() {
        let before = done.len();
        for (i, p) in parents.iter().enumerate() {
            if p.is_subset(&done) {
                done.insert(i);
            }
        }
        ensure!(before != done.len(), "dependency cycle");
    }
    Ok(parents)
}
pub fn canonical(events: &[Event], seed: u64) -> Schedule {
    Schedule {
        id: "canonical".into(),
        seed,
        operations: events
            .iter()
            .map(|e| Op::Apply {
                event: e.id(),
                crash: false,
            })
            .collect(),
    }
}
pub fn generate(events: &[Event], m: &Manifest, index: usize) -> Result<Schedule> {
    let seed = m.testing.seed.wrapping_add(index as u64);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let parents = edges(events, &m.ordering.mode)?;
    let mut done = BTreeSet::new();
    let mut order = Vec::new();
    while order.len() < events.len() {
        let ready: Vec<_> = (0..events.len())
            .filter(|i| !done.contains(i) && parents[*i].is_subset(&done))
            .collect();
        ensure!(!ready.is_empty(), "dependency cycle");
        let i = if m.testing.enable_reordering {
            ready[rng.gen_range(0..ready.len())]
        } else {
            ready[0]
        };
        done.insert(i);
        order.push(i);
    }
    let mut operations = Vec::new();
    let mut restarts = 0;
    for (position, i) in order.into_iter().enumerate() {
        if restarts < m.testing.max_restarts && (position == 0 || rng.gen_bool(0.25)) {
            operations.push(Op::Restart);
            restarts += 1;
        }
        let crash = m.testing.enable_crash_faults && position == 0;
        operations.push(Op::Apply {
            event: events[i].id(),
            crash,
        });
        if m.testing.max_duplicates_per_event > 0 {
            let copies = if position == 0 {
                m.testing.max_duplicates_per_event
            } else {
                rng.gen_range(0..=m.testing.max_duplicates_per_event)
            };
            for _ in 0..copies {
                operations.push(Op::Apply {
                    event: events[i].id(),
                    crash: false,
                });
            }
        }
    }
    // Delayed retry after the rest of the source stream, including a real process restart.
    if m.testing.max_duplicates_per_event > 0 {
        if restarts < m.testing.max_restarts {
            operations.push(Op::Restart);
        }
        // Move one immediate retry to the end, preserving the configured duplicate bound.
        if let Some(p) = operations
            .iter()
            .enumerate()
            .filter(|(_, o)| matches!(o,Op::Apply{event,..} if event==&events[0].id()))
            .nth(1)
            .map(|(p, _)| p)
        {
            let retry = operations.remove(p);
            operations.push(retry);
        }
    }
    let s = Schedule {
        id: format!("scenario-{index:04}"),
        seed,
        operations,
    };
    validate(&s, events, m)?;
    Ok(s)
}
pub fn validate(s: &Schedule, events: &[Event], m: &Manifest) -> Result<()> {
    ensure!(
        s.operations.len() <= m.testing.max_schedule_length,
        "schedule length limit exceeded"
    );
    let ids: BTreeMap<_, _> = events
        .iter()
        .enumerate()
        .map(|(i, e)| (e.id(), i))
        .collect();
    let p = edges(events, &m.ordering.mode)?;
    let mut seen = BTreeSet::new();
    let mut counts = vec![0; events.len()];
    let mut restarts = 0;
    for op in &s.operations {
        match op {
            Op::Restart => {
                restarts += 1;
                ensure!(restarts <= m.testing.max_restarts, "restart limit exceeded");
            }
            Op::Apply { event, crash } => {
                let i = *ids
                    .get(event)
                    .ok_or_else(|| anyhow::anyhow!("schedule references unknown event"))?;
                ensure!(
                    !crash || m.testing.enable_crash_faults,
                    "undeclared crash fault"
                );
                if !seen.contains(&i) {
                    ensure!(p[i].is_subset(&seen), "illegal first-delivery ordering");
                    seen.insert(i);
                }
                counts[i] += 1;
                ensure!(
                    counts[i] <= 1 + m.testing.max_duplicates_per_event,
                    "duplicate bound exceeded"
                );
            }
        }
    }
    ensure!(seen.len() == events.len(), "incomplete source delivery");
    Ok(())
}
