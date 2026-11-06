use crate::{
    config::Manifest,
    fixture::{self, Event},
    runner::{self, Execution},
    schedule::Schedule,
    snapshot::{Difference, Snapshot, differences},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::path::Path;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bundle {
    pub version: u32,
    pub tool_version: String,
    pub fixture_fingerprint: String,
    pub consumer_fingerprint: Option<String>,
    pub manifest: Manifest,
    pub events: Vec<Event>,
    pub schedule: Schedule,
    pub expected: Option<Snapshot>,
    #[serde(default)]
    pub expected_fingerprint: Option<String>,
    pub target_classification: String,
    pub execution: Execution,
    pub differences: Vec<Difference>,
    pub minimization: Option<Minimization>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Minimization {
    pub attempts: usize,
    pub original_events: usize,
    pub original_operations: usize,
    pub confirmations: usize,
    pub status: String,
}
impl Bundle {
    pub fn save(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)?;
        std::fs::write(path.join("bundle.json"), serde_json::to_vec_pretty(self)?)?;
        fixture::write(&path.join("events.jsonl"), &self.events)?;
        Ok(())
    }
    pub fn load(path: &Path) -> Result<Self> {
        ensure!(
            std::fs::metadata(path.join("bundle.json"))?.len() <= 256 * 1024 * 1024,
            "bundle too large"
        );
        let data = std::fs::read(path.join("bundle.json"))?;
        let b: Self = serde_json::from_slice(&data)?;
        ensure!(
            b.version == 1 && b.tool_version == env!("CARGO_PKG_VERSION"),
            "unsupported bundle/tool version"
        );
        b.manifest.validate()?;
        if let Some(expected) = &b.expected {
            expected.validate(&b.manifest.projection)?;
        }
        ensure!(
            b.target_classification != "STATE_DIVERGENCE" || b.expected.is_some(),
            "state-divergence bundle lacks baseline"
        );
        fixture::validate(&b.events, &b.manifest)?;
        crate::schedule::validate(&b.schedule, &b.events, &b.manifest)?;
        ensure!(
            fixture::fingerprint(&b.events) == b.fixture_fingerprint,
            "bundle fixture fingerprint mismatch"
        );
        Ok(b)
    }
    pub async fn reproduce(&self) -> Result<Execution> {
        if let Some(expected) = &self.consumer_fingerprint {
            let actual = executable_fingerprint(&self.manifest);
            ensure!(
                actual.as_ref() == Some(expected),
                "consumer executable changed; restore the recorded binary before exact reproduction"
            );
        }
        let mut r = runner::execute(&self.manifest, &self.events, &self.schedule).await?;
        if r.classification == "PASS" && self.expected.is_some() && r.snapshot != self.expected {
            r.classification = if matches!(
                self.target_classification.as_str(),
                "STATE_DIVERGENCE" | "EXPECTED_STATE_MISMATCH" | "NONDETERMINISTIC_BASELINE"
            ) {
                self.target_classification.clone()
            } else {
                "STATE_DIVERGENCE".into()
            };
        }
        Ok(r)
    }
}
pub fn executable_fingerprint(m: &Manifest) -> Option<String> {
    let command = &m.consumer.command[0];
    let p = if command.contains('/') {
        Some(std::path::PathBuf::from(command))
    } else {
        std::env::split_paths(&std::env::var_os("PATH")?)
            .map(|p| p.join(command))
            .find(|p| p.is_file())
    };
    std::fs::read(p?).ok().map(|bytes| fixture::hash(&bytes))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScenarioResult {
    pub schedule: Schedule,
    pub execution: Execution,
    pub differences: Vec<Difference>,
    pub bundle: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub version: u32,
    pub tool_version: String,
    pub fixture_name: String,
    pub fixture_fingerprint: String,
    pub source_events: usize,
    pub database: String,
    pub seed: u64,
    pub configuration: Manifest,
    pub baseline_fingerprint: Option<String>,
    pub classification: String,
    pub scenarios: Vec<ScenarioResult>,
    pub unsupported: usize,
    pub skipped: Vec<String>,
    pub minimization_status: String,
}
impl Report {
    pub fn exit_code(&self) -> i32 {
        if self.classification == "PASS" {
            0
        } else if matches!(
            self.classification.as_str(),
            "HARNESS_ERROR" | "UNSUPPORTED_FAULT" | "INVALID_FIXTURE"
        ) {
            2
        } else {
            1
        }
    }
    pub fn write(&self, dir: &Path, junit: Option<&Path>) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join("report.json"), serde_json::to_vec_pretty(self)?)?;
        let mut markdown = format!(
            "# Indexer Replay Oracle\n\nFixture: {}\n\nSeed: {}\n\nResult: **{}**\n\nExecuted scenarios (including baselines): {}. Unsupported: {}.\n\nPassing is bounded replay equivalence, not a proof of semantic correctness.\n\n",
            self.fixture_name,
            self.seed,
            self.classification,
            self.scenarios.len(),
            self.unsupported
        );
        for s in &self.scenarios {
            markdown.push_str(&format!(
                "- {}: {} ({} operations)\n",
                s.schedule.id,
                s.execution.classification,
                s.schedule.operations.len()
            ));
            for d in &s.differences {
                markdown.push_str(&format!(
                    "  - {} {:?}, column {}: {:?} → {:?}, delta {:?}\n",
                    d.table, d.key, d.column, d.expected, d.actual, d.numeric_delta
                ));
            }
        }
        std::fs::write(dir.join("report.md"), markdown)?;
        if let Some(path) = junit {
            if let Some(p) = path.parent() {
                std::fs::create_dir_all(p)?;
            }
            let failures = self
                .scenarios
                .iter()
                .filter(|s| s.execution.classification != "PASS")
                .count();
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"iro\" tests=\"{}\" failures=\"{failures}\">\n",
                self.scenarios.len()
            );
            for s in &self.scenarios {
                xml.push_str(&format!("<testcase name=\"{}\">", escape(&s.schedule.id)));
                if s.execution.classification != "PASS" {
                    xml.push_str(&format!(
                        "<failure type=\"{}\">{}</failure>",
                        escape(&s.execution.classification),
                        escape(&format!("{:?}", s.differences))
                    ));
                }
                xml.push_str("</testcase>\n");
            }
            xml.push_str("</testsuite>\n");
            std::fs::write(path, xml)?;
        }
        Ok(())
    }
}
fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
pub async fn verify(m: Manifest, dir: &Path) -> Result<Report> {
    m.validate()?;
    let events = fixture::load(&m)?;
    let golden = load_expected(&m)?;
    let mut report = Report {
        version: 1,
        tool_version: env!("CARGO_PKG_VERSION").into(),
        fixture_name: m.name.clone(),
        fixture_fingerprint: fixture::fingerprint(&events),
        source_events: events.len(),
        database: m.database.kind.clone(),
        seed: m.testing.seed,
        configuration: m.clone(),
        baseline_fingerprint: None,
        classification: "PASS".into(),
        scenarios: vec![],
        unsupported: 0,
        skipped: [
            (!m.testing.enable_crash_faults, "crash-after-commit"),
            (!m.testing.enable_reordering, "admissible-reordering"),
            (
                m.testing.max_duplicates_per_event == 0,
                "duplicate-delivery",
            ),
            (m.testing.max_restarts == 0, "restart-recovery"),
        ]
        .into_iter()
        .filter(|(skip, _)| *skip)
        .map(|(_, name)| name.to_string())
        .collect(),
        minimization_status: "not_requested".into(),
    };
    let canonical = crate::schedule::canonical(&events, m.testing.seed);
    let first = runner::execute(&m, &events, &canonical).await?;
    report.scenarios.push(ScenarioResult {
        schedule: canonical.clone(),
        execution: first.clone(),
        differences: vec![],
        bundle: None,
    });
    if first.classification != "PASS" {
        report.classification = first.classification.clone();
        let path = dir.join("failure-canonical");
        make_bundle(&m, &events, canonical, None, first, vec![]).save(&path)?;
        report.scenarios[0].bundle = Some(path.to_string_lossy().into());
        return Ok(report);
    }
    let baseline = first.snapshot.clone().unwrap();
    report.baseline_fingerprint = Some(baseline.fingerprint());
    let mut second = canonical.clone();
    second.id = "baseline-determinism".into();
    let mut actual = runner::execute(&m, &events, &second).await?;
    let diff = actual
        .snapshot
        .as_ref()
        .map(|s| differences(&baseline, s))
        .unwrap_or_default();
    if actual.classification == "PASS" && actual.snapshot.as_ref() != Some(&baseline) {
        actual.classification = "NONDETERMINISTIC_BASELINE".into();
    }
    if actual.classification != "PASS" {
        report.classification = actual.classification.clone();
    }
    report.scenarios.push(ScenarioResult {
        schedule: second,
        execution: actual,
        differences: diff,
        bundle: None,
    });
    if report.classification != "PASS" {
        let scenario = report.scenarios.last_mut().unwrap();
        let path = dir.join("failure-baseline-determinism");
        make_bundle(
            &m,
            &events,
            scenario.schedule.clone(),
            Some(baseline.clone()),
            scenario.execution.clone(),
            scenario.differences.clone(),
        )
        .save(&path)?;
        scenario.bundle = Some(path.to_string_lossy().into());
        return Ok(report);
    }
    if let Some(golden) = golden
        && golden != baseline
    {
        let mut e = first.clone();
        e.classification = "EXPECTED_STATE_MISMATCH".into();
        let diff = differences(&golden, &baseline);
        let bundle = make_bundle(
            &m,
            &events,
            canonical.clone(),
            Some(golden),
            e.clone(),
            diff.clone(),
        );
        let path = dir.join("failure-golden");
        bundle.save(&path)?;
        report.classification = "EXPECTED_STATE_MISMATCH".into();
        report.scenarios.push(ScenarioResult {
            schedule: canonical.clone(),
            execution: e,
            differences: diff,
            bundle: Some(path.to_string_lossy().into()),
        });
        return Ok(report);
    }
    let mut schedules = vec![Schedule {
        id: "exact-replay".into(),
        ..canonical.clone()
    }];
    if m.testing.max_duplicates_per_event > 0 {
        let mut duplicate = canonical.clone();
        duplicate.id = "duplicate-delivery".into();
        duplicate.operations.insert(
            1,
            crate::schedule::Op::Apply {
                event: events[0].id(),
                crash: false,
            },
        );
        if m.testing.max_duplicates_per_event > 1 {
            duplicate.operations.push(crate::schedule::Op::Apply {
                event: events[0].id(),
                crash: false,
            });
        }
        schedules.push(duplicate);
    }
    if m.testing.max_restarts > 0 {
        let mut restart = canonical.clone();
        restart.id = "restart-recovery".into();
        restart.operations.insert(0, crate::schedule::Op::Restart);
        if m.testing.max_restarts > 1 {
            restart.operations.insert(2, crate::schedule::Op::Restart);
        }
        if m.testing.max_duplicates_per_event > 0 {
            restart.operations.push(crate::schedule::Op::Apply {
                event: events[0].id(),
                crash: false,
            });
        }
        schedules.push(restart);
    }
    if m.testing.enable_crash_faults {
        let mut crash = canonical.clone();
        crash.id = "crash-after-commit".into();
        if let crate::schedule::Op::Apply { crash, .. } = &mut crash.operations[0] {
            *crash = true;
        }
        schedules.push(crash);
    }
    if m.testing.enable_reordering {
        let mut policy = m.clone();
        policy.testing.max_duplicates_per_event = 0;
        policy.testing.max_restarts = 0;
        policy.testing.enable_crash_faults = false;
        let mut reorder = crate::schedule::generate(&events, &policy, 0)?;
        reorder.id = "admissible-reordering".into();
        schedules.push(reorder);
    }
    for i in 0..m.testing.runs {
        schedules.push(crate::schedule::generate(&events, &m, i)?);
    }
    for s in schedules {
        let mut e = runner::execute(&m, &events, &s).await?;
        let diff = e
            .snapshot
            .as_ref()
            .map(|state| differences(&baseline, state))
            .unwrap_or_default();
        if e.classification == "PASS" && e.snapshot.as_ref() != Some(&baseline) {
            e.classification = "STATE_DIVERGENCE".into();
        }
        let mut path = None;
        if e.classification != "PASS" {
            if report.classification == "PASS"
                || matches!(
                    e.classification.as_str(),
                    "HARNESS_ERROR" | "UNSUPPORTED_FAULT"
                )
            {
                report.classification = e.classification.clone();
            }
            if e.classification == "UNSUPPORTED_FAULT" {
                report.unsupported += 1;
            }
            let p = dir.join(format!("failure-{}", s.id));
            make_bundle(
                &m,
                &events,
                s.clone(),
                Some(baseline.clone()),
                e.clone(),
                diff.clone(),
            )
            .save(&p)?;
            path = Some(p.to_string_lossy().into());
        }
        report.scenarios.push(ScenarioResult {
            schedule: s,
            execution: e,
            differences: diff,
            bundle: path,
        });
    }
    Ok(report)
}
fn make_bundle(
    m: &Manifest,
    events: &[Event],
    schedule: Schedule,
    expected: Option<Snapshot>,
    execution: Execution,
    differences: Vec<Difference>,
) -> Bundle {
    let mut manifest = m.clone();
    manifest.fixture = "events.jsonl".into();
    manifest.expected_state = None;
    Bundle {
        version: 1,
        tool_version: env!("CARGO_PKG_VERSION").into(),
        fixture_fingerprint: fixture::fingerprint(events),
        consumer_fingerprint: executable_fingerprint(m),
        manifest,
        events: events.to_vec(),
        schedule,
        expected_fingerprint: expected.as_ref().map(Snapshot::fingerprint),
        expected,
        target_classification: execution.classification.clone(),
        execution,
        differences,
        minimization: None,
    }
}

pub fn load_expected(m: &Manifest) -> Result<Option<Snapshot>> {
    if let Some(path) = &m.expected_state {
        ensure!(
            std::fs::metadata(path)?.len() <= 128 * 1024 * 1024,
            "expected snapshot exceeds 128 MiB"
        );
        let expected: Snapshot = serde_json::from_slice(&std::fs::read(path)?)?;
        expected.validate(&m.projection)?;
        Ok(Some(expected))
    } else {
        Ok(None)
    }
}
