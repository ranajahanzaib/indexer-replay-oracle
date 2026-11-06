use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub name: String,
    pub fixture: PathBuf,
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: u64,
    pub expected_state: Option<PathBuf>,
    pub consumer: Consumer,
    pub database: Database,
    #[serde(default)]
    pub ordering: Ordering,
    pub projection: Projection,
    #[serde(default)]
    pub testing: Testing,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Consumer {
    pub command: Vec<String>,
    #[serde(default = "ten")]
    pub timeout_seconds: u64,
    #[serde(default = "ten")]
    pub startup_timeout_seconds: u64,
    #[serde(default = "message_limit")]
    pub max_message_bytes: usize,
    #[serde(default = "stderr_limit")]
    pub max_stderr_bytes: usize,
}
fn ten() -> u64 {
    10
}
fn message_limit() -> usize {
    1024 * 1024
}
fn stderr_limit() -> usize {
    65536
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Database {
    pub kind: String,
    pub url_env: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ordering {
    pub mode: String,
}
impl Default for Ordering {
    fn default() -> Self {
        Self {
            mode: "strict".into(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    pub tables: Vec<Table>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub order_by: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Testing {
    pub runs: usize,
    pub seed: u64,
    pub max_duplicates_per_event: usize,
    pub max_restarts: usize,
    pub enable_crash_faults: bool,
    pub enable_reordering: bool,
    pub max_events: usize,
    pub max_schedule_length: usize,
    pub max_run_seconds: u64,
    pub max_minimization_attempts: usize,
    pub minimization_seconds: u64,
    pub confirmations: usize,
}
impl Default for Testing {
    fn default() -> Self {
        Self {
            runs: 20,
            seed: 42,
            max_duplicates_per_event: 2,
            max_restarts: 4,
            enable_crash_faults: true,
            enable_reordering: false,
            max_events: 10000,
            max_schedule_length: 100000,
            max_run_seconds: 120,
            max_minimization_attempts: 100,
            minimization_seconds: 120,
            confirmations: 2,
        }
    }
}
pub fn identifier(s: &str) -> Result<()> {
    ensure!(
        !s.is_empty()
            && s.len() <= 63
            && s.bytes().enumerate().all(|(i, c)| c == b'_'
                || c.is_ascii_alphabetic()
                || (i > 0 && c.is_ascii_digit())),
        "invalid SQL identifier {s:?}"
    );
    Ok(())
}
impl Manifest {
    pub fn load(path: &Path) -> Result<Self> {
        let path = path
            .canonicalize()
            .with_context(|| format!("manifest {}", path.display()))?;
        ensure!(
            std::fs::metadata(&path)?.len() <= 1024 * 1024,
            "manifest exceeds 1 MiB"
        );
        let mut m: Self = toml::from_str(&std::fs::read_to_string(&path)?)
            .with_context(|| format!("manifest {}", path.display()))?;
        let base = path.parent().unwrap();
        m.fixture = base.join(&m.fixture);
        m.expected_state = m.expected_state.map(|p| base.join(p));
        // Executable paths containing separators are relative to the manifest, PATH names remain PATH names.
        if let Some(c) = m.consumer.command.first_mut()
            && c.contains('/')
            && !Path::new(c).is_absolute()
        {
            *c = base.join(&*c).to_string_lossy().into();
        }
        m.validate()?;
        Ok(m)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "unsupported manifest version");
        ensure!(
            !self.name.is_empty() && self.from_block <= self.to_block,
            "invalid name or block range"
        );
        ensure!(
            !self.consumer.command.is_empty() && !self.consumer.command[0].is_empty(),
            "consumer.command must contain an executable"
        );
        ensure!(
            (1..=300).contains(&self.consumer.timeout_seconds)
                && (1..=300).contains(&self.consumer.startup_timeout_seconds)
                && (256..=16 * 1024 * 1024).contains(&self.consumer.max_message_bytes)
                && self.consumer.max_stderr_bytes <= 1024 * 1024,
            "invalid protocol limits"
        );
        ensure!(
            self.ordering.mode == "strict" || self.ordering.mode == "conflict_keys",
            "unknown ordering mode"
        );
        ensure!(
            !self.testing.enable_reordering || self.ordering.mode == "conflict_keys",
            "reordering requires explicit conflict_keys policy"
        );
        ensure!(
            self.testing.runs > 0
                && self.testing.runs <= 10000
                && self.testing.max_events > 0
                && self.testing.max_events <= 1000000
                && self.testing.max_schedule_length > 0
                && self.testing.max_schedule_length <= 1000000
                && self.testing.max_duplicates_per_event <= 100
                && self.testing.max_restarts <= 1000
                && (1..=3600).contains(&self.testing.max_run_seconds)
                && self.testing.max_minimization_attempts <= 10000
                && (1..=3600).contains(&self.testing.minimization_seconds)
                && self.testing.confirmations > 0
                && self.testing.confirmations <= 10,
            "invalid testing limits"
        );
        match self.database.kind.as_str() {
            "sqlite" => ensure!(
                self.database.url_env.is_none(),
                "SQLite always uses a fresh temporary database"
            ),
            "postgres" => {
                self.postgres_url()?;
            }
            _ => bail!("database.kind must be sqlite or postgres"),
        }
        ensure!(
            !self.projection.tables.is_empty() && self.projection.tables.len() <= 128,
            "no projection tables configured"
        );
        let mut names = std::collections::BTreeSet::new();
        for t in &self.projection.tables {
            identifier(&t.name)?;
            ensure!(names.insert(&t.name), "duplicate projection table");
            ensure!(
                !t.columns.is_empty() && t.columns.len() <= 256 && !t.order_by.is_empty(),
                "columns and order_by must be nonempty"
            );
            let mut cols = std::collections::BTreeSet::new();
            for c in &t.columns {
                identifier(c)?;
                ensure!(cols.insert(c), "duplicate column");
            }
            for c in &t.order_by {
                ensure!(
                    t.columns.contains(c),
                    "order_by column {c} must be selected"
                );
            }
        }
        Ok(())
    }
    pub fn postgres_url(&self) -> Result<String> {
        let key = self
            .database
            .url_env
            .as_deref()
            .unwrap_or("IRO_POSTGRES_URL");
        let value = std::env::var(key)
            .with_context(|| format!("set {key} to a local PostgreSQL test server"))?;
        let url = url::Url::parse(&value)
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL URL (redacted)"))?;
        ensure!(
            matches!(url.scheme(), "postgres" | "postgresql")
                && matches!(
                    url.host_str(),
                    Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
                ),
            "PostgreSQL destination must be loopback"
        );
        ensure!(
            url.query().is_none() && url.fragment().is_none(),
            "PostgreSQL URL query parameters and fragments are forbidden"
        );
        Ok(value)
    }
}
