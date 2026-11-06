use crate::config::Manifest;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Write},
    path::Path,
};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub version: u32,
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub contract_address: Option<String>,
    pub event_type: String,
    pub payload: Value,
    #[serde(default)]
    pub conflict_keys: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
}
pub fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
impl Event {
    pub fn id(&self) -> String {
        hash(
            &serde_json::to_vec(&(
                self.chain_id,
                self.block_hash.to_lowercase(),
                self.transaction_hash.to_lowercase(),
                self.log_index,
            ))
            .unwrap(),
        )
    }
    pub fn order(&self) -> (u64, u64, u64) {
        (self.block_number, self.transaction_index, self.log_index)
    }
    pub fn normalize(&mut self) {
        self.block_hash.make_ascii_lowercase();
        self.transaction_hash.make_ascii_lowercase();
        if let Some(a) = &mut self.contract_address {
            a.make_ascii_lowercase();
        }
        self.conflict_keys.sort();
        self.dependencies.sort();
    }
}
pub fn fingerprint(events: &[Event]) -> String {
    hash(&serde_json::to_vec(events).unwrap())
}
fn evm(s: &str, bytes: usize) -> bool {
    s.len() == 2 + bytes * 2 && s.starts_with("0x") && s[2..].bytes().all(|c| c.is_ascii_hexdigit())
}
pub fn read(path: &Path, max: usize) -> Result<Vec<Event>> {
    let f = std::fs::File::open(path).with_context(|| format!("fixture {}", path.display()))?;
    ensure!(
        f.metadata()?.len() <= 256 * 1024 * 1024,
        "fixture exceeds 256 MiB"
    );
    let mut events = Vec::new();
    for (i, line) in BufReader::new(f).lines().enumerate() {
        let line = line?;
        ensure!(
            line.len() <= 1024 * 1024,
            "{}:{}: event exceeds 1 MiB",
            path.display(),
            i + 1
        );
        ensure!(
            !line.trim().is_empty(),
            "{}:{}: empty event record",
            path.display(),
            i + 1
        );
        let mut e: Event =
            serde_json::from_str(&line).with_context(|| format!("{}:{}", path.display(), i + 1))?;
        e.normalize();
        events.push(e);
        ensure!(events.len() <= max, "source event limit exceeded");
    }
    Ok(events)
}
pub fn load(m: &Manifest) -> Result<Vec<Event>> {
    let e = read(&m.fixture, m.testing.max_events)?;
    validate(&e, m).with_context(|| {
        format!(
            "fixture {} (event indices below are 1-based lines)",
            m.fixture.display()
        )
    })?;
    Ok(e)
}
pub fn validate(events: &[Event], m: &Manifest) -> Result<()> {
    ensure!(!events.is_empty(), "fixture must contain events");
    let mut ids = BTreeSet::new();
    let mut blocks = BTreeMap::new();
    let mut txs = BTreeMap::new();
    let mut positions = BTreeMap::new();
    let mut logs = BTreeSet::new();
    for (i, e) in events.iter().enumerate() {
        let n = i + 1;
        ensure!(
            e.version == 1
                && e.chain_id == m.chain_id
                && (m.from_block..=m.to_block).contains(&e.block_number),
            "line {n}: unsupported version, wrong chain or event outside range"
        );
        ensure!(
            evm(&e.block_hash, 32)
                && evm(&e.transaction_hash, 32)
                && e.contract_address.as_ref().is_none_or(|a| evm(a, 20)),
            "line {n}: invalid EVM hash/address"
        );
        ensure!(
            !e.event_type.is_empty() && e.payload.is_object(),
            "line {n}: event_type and object payload required"
        );
        ensure!(ids.insert(e.id()), "line {n}: duplicate canonical identity");
        if let Some(h) = blocks.insert(e.block_number, &e.block_hash) {
            ensure!(h == &e.block_hash, "line {n}: conflicting block hash");
        }
        let meta = (e.block_number, e.transaction_index);
        if let Some(old) = txs.insert(&e.transaction_hash, meta) {
            ensure!(old == meta, "line {n}: conflicting transaction metadata");
        }
        if let Some(old) = positions.insert(meta, &e.transaction_hash) {
            ensure!(
                old == &e.transaction_hash,
                "line {n}: ambiguous transaction index"
            );
        }
        ensure!(
            logs.insert((e.block_number, e.log_index)),
            "line {n}: ambiguous block log index"
        );
        if i > 0 {
            ensure!(
                events[i - 1].order() < e.order()
                    && (events[i - 1].block_number != e.block_number
                        || events[i - 1].log_index < e.log_index),
                "line {n}: invalid canonical order"
            );
        }
        ensure!(
            e.conflict_keys.iter().all(|s| !s.trim().is_empty())
                && e.conflict_keys.iter().collect::<BTreeSet<_>>().len() == e.conflict_keys.len(),
            "line {n}: invalid conflict keys"
        );
        ensure!(
            e.dependencies.iter().collect::<BTreeSet<_>>().len() == e.dependencies.len(),
            "line {n}: duplicate dependency"
        );
        if m.ordering.mode == "conflict_keys" {
            ensure!(
                !e.conflict_keys.is_empty(),
                "line {n}: conflict_keys policy requires metadata on every event"
            );
        }
    }
    let edges = crate::schedule::edges(events, &m.ordering.mode)?;
    for (to, parents) in edges.iter().enumerate() {
        ensure!(
            parents.iter().all(|&from| from < to),
            "line {}: dependency violates canonical order or forms a cycle",
            to + 1
        );
    }
    Ok(())
}
pub fn write(path: &Path, events: &[Event]) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let mut f = std::fs::File::create(path)?;
    for e in events {
        serde_json::to_writer(&mut f, e)?;
        writeln!(f)?;
    }
    Ok(())
}
