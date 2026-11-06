use crate::fixture::hash;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Cell {
    Null,
    Integer(String),
    Decimal(String),
    Text(String),
    Binary(String),
    Json(String),
    Boolean(bool),
    Timestamp(String),
    Float(String),
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TableState {
    pub columns: Vec<String>,
    pub keys: Vec<String>,
    pub rows: Vec<Vec<Cell>>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub version: u32,
    pub tables: BTreeMap<String, TableState>,
}
impl Snapshot {
    pub fn fingerprint(&self) -> String {
        hash(&serde_json::to_vec(self).unwrap())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Difference {
    pub table: String,
    pub key: Vec<Cell>,
    pub column: String,
    pub expected: Option<Cell>,
    pub actual: Option<Cell>,
    pub numeric_delta: Option<String>,
}
pub fn differences(a: &Snapshot, b: &Snapshot) -> Vec<Difference> {
    let mut out = Vec::new();
    for name in a
        .tables
        .keys()
        .chain(b.tables.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        let left = a.tables.get(name);
        let right = b.tables.get(name);
        let t = left.or(right).unwrap();
        let index = |s: Option<&TableState>| -> BTreeMap<Vec<Cell>, Vec<Cell>> {
            s.map(|s| {
                s.rows
                    .iter()
                    .map(|r| {
                        (
                            s.keys
                                .iter()
                                .map(|k| r[s.columns.iter().position(|c| c == k).unwrap()].clone())
                                .collect(),
                            r.clone(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
        };
        let l = index(left);
        let r = index(right);
        for key in l
            .keys()
            .chain(r.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            for (i, c) in t.columns.iter().enumerate() {
                let x = l.get(key).and_then(|r| r.get(i)).cloned();
                let y = r.get(key).and_then(|r| r.get(i)).cloned();
                if x != y {
                    let number = |v: &Option<Cell>| -> Option<num_bigint::BigInt> {
                        match v {
                            Some(Cell::Integer(s) | Cell::Text(s)) => s.parse().ok(),
                            _ => None,
                        }
                    };
                    let numeric_delta =
                        number(&y).zip(number(&x)).map(|(y, x)| (y - x).to_string());
                    out.push(Difference {
                        table: name.clone(),
                        key: key.clone(),
                        column: c.clone(),
                        expected: x,
                        actual: y,
                        numeric_delta,
                    });
                }
            }
        }
    }
    out
}

impl Snapshot {
    pub fn validate(&self, projection: &crate::config::Projection) -> anyhow::Result<()> {
        anyhow::ensure!(self.version == 1, "unsupported snapshot version");
        anyhow::ensure!(
            self.tables.len() == projection.tables.len(),
            "snapshot table set differs from projection"
        );
        for config in &projection.tables {
            let table = self
                .tables
                .get(&config.name)
                .ok_or_else(|| anyhow::anyhow!("snapshot missing table {}", config.name))?;
            anyhow::ensure!(
                table.columns == config.columns && table.keys == config.order_by,
                "snapshot columns/keys differ from projection"
            );
            let mut previous = None;
            for row in &table.rows {
                anyhow::ensure!(
                    row.len() == table.columns.len(),
                    "snapshot row has invalid width"
                );
                let key: Vec<_> = table
                    .keys
                    .iter()
                    .map(|k| row[table.columns.iter().position(|c| c == k).unwrap()].clone())
                    .collect();
                anyhow::ensure!(
                    previous.as_ref().is_none_or(|p| p < &key),
                    "snapshot keys must be unique and canonically sorted"
                );
                previous = Some(key);
            }
        }
        Ok(())
    }
}
