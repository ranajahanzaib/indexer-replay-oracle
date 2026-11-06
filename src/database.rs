use crate::{
    config::{Manifest, Projection},
    snapshot::{Cell, Snapshot, TableState},
};
use anyhow::{Result, bail, ensure};
use sqlx::{
    Column, Connection, Row, TypeInfo, ValueRef,
    postgres::{PgConnectOptions, PgConnection},
    sqlite::SqliteConnection,
};
use std::{collections::BTreeMap, str::FromStr};
pub struct Workspace {
    pub url: String,
    pub schema: Option<String>,
    _temp: tempfile::TempDir,
}
impl Workspace {
    pub async fn create(m: &Manifest) -> Result<Self> {
        let temp = tempfile::tempdir()?;
        if m.database.kind == "sqlite" {
            let path = temp.path().join("projection.sqlite");
            let url = format!("sqlite://{}?mode=rwc", path.display());
            let mut c = SqliteConnection::connect(&url).await?;
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&mut c)
                .await?;
            c.close().await?;
            Ok(Self {
                url,
                schema: None,
                _temp: temp,
            })
        } else {
            let url = m.postgres_url()?;
            let schema = format!(
                "iro_{}",
                crate::fixture::hash(temp.path().to_string_lossy().as_bytes())
            );
            let schema = schema[..48].to_string();
            let mut c = PgConnection::connect(&url).await.map_err(|_| {
                anyhow::anyhow!("could not connect to local PostgreSQL test server")
            })?;
            sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
                .execute(&mut c)
                .await?;
            c.close().await?;
            Ok(Self {
                url,
                schema: Some(schema),
                _temp: temp,
            })
        }
    }
    pub async fn cleanup(&self) -> Result<()> {
        if let Some(s) = &self.schema {
            let mut c = PgConnection::connect(&self.url).await?;
            sqlx::query(&format!("DROP SCHEMA \"{s}\" CASCADE"))
                .execute(&mut c)
                .await?;
            c.close().await?;
        }
        Ok(())
    }
    pub async fn snapshot(&self, p: &Projection) -> Result<Snapshot> {
        let mut tables = BTreeMap::new();
        let mut snapshot_bytes = 0i64;
        for t in &p.tables {
            let cols = t
                .columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(",");
            let mut rows = Vec::new();
            if let Some(schema) = &self.schema {
                let mut c = PgConnection::connect(&self.url).await?;
                let sizes = t
                    .columns
                    .iter()
                    .map(|c| format!("COALESCE(octet_length(\"{c}\"::text),0)::bigint"))
                    .collect::<Vec<_>>()
                    .join("+");
                let stats=sqlx::query(&format!("SELECT count(*) AS n, COALESCE(sum({sizes}),0)::bigint AS bytes FROM \"{schema}\".\"{}\"",t.name)).fetch_one(&mut c).await?;
                let n: i64 = stats.try_get("n")?;
                snapshot_bytes += stats.try_get::<i64, _>("bytes")?;
                ensure!(
                    n <= 100000 && snapshot_bytes <= 64 * 1024 * 1024,
                    "snapshot exceeds 100,000 rows per table or 64 MiB selected data"
                );
                let sql = format!("SELECT {cols} FROM \"{schema}\".\"{}\"", t.name);
                for row in sqlx::query(&sql).fetch_all(&mut c).await? {
                    let mut values = Vec::new();
                    for i in 0..t.columns.len() {
                        let raw = row.try_get_raw(i)?;
                        let ty = row.columns()[i].type_info().name();
                        let v = if raw.is_null() {
                            Cell::Null
                        } else {
                            match ty {
                                "INT2" => Cell::Integer(row.try_get::<i16, _>(i)?.to_string()),
                                "INT4" => Cell::Integer(row.try_get::<i32, _>(i)?.to_string()),
                                "INT8" => Cell::Integer(row.try_get::<i64, _>(i)?.to_string()),
                                "NUMERIC" => Cell::Decimal(
                                    row.try_get::<bigdecimal::BigDecimal, _>(i)?
                                        .normalized()
                                        .to_string(),
                                ),
                                "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" => {
                                    Cell::Text(row.try_get(i)?)
                                }
                                "BOOL" => Cell::Boolean(row.try_get(i)?),
                                "BYTEA" => Cell::Binary(hex::encode(row.try_get::<Vec<u8>, _>(i)?)),
                                "JSON" | "JSONB" => Cell::Json(serde_json::to_string(
                                    &row.try_get::<serde_json::Value, _>(i)?,
                                )?),
                                "TIMESTAMPTZ" => Cell::Timestamp(
                                    row.try_get::<chrono::DateTime<chrono::Utc>, _>(i)?
                                        .to_rfc3339(),
                                ),
                                "TIMESTAMP" => Cell::Timestamp(
                                    row.try_get::<chrono::NaiveDateTime, _>(i)?.to_string(),
                                ),
                                "DATE" => Cell::Timestamp(
                                    row.try_get::<chrono::NaiveDate, _>(i)?.to_string(),
                                ),
                                "FLOAT4" => Cell::Float(format!(
                                    "{:08x}",
                                    row.try_get::<f32, _>(i)?.to_bits()
                                )),
                                "FLOAT8" => Cell::Float(format!(
                                    "{:016x}",
                                    row.try_get::<f64, _>(i)?.to_bits()
                                )),
                                _ => bail!(
                                    "unsupported PostgreSQL column type {ty}; choose explicit supported projection columns"
                                ),
                            }
                        };
                        values.push(v);
                    }
                    rows.push(values);
                }
                c.close().await?;
            } else {
                let mut c = SqliteConnection::connect(&self.url).await?;
                // Check names explicitly: SQLite otherwise accepts a missing quoted column as a string literal.
                let info = sqlx::query(&format!("PRAGMA table_info(\"{}\")", t.name))
                    .fetch_all(&mut c)
                    .await?;
                for col in &t.columns {
                    ensure!(
                        info.iter().any(|r| r.get::<String, _>("name") == *col),
                        "missing SQLite table/column {}.{col}",
                        t.name
                    );
                }
                let sizes = t
                    .columns
                    .iter()
                    .map(|c| format!("COALESCE(length(CAST(\"{c}\" AS BLOB)),0)"))
                    .collect::<Vec<_>>()
                    .join("+");
                let stats = sqlx::query(&format!(
                    "SELECT count(*) AS n, COALESCE(sum({sizes}),0) AS bytes FROM \"{}\"",
                    t.name
                ))
                .fetch_one(&mut c)
                .await?;
                let n: i64 = stats.try_get("n")?;
                snapshot_bytes += stats.try_get::<i64, _>("bytes")?;
                ensure!(
                    n <= 100000 && snapshot_bytes <= 64 * 1024 * 1024,
                    "snapshot exceeds 100,000 rows per table or 64 MiB selected data"
                );
                for row in sqlx::query(&format!("SELECT {cols} FROM \"{}\"", t.name))
                    .fetch_all(&mut c)
                    .await?
                {
                    let mut values = Vec::new();
                    for i in 0..t.columns.len() {
                        let raw = row.try_get_raw(i)?;
                        let v = if raw.is_null() {
                            Cell::Null
                        } else {
                            match raw.type_info().name() {
                                "INTEGER" => Cell::Integer(row.try_get::<i64, _>(i)?.to_string()),
                                "REAL" => Cell::Float(format!(
                                    "{:016x}",
                                    row.try_get::<f64, _>(i)?.to_bits()
                                )),
                                "BLOB" => Cell::Binary(hex::encode(row.try_get::<Vec<u8>, _>(i)?)),
                                "TEXT" => Cell::Text(row.try_get(i)?),
                                ty => bail!("unsupported SQLite type {ty}"),
                            }
                        };
                        values.push(v);
                    }
                    rows.push(values);
                }
                c.close().await?;
            }
            let positions: Vec<_> = t
                .order_by
                .iter()
                .map(|k| t.columns.iter().position(|c| c == k).unwrap())
                .collect();
            rows.sort_by_key(|r| positions.iter().map(|&i| r[i].clone()).collect::<Vec<_>>());
            for pair in rows.windows(2) {
                ensure!(
                    positions.iter().any(|&i| pair[0][i] != pair[1][i]),
                    "order_by must uniquely identify projection rows in {}",
                    t.name
                );
            }
            tables.insert(
                t.name.clone(),
                TableState {
                    columns: t.columns.clone(),
                    keys: t.order_by.clone(),
                    rows,
                },
            );
        }
        Ok(Snapshot { version: 1, tables })
    }
}
pub async fn postgres_consumer_connect(url: &str, schema: &str) -> Result<PgConnection> {
    crate::config::identifier(schema)?;
    ensure!(
        schema.starts_with("iro_"),
        "consumer requires harness schema"
    );
    let opts = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);
    let mut c = PgConnection::connect_with(&opts).await?;
    let current: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&mut c)
        .await?;
    ensure!(current == schema, "schema isolation failed");
    Ok(c)
}
