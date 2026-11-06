use anyhow::{Result, bail, ensure};
use clap::Parser;
use iro::{
    fixture::Event,
    protocol::{Request, Response},
};
use num_bigint::BigInt;
use sqlx::{Connection, Row};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[derive(Parser)]
struct Args {
    #[arg(default_value = "serve")]
    command: String,
    #[arg(long,default_value="correct",value_parser=["correct","buggy","nondeterministic","malformed","exit","nohook","wrongid","oversized","timeout","wrongversion"])]
    mode: String,
}
enum Db {
    Sqlite(sqlx::SqliteConnection),
    Postgres(sqlx::PgConnection),
}
impl Db {
    async fn open(url: &str, schema: Option<&str>) -> Result<Self> {
        if url.starts_with("sqlite:") {
            let mut c = sqlx::SqliteConnection::connect(url).await?;
            sqlx::query("CREATE TABLE IF NOT EXISTS balances(account_id TEXT PRIMARY KEY,balance TEXT NOT NULL)").execute(&mut c).await?;
            sqlx::query("CREATE TABLE IF NOT EXISTS processed_events(event_id TEXT PRIMARY KEY)")
                .execute(&mut c)
                .await?;
            Ok(Self::Sqlite(c))
        } else {
            let mut c = iro::database::postgres_consumer_connect(
                url,
                schema.ok_or_else(|| anyhow::anyhow!("schema required"))?,
            )
            .await?;
            sqlx::query("CREATE TABLE IF NOT EXISTS balances(account_id TEXT PRIMARY KEY,balance TEXT NOT NULL)").execute(&mut c).await?;
            sqlx::query("CREATE TABLE IF NOT EXISTS processed_events(event_id TEXT PRIMARY KEY)")
                .execute(&mut c)
                .await?;
            Ok(Self::Postgres(c))
        }
    }
    async fn apply(&mut self, e: &Event, buggy: bool) -> Result<()> {
        let account = e.payload["account_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("account_id must be a string"))?;
        let amount = e.payload["amount"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("amount must be integer string"))?;
        ensure!(
            !amount.is_empty() && amount.bytes().all(|b| b.is_ascii_digit()),
            "amount must be nonnegative integer string"
        );
        let mut delta: BigInt = amount.parse()?;
        match e.event_type.as_str() {
            "BalanceCredited" => {}
            "BalanceDebited" => delta = -delta,
            _ => bail!("unknown event type"),
        }
        let id = e.id();
        // Both dialects keep registration and business effects in the same real transaction.
        match self {
            Self::Sqlite(c) => {
                let mut tx = c.begin().await?;
                let registered = sqlx::query(
                    "INSERT INTO processed_events(event_id) VALUES (?) ON CONFLICT DO NOTHING",
                )
                .bind(&id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if registered > 0 || buggy {
                    let old = sqlx::query("SELECT balance FROM balances WHERE account_id=?")
                        .bind(account)
                        .fetch_optional(&mut *tx)
                        .await?
                        .map(|r| r.get::<String, _>(0))
                        .unwrap_or_else(|| "0".into());
                    let balance = (old.parse::<BigInt>()? + &delta).to_string();
                    sqlx::query("INSERT INTO balances(account_id,balance) VALUES (?,?) ON CONFLICT(account_id) DO UPDATE SET balance=excluded.balance").bind(account).bind(balance).execute(&mut *tx).await?;
                }
                tx.commit().await?;
            }
            Self::Postgres(c) => {
                let mut tx = c.begin().await?;
                let registered = sqlx::query(
                    "INSERT INTO processed_events(event_id) VALUES ($1) ON CONFLICT DO NOTHING",
                )
                .bind(&id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if registered > 0 || buggy {
                    sqlx::query("INSERT INTO balances(account_id,balance) VALUES ($1,'0') ON CONFLICT DO NOTHING").bind(account).execute(&mut *tx).await?;
                    let old: String = sqlx::query_scalar(
                        "SELECT balance FROM balances WHERE account_id=$1 FOR UPDATE",
                    )
                    .bind(account)
                    .fetch_one(&mut *tx)
                    .await?;
                    let balance = (old.parse::<BigInt>()? + &delta).to_string();
                    sqlx::query("UPDATE balances SET balance=$2 WHERE account_id=$1")
                        .bind(account)
                        .bind(balance)
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
            }
        }
        Ok(())
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.command == "serve", "expected serve");
    let schema = std::env::var("IRO_DATABASE_SCHEMA").ok();
    let mut db = Db::open(&std::env::var("IRO_DATABASE_URL")?, schema.as_deref()).await?;
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    loop {
        let mut line = String::new();
        let n = (&mut input)
            .take(1024 * 1024 + 1)
            .read_line(&mut line)
            .await?;
        if n == 0 {
            break;
        }
        ensure!(
            n <= 1024 * 1024 && line.ends_with('\n'),
            "message too large"
        );
        let request: Request = serde_json::from_str(&line)?;
        if args.mode == "exit" {
            std::process::exit(7);
        }
        if args.mode == "malformed" {
            output.write_all(b"not json\n").await?;
            output.flush().await?;
            continue;
        }
        if args.mode == "oversized" {
            output.write_all(&vec![b'x'; 4096]).await?;
            output.write_all(b"\n").await?;
            output.flush().await?;
            continue;
        }
        if args.mode == "timeout" {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
        let shutdown = matches!(request, Request::Shutdown { .. });
        let response = match request {
            Request::Hello {
                request_id,
                version,
            } => {
                ensure!(version == 1, "unsupported protocol version");
                Response::Hello {
                    request_id: if args.mode == "wrongid" {
                        request_id + 1
                    } else {
                        request_id
                    },
                    version: if args.mode == "wrongversion" { 2 } else { 1 },
                    capabilities: [
                        "durable_ack",
                        "shutdown",
                        "schema_isolation",
                        "fault.after_commit_before_ack",
                    ]
                    .into_iter()
                    .filter(|c| args.mode != "nohook" || *c != "fault.after_commit_before_ack")
                    .map(String::from)
                    .collect(),
                    schema: schema.clone(),
                }
            }
            Request::Apply {
                request_id,
                mut event,
                fault,
            } => {
                if args.mode == "nondeterministic" {
                    event.payload["account_id"] =
                        serde_json::Value::String(format!("process-{}", std::process::id()));
                }
                match db.apply(&event, args.mode == "buggy").await {
                    Ok(()) => {
                        if let Some(f) = fault {
                            ensure!(f == "after_commit_before_ack", "unknown fault");
                            std::process::exit(86);
                        }
                        Response::Ack { request_id }
                    }
                    Err(_) => Response::Error {
                        request_id,
                        code: "INVALID_EVENT".into(),
                        message: "event application failed".into(),
                    },
                }
            }
            Request::Shutdown { request_id } => Response::Ack { request_id },
        };
        output.write_all(&serde_json::to_vec(&response)?).await?;
        output.write_all(b"\n").await?;
        output.flush().await?;
        if shutdown {
            break;
        }
    }
    Ok(())
}
use tokio::io::AsyncReadExt;
