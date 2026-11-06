use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use iro::{config::Manifest, fixture, report::Bundle};
use std::path::PathBuf;
#[derive(Parser)]
#[command(
    name = "iro",
    version,
    about = "Deterministic replay testing for persistent event projections"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Fixture {
        #[command(subcommand)]
        command: FixtureCommand,
    },
    Verify {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        runs: Option<usize>,
        #[arg(long)]
        seed: Option<u64>,
        #[arg(long, default_value = "reports")]
        report_dir: PathBuf,
        #[arg(long)]
        junit: Option<PathBuf>,
    },
    Reproduce {
        #[arg(long)]
        bundle: PathBuf,
    },
    Minimize {
        #[arg(long)]
        bundle: PathBuf,
    },
}
#[derive(Subcommand)]
enum FixtureCommand {
    Record {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        chain_id: u64,
        #[arg(long)]
        from_block: u64,
        #[arg(long)]
        to_block: u64,
    },
    Validate {
        #[arg(long)]
        manifest: PathBuf,
    },
}
#[tokio::main]
async fn main() {
    match run().await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("iro: {e:#}");
            std::process::exit(2);
        }
    }
}
async fn run() -> Result<i32> {
    match Cli::parse().command {
        Command::Fixture {
            command: FixtureCommand::Validate { manifest },
        } => {
            let m = Manifest::load(&manifest)?;
            let e = fixture::load(&m)?;
            iro::report::load_expected(&m)?;
            println!(
                "VALID: {} events; SHA-256 {}",
                e.len(),
                fixture::fingerprint(&e)
            );
            Ok(0)
        }
        Command::Fixture {
            command:
                FixtureCommand::Record {
                    input,
                    output,
                    chain_id,
                    from_block,
                    to_block,
                },
        } => {
            ensure!(
                !output.exists(),
                "output already exists; refusing to overwrite fixture"
            );
            let mut events = fixture::read(&input, 1000000)?;
            events.retain(|e| {
                e.chain_id == chain_id && (from_block..=to_block).contains(&e.block_number)
            });
            events.sort_by_key(|e| e.order());
            let m: Manifest = toml::from_str(&format!(
                "version=1\nname='recording'\nfixture='events.jsonl'\nchain_id={chain_id}\nfrom_block={from_block}\nto_block={to_block}\n[consumer]\ncommand=['unused']\n[database]\nkind='sqlite'\n[[projection.tables]]\nname='unused'\ncolumns=['id']\norder_by=['id']\n"
            ))?;
            fixture::validate(&events, &m)?;
            fixture::write(&output, &events)?;
            println!(
                "Recorded {} events; SHA-256 {}",
                events.len(),
                fixture::fingerprint(&events)
            );
            Ok(0)
        }
        Command::Verify {
            manifest,
            runs,
            seed,
            report_dir,
            junit,
        } => {
            let mut m = Manifest::load(&manifest)?;
            if let Some(r) = runs {
                m.testing.runs = r;
            }
            if let Some(s) = seed {
                m.testing.seed = s;
            }
            let r = iro::report::verify(m, &report_dir).await?;
            r.write(&report_dir, junit.as_deref())?;
            println!(
                "Indexer Replay Oracle\nFixture: {} ({} events)\nDatabase: {}\nSeed: {}\nResult: {}",
                r.fixture_name, r.source_events, r.database, r.seed, r.classification
            );
            for s in &r.scenarios {
                println!(
                    "{}: {} ({} operations)",
                    s.schedule.id,
                    s.execution.classification,
                    s.schedule.operations.len()
                );
                for d in s.differences.iter().take(8) {
                    println!(
                        "  {} key {:?}, {}: {:?} -> {:?}; delta {:?}",
                        d.table, d.key, d.column, d.expected, d.actual, d.numeric_delta
                    );
                }
                if let Some(p) = &s.bundle {
                    println!("  iro reproduce --bundle {p}");
                }
            }
            println!("Reports: {}", report_dir.display());
            Ok(r.exit_code())
        }
        Command::Reproduce { bundle } => {
            let b = Bundle::load(&bundle)?;
            let e = b.reproduce().await?;
            println!(
                "Reproduction: {} (recorded {})",
                e.classification, b.target_classification
            );
            if let (Some(s), Some(expected)) = (&e.snapshot, &b.expected) {
                for d in iro::snapshot::differences(expected, s) {
                    println!(
                        "{} {:?} {}: {:?} -> {:?}",
                        d.table, d.key, d.column, d.expected, d.actual
                    );
                }
            }
            Ok(if e.classification == "PASS" {
                0
            } else if e.classification == "HARNESS_ERROR" || e.classification == "UNSUPPORTED_FAULT"
            {
                2
            } else {
                1
            })
        }
        Command::Minimize { bundle } => {
            let b = Bundle::load(&bundle)?;
            let reduced = iro::minimize::minimize(&b).await?;
            let path = bundle.join("minimized");
            ensure!(!path.exists(), "minimized output already exists");
            reduced.save(&path)?;
            println!(
                "Minimized: {} source events, {} operations; {:?}\niro reproduce --bundle {}",
                reduced.events.len(),
                reduced.schedule.operations.len(),
                reduced.minimization.as_ref().unwrap(),
                path.display()
            );
            Ok(0)
        }
    }
}
