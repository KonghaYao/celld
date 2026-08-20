// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The D1 CLI reads operator input and migration files outside node storage.
#![allow(clippy::disallowed_methods)]

//! `celld d1` — run SQL and migrations against a deployed D1 database.
//!
//! A D1 database is a cell, so it is only reachable through the node that owns
//! it. This command finds the fleet the way `celld diagnose` does — it walks
//! the node leases in the bucket, and it reads the same shared secret — and
//! then sends the SQL to a live node's `/__d1/` route, which forwards to the
//! owner. The CLI therefore holds no ownership logic and no SQLite: it reaches
//! the database over the dispatch a Worker's `env.DB` reaches it over.
//!
//! The route is authenticated. A D1 database holds application data and
//! answers arbitrary SQL, and its scope is an HMAC over the database identity
//! rather than a secret, so the unauthenticated `/do/` route
//! refuses a D1 scope and sends the caller here.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use serde_json::Value;

use crate::bucket::Bucket;

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    let project = crate::deploy::read_d1_project(command.project.clone())?;
    // The declaration, not just the name: `migrations_dir` belongs to the
    // binding, so the directory has to come from the database the command
    // names or one database's migrations reach another.
    let declaration = project
        .databases
        .iter()
        .find(|database| database.database_name == command.database)
        .ok_or_else(|| {
            anyhow!(
                "the project declares no D1 database named {:?}; it declares {}",
                command.database,
                if project.databases.is_empty() {
                    "none".to_string()
                } else {
                    project
                        .databases
                        .iter()
                        .map(|database| format!("{:?}", database.database_name))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            )
        })?;
    let scope = crate::js::d1_cell_scope(&declaration.database_identity);
    let database = Reachable::open(&command, scope).await?;

    match command.action {
        Action::Execute { command: sql, file } => {
            let source = match (sql, file) {
                (Some(sql), _) => sql,
                (None, Some(file)) => std::fs::read_to_string(&file)
                    .with_context(|| format!("read {}", file.display()))?,
                (None, None) => unreachable!("parse refuses neither --command nor --file"),
            };
            let result = database.exec(&source).await?;
            // The rows, not only the count: an operator running a SELECT is
            // asking for its result, and `wrangler d1 execute` prints it.
            for set in result
                .get("results")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let columns: Vec<&str> = set
                    .get("columns")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect();
                let shaped: Vec<Value> = set
                    .get("rows")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_array)
                    .map(|row| {
                        Value::Object(
                            columns
                                .iter()
                                .zip(row)
                                .map(|(column, value)| (column.to_string(), value.clone()))
                                .collect(),
                        )
                    })
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&Value::Array(shaped))
                        .context("encode the result rows")?
                );
            }
            println!(
                "Executed {} statement(s) in {:.2} sec",
                result
                    .get("count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                result
                    .get("duration")
                    .and_then(Value::as_f64)
                    .unwrap_or_default()
                    / 1000.0
            );
        }
        Action::MigrationsList => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                println!("No migrations to apply.");
                return Ok(());
            }
            println!("Migrations to be applied:");
            for migration in &pending {
                println!("  {}", migration.name);
            }
        }
        Action::MigrationsApply => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                println!("No migrations to apply.");
                return Ok(());
            }
            for migration in pending {
                database
                    .apply(&migration, &declaration.migrations_table)
                    .await?;
                println!("Applied {}", migration.name);
            }
        }
    }
    Ok(())
}

struct Migration {
    name: String,
    sql: String,
}

/// One entrance into the fleet: a node's address and the identity a request
/// to it must be signed for.
struct Entrance {
    node: String,
    url: String,
    path: String,
}

/// A database resolved to the nodes that can carry SQL to it.
struct Reachable {
    http: reqwest::Client,
    /// The fleet's shared peer secret, read from the bucket. `/__d1/` refuses
    /// an unsigned request, because a D1 database holds application data and
    /// answers arbitrary SQL.
    auth: crate::peer_auth::PeerAuth,
    /// Every node with a live lease. Any one of them forwards to the owner, so
    /// the list is a set of equal entrances and not a route: it stays correct
    /// even when the cell moves between calls.
    entrances: Vec<Entrance>,
}

impl Reachable {
    async fn open(command: &Command, scope: String) -> anyhow::Result<Self> {
        let bucket = crate::fleet::bucket_client(
            &command.bucket,
            command.endpoint.as_deref(),
            &command.region,
        )?;
        let nodes = live_nodes(&bucket, command.unsafe_public_advertise).await?;
        // The same secret and the same source shape `celld diagnose` uses.
        let auth =
            crate::peer_auth::PeerAuth::new(crate::peer_auth::load_existing(&bucket).await?, "d1")?;
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            // A migration on a large table is not a diagnostic ping, so this
            // budget is the handler budget's order rather than the probe's.
            .timeout(Duration::from_secs(120))
            .build()
            .context("build the D1 client")?;
        let path = format!("/__d1/{scope}");
        Ok(Self {
            http,
            auth,
            entrances: nodes
                .into_iter()
                .map(|(node, addr)| Entrance {
                    node,
                    url: format!("http://{addr}{path}"),
                    path: path.clone(),
                })
                .collect(),
        })
    }

    /// A lease outlives the node that wrote it, and a node that drains keeps
    /// its lease while it refuses work, so the first entrance in the list is
    /// not always usable. Move to the next one when a node cannot be reached
    /// or is draining, because in both cases the request provably did not run.
    /// Nothing else is retried: a timeout is not evidence that the work was
    /// refused, and re-sending a migration that in fact ran would apply it
    /// twice.
    async fn call(&self, body: Value) -> anyhow::Result<Value> {
        // Signed per entrance, because the signature binds the node it is
        // addressed to: a signature for one node is not replayable at another.
        let payload = serde_json::to_vec(&body).context("encode the request")?;
        let mut refused = Vec::new();
        for (index, entrance) in self.entrances.iter().enumerate() {
            let last = index + 1 == self.entrances.len();
            let url = &entrance.url;
            let request = self
                .auth
                .sign(
                    self.http.post(url).body(payload.clone()),
                    "POST",
                    &entrance.path,
                    &payload,
                    &entrance.node,
                )
                .with_context(|| format!("sign the request to {}", entrance.node))?;
            let response = match request.send().await {
                Ok(response) => response,
                Err(error) if error.is_connect() && !last => {
                    refused.push(format!("{url}: {error}"));
                    continue;
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error)).with_context(|| {
                        format!("reach the database at {url}{}", report(&refused))
                    });
                }
            };
            let status = response.status();
            let text = response.text().await.context("read the database reply")?;
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                && text.contains("\"draining\"")
                && !last
            {
                refused.push(format!("{url}: the node is draining"));
                continue;
            }
            // The dispatcher refusing at the door is the doc comment's
            // criterion exactly: the request provably did not run, so the
            // next entrance is safe to try where a timeout would not be.
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                && text.contains("dispatcher unavailable")
                && !last
            {
                refused.push(format!("{url}: the node's dispatcher is unavailable"));
                continue;
            }
            let value: Value = serde_json::from_str(&text)
                .with_context(|| format!("decode the database reply ({status}): {text}"))?;
            if let Some(error) = value.get("error").and_then(Value::as_str) {
                bail!("{error}{}", report(&refused));
            }
            if !status.is_success() {
                bail!(
                    "the database refused the request ({status}): {text}{}",
                    report(&refused)
                );
            }
            return value.get("result").cloned().ok_or_else(|| {
                anyhow!(
                    "the database reply carried no result: {text}{}",
                    report(&refused)
                )
            });
        }
        bail!("no node in the fleet answered{}", report(&refused))
    }

    async fn exec(&self, source: &str) -> anyhow::Result<Value> {
        // `rows: true` is the CLI's difference from the Worker binding's
        // exec(): the operator asked to see the result, not only the count.
        self.call(serde_json::json!({ "exec": { "sql": source, "rows": true } }))
            .await
    }

    /// Rows for one statement, as `all()` would shape them.
    async fn query(&self, sql: &str) -> anyhow::Result<Vec<Vec<Value>>> {
        let result = self
            .call(serde_json::json!({ "statements": [{ "sql": sql, "params": [] }] }))
            .await?;
        let rows = result
            .get(0)
            .and_then(|first| first.get("rows"))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("the database reply carried no rows"))?;
        Ok(rows
            .iter()
            .filter_map(|row| row.as_array().cloned())
            .collect())
    }

    /// Every migration file the database has not recorded, in wrangler's
    /// order. The bookkeeping table is the binding's `migrations_table`,
    /// validated at config read, so the name joined into this SQL is a plain
    /// identifier and nothing else.
    async fn pending(
        &self,
        declaration: &crate::deploy::D1Declaration,
    ) -> anyhow::Result<Vec<Migration>> {
        let on_disk = read_migrations(&declaration.migrations_dir)?;
        let table = &declaration.migrations_table;
        self.exec(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" (\
               id INTEGER PRIMARY KEY AUTOINCREMENT, \
               name TEXT UNIQUE, \
               applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);"
        ))
        .await?;
        let applied = self
            .query(&format!("SELECT name FROM \"{table}\""))
            .await?
            .into_iter()
            .filter_map(|row| row.first().and_then(Value::as_str).map(str::to_string))
            .collect::<std::collections::BTreeSet<_>>();
        Ok(on_disk
            .into_iter()
            .filter(|migration| !applied.contains(&migration.name))
            .collect())
    }

    /// The cell applies the file and records it in one transaction, so a
    /// migration that fails part-way rolls back whole and a re-run starts
    /// clean. The CLI sends the file and the name separately and joins no SQL:
    /// the name reaches the bookkeeping row as a bound parameter, and a file
    /// that ends in a comment or an unterminated statement is refused by the
    /// cell rather than repaired by a guess here.
    async fn apply(&self, migration: &Migration, table: &str) -> anyhow::Result<()> {
        self.call(serde_json::json!({
            "migrate": { "name": migration.name, "sql": migration.sql, "table": table },
        }))
        .await
        .with_context(|| format!("apply {}", migration.name))?;
        Ok(())
    }
}

fn read_migrations(directory: &std::path::Path) -> anyhow::Result<Vec<Migration>> {
    if !directory.exists() {
        bail!(
            "no migrations directory at {}; create it, or set `migrations_dir` \
             on the d1_databases entry",
            directory.display()
        );
    }
    let mut migrations = Vec::new();
    for entry in
        std::fs::read_dir(directory).with_context(|| format!("read {}", directory.display()))?
    {
        let path = entry?.path();
        // A directory is never a migration, whatever it is named — but a
        // directory HOLDING migrations is a layout this command does not
        // read, and skipping it silently would apply half a schema.
        if path.is_dir() {
            let nested = std::fs::read_dir(&path)
                .map(|entries| {
                    entries.flatten().any(|entry| {
                        entry
                            .path()
                            .extension()
                            .is_some_and(|extension| extension == "sql")
                    })
                })
                .unwrap_or(false);
            if nested {
                bail!(
                    "{} holds .sql files, and celld reads migrations from a flat \
                     directory only; move them into {} itself",
                    path.display(),
                    directory.display()
                );
            }
            continue;
        }
        if path.extension().is_none_or(|extension| extension != "sql") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("migration {} has a non-UTF-8 name", path.display()))?
            .to_string();
        let sql =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        migrations.push(Migration { name, sql });
    }
    // Wrangler orders migrations by their numeric prefix, not by the file
    // name as text: `10_` runs after `9_`, where a text sort runs it first
    // and the ALTERs land before the CREATE they depend on. A file without
    // the prefix has no place in that order, so it is refused rather than
    // guessed about.
    let mut ordered = Vec::with_capacity(migrations.len());
    for migration in migrations {
        let digits: String = migration
            .name
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if digits.is_empty() {
            bail!(
                "migration {:?} has no numeric prefix; wrangler names migrations \
                 NNNN_description.sql, and the prefix is the application order",
                migration.name
            );
        }
        // A parse failure past this point is overflow, not absence: 20 or
        // more digits do not fit a u64. Reporting that as "no numeric
        // prefix" sent the operator to rename a file that plainly starts
        // with digits.
        let number: u64 = digits.parse().map_err(|_| {
            anyhow!(
                "migration {:?} has a numeric prefix that does not fit in an \
                 unsigned 64-bit number; use a shorter prefix",
                migration.name
            )
        })?;
        ordered.push((number, migration));
    }
    ordered.sort_by(|left, right| (left.0, &left.1.name).cmp(&(right.0, &right.1.name)));
    Ok(ordered
        .into_iter()
        .map(|(_, migration)| migration)
        .collect())
}

/// Every node with an unexpired lease and a usable address, as (id, address).
/// The id is not decoration: a peer signature binds the node it is addressed
/// to. Any of them will do, because the route forwards to the owner, so the
/// CLI does not resolve ownership and cannot race a cell that moves while it
/// works. They are all returned rather than only the first, because a lease
/// outlives the node that wrote it and a draining node keeps its lease while
/// it refuses work.
async fn live_nodes(
    bucket: &Bucket,
    unsafe_public_advertise: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    let nodes = crate::fleet::node_lease_ids(bucket).await?;
    if nodes.is_empty() {
        bail!("no node leases in the bucket; celld d1 needs a running fleet");
    }
    let mut reachable = Vec::new();
    let mut refused = Vec::new();
    for node in &nodes {
        let lease = match crate::fleet::live_node_lease(bucket, node).await {
            Ok(Some(lease)) => lease,
            Ok(None) => continue,
            Err(error) => {
                refused.push(format!("{node}: {error}"));
                continue;
            }
        };
        let advertise = match crate::startup::parse_advertise(&lease.addr) {
            Ok(advertise) => advertise,
            Err(error) => {
                refused.push(format!(
                    "{node}: malformed address {:?}: {error}",
                    lease.addr
                ));
                continue;
            }
        };
        if advertise.is_public_ip() && !unsafe_public_advertise {
            refused.push(format!(
                "{node}: public advertise address {}; use a private overlay or \
                 --unsafe-public-advertise",
                lease.addr
            ));
            continue;
        }
        reachable.push((node.clone(), lease.addr.clone()));
    }
    if reachable.is_empty() {
        bail!(
            "no node in the fleet is reachable; every lease is expired or refused{}",
            report(&refused)
        );
    }
    Ok(reachable)
}

/// Indented detail under a refusal, or nothing when there is none to give.
fn report(refused: &[String]) -> String {
    if refused.is_empty() {
        String::new()
    } else {
        format!("\n  {}", refused.join("\n  "))
    }
}

enum Action {
    /// Exactly one of the two, enforced at parse.
    Execute {
        command: Option<String>,
        file: Option<PathBuf>,
    },
    MigrationsList,
    MigrationsApply,
}

struct Command {
    action: Action,
    database: String,
    project: Option<PathBuf>,
    bucket: String,
    endpoint: Option<String>,
    region: String,
    unsafe_public_advertise: bool,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let action = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            "execute" => None,
            // An option in the subcommand slot means the subcommand is
            // missing, not that the option names one. Reporting `--bucket` as
            // an unknown subcommand would send an operator looking for a
            // spelling mistake they did not make.
            "migrations" => match arguments.next().as_deref() {
                Some("apply") => Some(Action::MigrationsApply),
                Some("list") => Some(Action::MigrationsList),
                Some(other) if !other.starts_with('-') => {
                    bail!("unknown `celld d1 migrations` subcommand: {other}")
                }
                _ => bail!("`celld d1 migrations` needs `apply` or `list`"),
            },
            other => bail!("unknown `celld d1` subcommand: {other}"),
        };
        let database = arguments
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| anyhow!("celld d1 needs a database name"))?;

        let env = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let mut sql = None;
        let mut file = None;
        let mut project = None;
        let mut bucket = env("CELLD_BUCKET");
        let mut endpoint = env("S3_ENDPOINT");
        let mut region = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION"));
        let mut unsafe_public_advertise = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--command" => sql = Some(value("--command")?),
                "--file" => file = Some(PathBuf::from(value("--file")?)),
                "--bucket" => {
                    bucket = Some(value("--bucket")?.trim_start_matches("s3://").to_string());
                }
                "--endpoint" => endpoint = Some(value("--endpoint")?),
                "--region" => region = Some(value("--region")?),
                "--unsafe-public-advertise" => unsafe_public_advertise = true,
                "--help" | "-h" => return Ok(None),
                other if other.starts_with('-') => {
                    bail!("unknown option: {other}; run `celld d1 --help` for usage")
                }
                other => project = Some(PathBuf::from(other)),
            }
        }
        let action = match action {
            Some(action) => {
                // Accepting the flag and ignoring it would tell an operator
                // their SQL ran when nothing read it.
                if sql.is_some() || file.is_some() {
                    bail!(
                        "`celld d1 migrations` takes no --command or --file; \
                         migrations come from the migrations directory"
                    );
                }
                action
            }
            None => {
                if sql.is_some() == file.is_some() {
                    bail!("celld d1 execute requires exactly one of --command or --file");
                }
                Action::Execute { command: sql, file }
            }
        };
        let bucket = bucket.ok_or_else(|| {
            anyhow!("celld d1 requires --bucket [s3://|gs://|az://]NAME (or CELLD_BUCKET)")
        })?;
        Ok(Some(Self {
            action,
            database,
            project,
            bucket,
            endpoint,
            region: region.unwrap_or_else(|| "us-east-1".to_string()),
            unsafe_public_advertise,
        }))
    }
}

pub fn print_help() {
    println!(
        r#"celld d1 — run SQL and migrations against a deployed D1 database

USAGE:
  celld d1 execute DATABASE (--command SQL | --file PATH) [PROJECT] --bucket NAME
  celld d1 migrations apply DATABASE [PROJECT] --bucket NAME
  celld d1 migrations list  DATABASE [PROJECT] --bucket NAME

DATABASE is a `database_name` the project declares in `d1_databases`. PROJECT
is the directory holding wrangler.jsonc, and defaults to the working directory.
The `database_id` is the stable database identity when the declaration has one.
Otherwise, celld uses the `database_name` as the identity.

celld d1 needs a running fleet. It finds a node through the bucket's node
leases and sends the SQL to that node, which forwards it to the node that owns
the database. It signs each request with the fleet's shared secret, which it
reads from the bucket, so it needs the same bucket credentials celld deploy
needs.

Migration files are `NNNN_description.sql` in `migrations/`, applied in the
order of their numeric prefixes as wrangler applies them. `migrations_dir` on
the d1_databases entry moves the directory, and `migrations_table` renames the
bookkeeping table. celld records applied migrations in that table
(`d1_migrations` by default), the same table and columns
`wrangler d1 migrations` uses.

OPTIONS:
  --command SQL    SQL to run; several statements are permitted
  --file PATH      Read the SQL from a file instead
  --bucket [s3://|gs://|az://]NAME[/PREFIX]
                   Fleet bucket; same as CELLD_BUCKET
  --endpoint URL   S3-compatible endpoint; same as S3_ENDPOINT
  --region REGION  Storage region (default: AWS_REGION or us-east-1)
  --unsafe-public-advertise
                   Permit a node whose advertised address is a public IP
  -h, --help       Show this help"#
    );
}
