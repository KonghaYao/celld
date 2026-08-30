// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The D1 CLI reads operator input and migration files outside node storage.
#![allow(clippy::disallowed_methods)]
// Reaching stdout past `Output` is what mixed the result rows and the
// summary onto one stream here in the first place.

//! `celld d1` — run SQL and migrations against a deployed D1 database.
//!
//! A D1 database is a cell, so it is only reachable through the node that owns
//! it. This command finds the fleet the way `celld diagnose` does — it walks
//! the node leases in the bucket, and it reads the same shared secret — and
//! then sends the SQL to a live node's `/runtime/` route, which forwards to the
//! owner. The CLI therefore holds no ownership logic and no SQLite: it reaches
//! the database over the dispatch a Worker's `env.DB` reaches it over.
//!
//! The route is authenticated. A D1 database holds application data and
//! answers arbitrary SQL, and its scope is an HMAC over the database identity
//! rather than a secret, so the unauthenticated `/do/` route
//! refuses a D1 scope and sends the caller here.

use std::io::Read as _;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context};
use serde_json::Value;

use crate::cli_options::FleetFlags;
use crate::cli_options::FLEET_HELP;
use crate::cli_output::{align, Format, Output, Record};
use crate::note;
use crate::operator_cell::{Fleet, Reachable, Subject};
use std::borrow::Cow;

/// One import reply, as the owner returns it inside `result`.
struct ImportReply(Value);

impl Record for ImportReply {
    fn json(&self) -> Value {
        self.0.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(
            serde_json::to_string_pretty(&self.0)
                .expect("a serde_json::Value always serializes as JSON"),
        )
    }
}

/// One row of a `d1 execute` result set.
///
/// The text is rendered when the set is laid out, because a column's width
/// depends on every row in it. The JSON keeps the column names as keys, so
/// `--json` output is self-describing without the header line.
struct Row {
    object: Value,
    text: String,
}

impl Record for Row {
    fn json(&self) -> Value {
        self.object.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.text)
    }
}

/// One migration, listed rather than applied.
struct Pending {
    name: String,
}

impl Record for Pending {
    fn json(&self) -> Value {
        serde_json::json!({ "migration": self.name })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }
}

/// A SQL value as a table cell. JSON keeps the real type; this is only the
/// human rendering, so a string prints bare and a null prints as nothing.
fn cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

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
    let storage = command.fleet.clone().resolve("celld d1")?;
    let import_seed = match &command.action {
        Action::Import { file } => Some(validate_import_seed(file)?),
        _ => None,
    };
    let timeout = match &command.action {
        Action::Import { .. } => import_seed
            .as_ref()
            .map(|(_, size)| crate::operator_cell::import_timeout(*size))
            .unwrap_or_else(|| std::time::Duration::from_secs(120)),
        Action::Branch { .. } => crate::operator_cell::branch_timeout(0),
        _ => std::time::Duration::from_secs(120),
    };
    let database = Reachable::open(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise: command.unsafe_public_advertise,
        },
        Subject {
            noun: "database",
            source: "d1",
            timeout,
        },
        scope,
        None,
    )
    .await?;

    let mut out = Output::new(if command.json {
        Format::Json
    } else {
        Format::Text
    });
    match command.action {
        Action::Execute { command: sql, file } => {
            let source = match (sql, file) {
                (Some(sql), _) => sql,
                (None, Some(file)) => {
                    reject_sqlite_seed_for_execute(&file)?;
                    std::fs::read_to_string(&file)
                        .with_context(|| format!("read {}", file.display()))?
                }
                (None, None) => unreachable!("parse refuses neither --command nor --file"),
            };
            let result = database.exec(&source).await?;
            // The rows, not only the count: an operator running a SELECT is
            // asking for its result, and `wrangler d1 execute` prints it.
            for (index, set) in result
                .get("results")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
            {
                let columns: Vec<String> = set
                    .get("columns")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
                let values: Vec<Vec<Value>> = set
                    .get("rows")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_array)
                    .map(|row| row.to_vec())
                    .collect();
                let cells: Vec<Vec<String>> = values
                    .iter()
                    .map(|row| row.iter().map(cell).collect())
                    .collect();
                let (header, rule, lines) = align(&columns, &cells);
                // A blank line between result sets, so several statements
                // read as several tables rather than one ragged one.
                if index > 0 {
                    out.header("")?;
                }
                out.header(&header)?;
                out.header(&rule)?;
                for (row, text) in values.iter().zip(lines) {
                    let object =
                        Value::Object(columns.iter().cloned().zip(row.iter().cloned()).collect());
                    out.row(&Row { object, text })?;
                }
            }
            // stderr, not stdout. This line used to follow the rows on the
            // same stream, so `celld d1 execute | jq` parsed prose.
            let count = result
                .get("count")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let milliseconds = result
                .get("duration")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            // "0.00 sec" reads as a missing measurement rather than a fast
            // statement, the same way "0.0s" did on a listing summary.
            let took = if milliseconds < 1000.0 {
                format!("{}ms", milliseconds.round() as u64)
            } else {
                format!("{:.2} sec", milliseconds / 1000.0)
            };
            note!("Executed {count} statement(s) in {took}");
        }
        Action::Import { .. } => {
            let (absolute, _) = import_seed
                .ok_or_else(|| anyhow!("internal error: import seed was not validated"))?;
            let result = database.import(&absolute).await?;
            if command.json {
                out.row(&ImportReply(result.clone()))?;
            } else {
                let bytes = result.get("bytes").and_then(Value::as_u64).unwrap_or_default();
                let duration_ms = result
                    .get("duration_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let snapshot_txid = result
                    .get("snapshot_txid")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                note!(
                    "Imported {bytes} bytes in {duration_ms}ms (snapshot txid {snapshot_txid})"
                );
            }
        }
        Action::Branch { parent_bucket } => {
            let project_id = crate::d1_branch::branch_cli_project_id(
                &project.project_id,
                Some(&storage.bucket),
            );
            crate::d1_branch::validate_parent_bucket(&parent_bucket, &project_id)
                .map_err(|error| anyhow!("{error}"))?;
            let result = database
                .branch(&parent_bucket)
                .await
                .with_context(|| format!("branch from {parent_bucket}"))?;
            if command.json {
                out.row(&ImportReply(result.clone()))?;
            } else {
                let fork_txid = result.get("fork_txid").and_then(Value::as_u64).unwrap_or_default();
                let bytes_parent = result
                    .get("bytes_parent")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let duration_ms = result
                    .get("duration_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                note!(
                    "Branched at fork txid {fork_txid} ({bytes_parent} parent bytes) in {duration_ms}ms"
                );
            }
        }
        Action::MigrationsList => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                note!("No migrations to apply.");
                return out.finish();
            }
            note!("Migrations to be applied:");
            for migration in &pending {
                out.row(&Pending {
                    name: migration.name.clone(),
                })?;
            }
        }
        Action::MigrationsApply => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                note!("No migrations to apply.");
                return out.finish();
            }
            for migration in pending {
                database
                    .apply(&migration, &declaration.migrations_table)
                    .await?;
                // Applying is an action, not data: its progress belongs on
                // stderr so a redirect of the listing stays parseable.
                note!("Applied {}", migration.name);
            }
        }
    }
    out.finish()
}

struct Migration {
    name: String,
    sql: String,
}

/// One entrance into the fleet: a node's address and the identity a request
/// to it must be signed for.
impl Reachable {
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

    async fn import(&self, path: &Path) -> anyhow::Result<Value> {
        self.call(serde_json::json!({ "import": { "path": path.display().to_string() } }))
            .await
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

const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";
const DEFAULT_IMPORT_MAX_BYTES: u64 = 512 * 1024 * 1024;

fn import_max_bytes() -> u64 {
    std::env::var("CELLD_D1_IMPORT_MAX_MB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|megabytes| *megabytes > 0)
        .map(|megabytes| megabytes * 1024 * 1024)
        .unwrap_or(DEFAULT_IMPORT_MAX_BYTES)
}

fn sqlite_magic_at(path: &Path) -> anyhow::Result<bool> {
    let mut file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut header = [0u8; 16];
    let read = file.read(&mut header)?;
    Ok(read >= SQLITE_MAGIC.len() && &header[..SQLITE_MAGIC.len()] == SQLITE_MAGIC)
}

fn reject_sqlite_seed_for_execute(path: &Path) -> anyhow::Result<()> {
    if path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("db"))
    {
        bail!(
            "{} is a SQLite database file; use `celld d1 import` instead of execute --file",
            path.display()
        );
    }
    if sqlite_magic_at(path)? {
        bail!(
            "{} is a SQLite database file; use `celld d1 import` instead of execute --file",
            path.display()
        );
    }
    Ok(())
}

/// Local checks before the owner opens the seed on the same machine (AD-1).
fn validate_import_seed(path: &Path) -> anyhow::Result<(PathBuf, u64)> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve the working directory")?
            .join(path)
    };
    let metadata = std::fs::metadata(&path)
        .with_context(|| format!("read {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{} is not a file", path.display());
    }
    let size = metadata.len();
    let limit = import_max_bytes();
    if size > limit {
        bail!(
            "sqlite seed {} bytes exceeds limit of {} bytes (CELLD_D1_IMPORT_MAX_MB)",
            size,
            limit
        );
    }
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let shm = PathBuf::from(format!("{}-shm", path.display()));
    if wal.exists() {
        bail!(
            "refusing import with WAL sidecar {}; checkpoint the database and remove it first",
            wal.display()
        );
    }
    if shm.exists() {
        bail!(
            "refusing import with SHM sidecar {}; checkpoint the database and remove it first",
            shm.display()
        );
    }
    if !sqlite_magic_at(&path)? {
        bail!(
            "{} is not a SQLite database (missing SQLite format 3 header)",
            path.display()
        );
    }
    let absolute = std::fs::canonicalize(&path)
        .with_context(|| format!("resolve {}", path.display()))?;
    Ok((absolute, size))
}

/// Every node with an unexpired lease and a usable address, as (id, address).
/// The id is not decoration: a peer signature binds the node it is addressed
/// to. Any of them will do, because the route forwards to the owner, so the
/// CLI does not resolve ownership and cannot race a cell that moves while it
/// works. They are all returned rather than only the first, because a lease
/// outlives the node that wrote it and a draining node keeps its lease while
/// it refuses work.
enum Action {
    /// Exactly one of the two, enforced at parse.
    Execute {
        command: Option<String>,
        file: Option<PathBuf>,
    },
    Import {
        file: PathBuf,
    },
    Branch {
        parent_bucket: String,
    },
    MigrationsList,
    MigrationsApply,
}

struct Command {
    action: Action,
    database: String,
    project: Option<PathBuf>,
    fleet: FleetFlags,
    json: bool,
    unsafe_public_advertise: bool,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let mut import = false;
        let mut branch = false;
        let action = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            "execute" => None,
            "import" => {
                import = true;
                None
            }
            "branch" => {
                branch = true;
                None
            }
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
        if matches!(
            arguments.peek().map(|value| value.as_str()),
            Some("--help" | "-h")
        ) {
            return Ok(None);
        }
        let database = arguments
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| anyhow!("celld d1 needs a database name"))?;

        let mut sql = None;
        let mut file = None;
        let mut project = None;
        let mut fleet = FleetFlags::default();
        let mut json = false;
        let mut unsafe_public_advertise = false;
        let mut parent_bucket = None;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--command" => sql = Some(value("--command")?),
                "--file" => file = Some(PathBuf::from(value("--file")?)),
                "--parent-bucket" => parent_bucket = Some(value("--parent-bucket")?),
                "--json" => json = true,
                "--unsafe-public-advertise" => unsafe_public_advertise = true,
                "--help" | "-h" => return Ok(None),
                other => {
                    // The fleet flags are shared, so `--bucket gs://name`
                    // resolves here exactly as it does for every other
                    // command rather than through a second copy of the rule.
                    if fleet.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld d1 --help` for usage")
                    }
                    project = Some(PathBuf::from(other));
                }
            }
        }
        let action = if branch {
            if sql.is_some() || file.is_some() {
                bail!("`celld d1 branch` takes no --command or --file");
            }
            let parent_bucket = parent_bucket
                .ok_or_else(|| anyhow!("celld d1 branch requires --parent-bucket"))?;
            Action::Branch { parent_bucket }
        } else if import {
            if sql.is_some() {
                bail!("`celld d1 import` takes no --command");
            }
            let file = file.ok_or_else(|| anyhow!("celld d1 import requires --file"))?;
            Action::Import { file }
        } else {
            match action {
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
            }
        };
        Ok(Some(Self {
            action,
            database,
            project,
            // Resolution is deferred to `run`, so the missing-bucket message
            // is the one every command shares.
            fleet: fleet.with_environment(),
            json,
            unsafe_public_advertise,
        }))
    }
}

pub fn print_help() {
    let text = format!(
        r#"celld d1 — run SQL and migrations against a deployed D1 database

USAGE:
  celld d1 execute DATABASE (--command SQL | --file PATH) [PROJECT] --bucket NAME
  celld d1 import   DATABASE --file PATH [PROJECT] --bucket NAME
  celld d1 branch   DATABASE --parent-bucket URI [PROJECT] --bucket NAME
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

`import` uploads a binary SQLite file from this machine. The seed must carry
the SQLite format 3 header, must not have `-wal` or `-shm` sidecars, and must
be at most 512 MiB (override with CELLD_D1_IMPORT_MAX_MB). `execute --file`
accepts SQL text only; pass a `.db` file to `import` instead.

Migration files are `NNNN_description.sql` in `migrations/`, applied in the
order of their numeric prefixes as wrangler applies them. `migrations_dir` on
the d1_databases entry moves the directory, and `migrations_table` renames the
bookkeeping table. celld records applied migrations in that table
(`d1_migrations` by default), the same table and columns
`wrangler d1 migrations` uses.

`execute` prints a table, and every message goes to stderr, so its stdout
carries only result rows. Pass --json for one JSON object per row. `import`
with --json prints one JSON object with bytes, duration_ms, and snapshot_txid.

OPTIONS:
  --command SQL       SQL to run; several statements are permitted
  --file PATH         For execute: read SQL from a file. For import: the SQLite seed
  --json              JSON output instead of a table or summary line
{FLEET_HELP}
  --unsafe-public-advertise
                      Permit a node whose advertised address is a public IP
  -h, --help          Show this help"#
    );
    let _ = Output::new(Format::Text).help(&text);
}

#[cfg(test)]
mod import_parse_tests {
    use super::*;

    #[test]
    fn branch_help_does_not_require_database() {
        assert!(Command::parse(vec!["branch".into(), "--help".into()])
            .unwrap()
            .is_none());
        assert!(Command::parse(vec!["branch".into(), "-h".into()])
            .unwrap()
            .is_none());
    }

    #[test]
    fn import_help_does_not_require_database() {
        assert!(Command::parse(vec!["import".into(), "--help".into()])
            .unwrap()
            .is_none());
        assert!(Command::parse(vec!["import".into(), "-h".into()])
            .unwrap()
            .is_none());
    }
}
