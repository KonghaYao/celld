// Copyright 2026 Deno Land Inc. Apache-2.0 license.

#![allow(clippy::disallowed_methods)]

//! `celld r2` — minimal operator surface for R2 binding branch.

use anyhow::{anyhow, bail, Context};
use std::borrow::Cow;

use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::note;

struct BranchReply {
    parent_bucket: String,
    duration_ms: u64,
}

impl Record for BranchReply {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "parent_bucket": self.parent_bucket,
            "duration_ms": self.duration_ms,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "parent_bucket: {}\nduration_ms: {}",
            self.parent_bucket, self.duration_ms
        ))
    }
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    match command.action {
        Action::Branch {
            bucket_name,
            parent_bucket,
            fleet,
            json,
        } => {
            let result = crate::r2_branch::branch_r2_binding(&bucket_name, &parent_bucket, &fleet)
                .await
                .with_context(|| format!("branch R2 binding {bucket_name:?}"))?;
            let mut out = Output::new(if json { Format::Json } else { Format::Text });
            if json {
                out.row(&BranchReply {
                    parent_bucket: result.parent_bucket.clone(),
                    duration_ms: result.duration_ms,
                })?;
            } else {
                note!(
                    "Branched R2 binding {bucket_name:?} from {} in {}ms",
                    result.parent_bucket,
                    result.duration_ms
                );
            }
            out.finish()?;
        }
    }
    Ok(())
}

#[derive(Debug)]
enum Action {
    Branch {
        bucket_name: String,
        parent_bucket: String,
        fleet: FleetFlags,
        json: bool,
    },
}

#[derive(Debug)]
struct Command {
    action: Action,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter();
        let Some(verb) = arguments.next() else {
            return Ok(None);
        };
        match verb.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            "branch" => {}
            other => bail!("unknown `celld r2` subcommand: {other}"),
        }
        let mut bucket_name = None;
        let mut parent_bucket = None;
        let mut fleet = FleetFlags::default();
        let mut json = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--name" => bucket_name = Some(value("--name")?),
                "--parent-bucket" => parent_bucket = Some(value("--parent-bucket")?),
                "--json" => json = true,
                "--help" | "-h" => return Ok(None),
                other => {
                    if fleet.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld r2 --help` for usage")
                    }
                    bail!("`celld r2 branch` takes no positional argument: {other:?}")
                }
            }
        }
        let bucket_name = bucket_name.ok_or_else(|| anyhow!("celld r2 branch requires --name"))?;
        let parent_bucket = parent_bucket
            .ok_or_else(|| anyhow!("celld r2 branch requires --parent-bucket"))?;
        Ok(Some(Self {
            action: Action::Branch {
                bucket_name,
                parent_bucket,
                fleet: fleet.with_environment(),
                json,
            },
        }))
    }
}

pub fn print_help() {
    let text = format!(
        "celld r2 — R2 binding branch overlay

USAGE
  celld r2 branch --name BUCKET_NAME --parent-bucket URI [--bucket CHILD] [fleet options]

Writes `r2/<name>/base.json` on the child version bucket pointing at the
parent version. Worker-visible overlay (GET miss -> parent, PUT -> child,
DELETE -> tombstone under `r2/<name>/.tombstones/<key>`) is installed on
the next node start after branch.

FLEET OPTIONS
{FLEET_HELP}
  --json                Print one JSON object"
    );
    let _ = Output::new(Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_parse_requires_name_and_parent() {
        let error = Command::parse(vec![
            "branch".into(),
            "--name".into(),
            "assets".into(),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("--parent-bucket"), "{error}");
    }

    #[test]
    fn branch_help_does_not_require_name() {
        assert!(Command::parse(vec!["branch".into(), "--help".into()])
            .unwrap()
            .is_none());
    }
}
