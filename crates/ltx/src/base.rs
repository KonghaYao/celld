//! `base.json` — cross-version LTX base pointer (D1-BRANCH).

use crate::error::{Error, Result};
use crate::ltx;
use crate::Checksum;
use crate::TXID;
use serde::{Deserialize, Serialize};

pub const BASE_JSON: &str = "base.json";

/// Parsed `cells/<scope>/ltx/e<epoch>/base.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasePointer {
    pub parent_bucket: String,
    pub parent_cell: String,
    #[serde(default)]
    pub parent_epoch: u64,
    pub fork_txid: u64,
    pub fork_checksum: String,
}

impl BasePointer {
    pub fn parse_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|error| {
            Error::Other(format!("parse base.json: {error}").into())
        })
    }

    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|error| {
            Error::Other(format!("serialize base.json: {error}").into())
        })
    }

    pub fn fork_txid(&self) -> TXID {
        TXID(self.fork_txid)
    }

    pub fn fork_checksum_value(&self) -> Result<Checksum> {
        parse_checksum_hex(&self.fork_checksum)
    }
}

pub fn parse_checksum_hex(text: &str) -> Result<Checksum> {
    let text = text.strip_prefix("0x").unwrap_or(text);
    if text.len() != 16 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Other(
            format!("invalid fork_checksum hex: {text:?}").into(),
        ));
    }
    u64::from_str_radix(text, 16)
        .map_err(|_| Error::Other(format!("invalid fork_checksum hex: {text:?}").into()))
}

pub fn checksum_hex(value: Checksum) -> String {
    format!("{value:016x}")
}

/// Read the post-apply checksum from a terminal parent LTX object.
pub fn post_apply_checksum_from_ltx(bytes: &[u8]) -> Result<Checksum> {
    if bytes.len() < ltx::TRAILER_SIZE {
        return Err(Error::LTXCorrupted);
    }
    let trailer = ltx::Trailer::parse(&bytes[bytes.len() - ltx::TRAILER_SIZE..])?;
    Ok(trailer.post_apply_checksum)
}

pub fn base_json_key(epoch_prefix: &str) -> String {
    format!("{epoch_prefix}/{BASE_JSON}")
}
