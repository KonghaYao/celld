//! Cell-scope predicates, reified sans-IO.
//!
//! A cell scope names one Durable Object: `Class:instance`, or a bare instance
//! that the runtime prefixes with the single configured class. The scope is
//! then used as a path component and as an object-store key — `db_path` joins
//! it under the data directory, and the replication client builds
//! `cells/<scope>/ltx/e<epoch>` from it — so its charset is a SECURITY fence,
//! exactly as `peer::valid_identity`'s is. Admit `/` and a scope carries its
//! own path segments: it escapes the data directory through `..` and escapes
//! the bucket prefix the same way.
//!
//! The gate lives here, and the callers that accept a scope from the network
//! apply it before the scope reaches storage.

/// Is a cell scope well-formed? Non-empty, at most 512 bytes, and only ASCII
/// alphanumerics plus `_ - . : $`.
///
/// The charset is the fence. `/` and `\` are excluded so the scope can never be
/// more than one path component, which is what makes `..` inert: `Class:..` is
/// a literal directory name, while `Class:../..` would traverse. Control bytes
/// and NUL are excluded with everything else outside the set.
///
/// `:` is admitted because it is the class/instance separator and an instance
/// may itself contain one. `$` is admitted because it is a legal JavaScript
/// identifier character, so it is a legal exported class name. Both are inert
/// in a path component.
///
/// The 512-byte bound is generous next to the two shapes that actually occur —
/// a class name and a 64-character hex id, or a class name and a
/// developer-chosen instance name — and it bounds what a single request can
/// make the storage layer allocate.
pub fn valid_cell_scope(scope: &str) -> bool {
    !scope.is_empty()
        && scope.len() <= 512
        && scope
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':' | b'$'))
}
