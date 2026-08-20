//! fuzz_bundle — robustness targets for the bundle envelope
//! ([`celld_ltx::bundle`]), in the style of `fuzz_parsers.rs`.
//!
//! ## Contract under test
//! `decode_rows` and `slice` are an untrusted-input boundary: every byte
//! they see comes from object storage. The invariants:
//!
//! > For ANY input slice — empty, truncated, random, or an adversarially
//! > mutated copy of a real bundle — `decode_rows` returns `Err` or a
//! > valid parse, and `slice` on any returned row returns `Err` or an
//! > in-bounds slice. They MUST NOT panic or read out of bounds.
//!
//! > A TRUNCATED bundle is refused deterministically: the trailer (footer
//! > length + magic) sits at the object's end, so any proper prefix of a
//! > bundle whose payloads do not embed a counterfeit trailer fails to
//! > decode. Torn objects cannot exist at rest (an object store PUT is
//! > atomic), so this discipline is defense in depth for the read path,
//! > not a crash-recovery requirement.
//!
//! A payload CAN embed counterfeit trailer bytes — the envelope adds no
//! checksum, by the envelope rule (never a new format). For such inputs
//! the boundary contract still holds: a coincidental parse yields rows
//! whose slices are bounds-checked, and the corrupt L0 bytes they carry
//! are caught downstream by the LTX checksums the per-cell path already
//! validates.

use celld_ltx::bundle::{decode_rows, encode, slice, BundleEntry};

const RANDOM_ITERS: usize = 20_000;
const MUTATION_ITERS: usize = 4_000;
const SEED: u64 = 0x424E_444C_5F52_5931; // "BNDL_RY1"

struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for byte in buf.iter_mut() {
            *byte = self.next_u64() as u8;
        }
    }
}

fn assert_no_panic(label: &str, input: &[u8]) {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(|| {
        if let Ok(rows) = decode_rows(input) {
            for row in &rows {
                // Err or an in-bounds slice; a panic here is the failure.
                let _ = slice(input, row);
            }
        }
    });
    std::panic::set_hook(hook);
    assert!(
        outcome.is_ok(),
        "{label}: panic on {} bytes: {:02x?}",
        input.len(),
        &input[..input.len().min(64)]
    );
}

/// A representative bundle whose payloads carry no counterfeit trailer.
fn golden() -> Vec<u8> {
    let entries = vec![
        BundleEntry {
            cell: "cell-alpha".into(),
            cell_epoch: 2,
            txid: 41,
            bytes: vec![0x11; 96],
        },
        BundleEntry {
            cell: "cell-beta".into(),
            cell_epoch: 7,
            txid: 1,
            bytes: Vec::new(),
        },
        BundleEntry {
            cell: "z".into(),
            cell_epoch: 1,
            txid: u64::MAX,
            bytes: vec![0x22; 7],
        },
    ];
    encode(&entries).unwrap()
}

#[test]
fn every_truncation_of_a_bundle_is_refused() {
    let bundle = golden();
    for cut in 0..bundle.len() {
        assert!(
            decode_rows(&bundle[..cut]).is_err(),
            "a {cut}-byte prefix of a {}-byte bundle must not decode",
            bundle.len()
        );
    }
}

#[test]
fn random_bytes_never_panic() {
    let mut rng = SplitMix64(SEED);
    for _ in 0..RANDOM_ITERS {
        let mut buf = vec![0_u8; rng.below(512)];
        rng.fill(&mut buf);
        assert_no_panic("random", &buf);
    }
}

#[test]
fn mutated_golden_never_panics() {
    let bundle = golden();
    let mut rng = SplitMix64(SEED ^ 0xACE1);
    for _ in 0..MUTATION_ITERS {
        let mut copy = bundle.clone();
        match rng.below(5) {
            // Bit flip.
            0 => {
                let index = rng.below(copy.len());
                copy[index] ^= 1 << rng.below(8);
            }
            // Byte set.
            1 => {
                let index = rng.below(copy.len());
                copy[index] = rng.next_u64() as u8;
            }
            // Truncate.
            2 => copy.truncate(rng.below(copy.len())),
            // Splice two halves at mismatched offsets.
            3 => {
                let a = rng.below(copy.len());
                let b = rng.below(copy.len());
                let tail: Vec<u8> = copy[b..].to_vec();
                copy.truncate(a);
                copy.extend_from_slice(&tail);
            }
            // Zero a run.
            _ => {
                let start = rng.below(copy.len());
                let len = rng.below(copy.len() - start + 1);
                for byte in &mut copy[start..start + len] {
                    *byte = 0;
                }
            }
        }
        assert_no_panic("mutated", &copy);
    }
}

/// A payload that embeds a counterfeit trailer must still honor the
/// boundary contract when the object is cut exactly at the counterfeit.
#[test]
fn an_embedded_counterfeit_trailer_stays_in_bounds() {
    // The payload's final 8 bytes claim "footer of length 0" + magic, so
    // the prefix ending at the payload IS a decodable (empty) bundle;
    // deeper cuts and full reads must parse or refuse without panicking.
    let mut payload = vec![0x33_u8; 40];
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(b"CLB1");
    let bundle = encode(&[BundleEntry {
        cell: "trap".into(),
        cell_epoch: 1,
        txid: 1,
        bytes: payload,
    }])
    .unwrap();
    for cut in 0..=bundle.len() {
        assert_no_panic("counterfeit", &bundle[..cut]);
    }
    // The complete object still decodes to the true row, byte-identical.
    let rows = decode_rows(&bundle).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(slice(&bundle, &rows[0]).unwrap().len(), 48);
}

/// Round-trip property over randomized entries: whatever goes in comes
/// back byte-identical, including empty payloads and empty bundles.
#[test]
fn random_round_trips_are_byte_identical() {
    let mut rng = SplitMix64(SEED ^ 0x5EED);
    for _ in 0..200 {
        let count = rng.below(6);
        let entries: Vec<BundleEntry> = (0..count)
            .map(|index| {
                let mut bytes = vec![0_u8; rng.below(300)];
                rng.fill(&mut bytes);
                BundleEntry {
                    cell: format!("cell-{index}-{}", rng.below(1000)),
                    cell_epoch: rng.next_u64(),
                    txid: rng.next_u64(),
                    bytes,
                }
            })
            .collect();
        let bundle = encode(&entries).unwrap();
        let rows = decode_rows(&bundle).unwrap();
        assert_eq!(rows.len(), entries.len());
        for (row, entry) in rows.iter().zip(&entries) {
            assert_eq!(slice(&bundle, row).unwrap(), entry.bytes.as_slice());
            assert_eq!(
                (row.cell.as_str(), row.cell_epoch, row.txid),
                (entry.cell.as_str(), entry.cell_epoch, entry.txid)
            );
        }
    }
}
