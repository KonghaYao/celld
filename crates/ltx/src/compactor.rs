//! A streaming port of `superfly/ltx` v0.5.2 `Compactor`.
//!
//! The compactor keeps one decompressed page per input. It does not build a
//! database-sized page map. Inputs must be ordered by transaction range.

use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::ltx::{Header, PageHeader, Trailer, VERSION};
use crate::TXID;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU32, Ordering};

/// The current progress of a compaction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactorStatus {
    /// The last page number processed by the merge.
    pub page_number: u32,
    /// The final input's commit page count.
    pub total: u32,
}

impl CompactorStatus {
    pub fn is_zero(self) -> bool {
        self.page_number == 0 && self.total == 0
    }

    pub fn fraction(self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            f64::from(self.page_number) / f64::from(self.total)
        }
    }
}

/// Merges ordered LTX inputs into one byte-compatible LTX output.
pub struct Compactor<W, R> {
    encoder: Encoder<W>,
    inputs: Vec<CompactorInput<R>>,
    page_number: AtomicU32,
    total: AtomicU32,

    /// Flags to set on the compacted LTX header.
    pub header_flags: u32,
    /// Allows gaps in the ordered input transaction ranges.
    pub allow_non_contiguous_txids: bool,
}

impl<W: Write, R: Read> Compactor<W, R> {
    pub fn new(writer: W, readers: Vec<R>) -> Self {
        Self {
            encoder: Encoder::new_block(writer),
            inputs: readers
                .into_iter()
                .map(|reader| CompactorInput {
                    decoder: Decoder::new(reader),
                    page: None,
                    data: Vec::new(),
                })
                .collect(),
            page_number: AtomicU32::new(0),
            total: AtomicU32::new(0),
            header_flags: 0,
            allow_non_contiguous_txids: false,
        }
    }

    pub fn header(&self) -> Header {
        self.encoder.header
    }

    pub fn trailer(&self) -> Trailer {
        self.encoder.trailer
    }

    pub fn status(&self) -> CompactorStatus {
        CompactorStatus {
            page_number: self.page_number.load(Ordering::Relaxed),
            total: self.total.load(Ordering::Relaxed),
        }
    }

    pub fn writer(&self) -> &W {
        &self.encoder.writer
    }

    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.encoder.writer
    }

    pub fn into_writer(self) -> W {
        self.encoder.writer
    }

    pub fn compact(&mut self) -> Result<()> {
        if self.inputs.is_empty() {
            return Err(Error::LTXCorrupted);
        }

        for input in &mut self.inputs {
            input.decoder.decode_header()?;
        }

        for index in 1..self.inputs.len() {
            let previous = self.inputs[index - 1].decoder.header;
            let current = self.inputs[index].decoder.header;
            if previous.page_size != current.page_size {
                return Err(Error::LTXCorrupted);
            }
            if !self.allow_non_contiguous_txids
                && !is_contiguous(previous.max_txid, current.min_txid, current.max_txid)
            {
                return Err(Error::LTXCorrupted);
            }
        }

        let first = self.inputs[0].decoder.header;
        let last = self.inputs[self.inputs.len() - 1].decoder.header;
        self.encoder.encode_header(Header {
            version: VERSION,
            flags: self.header_flags,
            page_size: first.page_size,
            commit: last.commit,
            min_txid: first.min_txid,
            max_txid: last.max_txid,
            timestamp: last.timestamp,
            pre_apply_checksum: first.pre_apply_checksum,
            ..Header::default()
        })?;
        self.total.store(last.commit, Ordering::Relaxed);

        for input in &mut self.inputs {
            input.data.resize(first.page_size as usize, 0);
        }

        loop {
            let Some(page_number) = self.fill_page_buffers()? else {
                break;
            };
            self.write_page_buffer(page_number)?;
            self.page_number.store(page_number, Ordering::Relaxed);
        }

        for input in &mut self.inputs {
            input.decoder.close()?;
        }

        let post_apply_checksum = self.inputs[self.inputs.len() - 1]
            .decoder
            .trailer
            .post_apply_checksum;
        self.encoder.close(post_apply_checksum)
    }

    fn fill_page_buffers(&mut self) -> Result<Option<u32>> {
        let mut minimum = None;
        for input in &mut self.inputs {
            if input.page.is_none() {
                input.page = input.decoder.decode_page(&mut input.data)?;
            }
            if let Some(page) = input.page {
                minimum = Some(minimum.map_or(page.pgno, |value: u32| value.min(page.pgno)));
            }
        }
        Ok(minimum)
    }

    fn write_page_buffer(&mut self, page_number: u32) -> Result<()> {
        let commit = self.encoder.header.commit;
        let mut written = false;
        for input in self.inputs.iter_mut().rev() {
            let Some(page) = input.page else {
                continue;
            };
            if page.pgno != page_number {
                continue;
            }
            input.page = None;
            if written || page_number > commit {
                continue;
            }
            written = true;
            self.encoder.encode_page(page, &input.data)?;
        }
        Ok(())
    }
}

fn is_contiguous(previous_max: TXID, min: TXID, max: TXID) -> bool {
    min.0 <= previous_max.0.wrapping_add(1) && max > previous_max
}

struct CompactorInput<R> {
    decoder: Decoder<R>,
    page: Option<PageHeader>,
    data: Vec<u8>,
}

#[cfg(test)]
// Unit tests inspect materialized file-format fixtures outside production.
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::ltx::{
        checksum_page, decode_file, decode_file_pages, encode_file_v0_5_2, HEADER_FLAG_NO_CHECKSUM,
    };
    use crate::CHECKSUM_FLAG;
    use std::io::Cursor;

    fn header(
        page_size: u32,
        commit: u32,
        min_txid: u64,
        max_txid: u64,
        timestamp: i64,
        pre_apply_checksum: u64,
    ) -> Header {
        Header {
            version: VERSION,
            page_size,
            commit,
            min_txid: TXID(min_txid),
            max_txid: TXID(max_txid),
            timestamp,
            pre_apply_checksum,
            ..Header::default()
        }
    }

    fn input(header: Header, pages: &[(u32, u8)], post_apply_checksum: u64) -> Vec<u8> {
        let pages = pages
            .iter()
            .map(|&(page_number, byte)| (page_number, vec![byte; header.page_size as usize]))
            .collect::<Vec<_>>();
        encode_file_v0_5_2(&header, &pages, post_apply_checksum).expect("encode input")
    }

    fn snapshot_checksum(page_size: usize, pages: &[(u32, u8)]) -> u64 {
        pages.iter().fold(CHECKSUM_FLAG, |rolling, &(pgno, byte)| {
            CHECKSUM_FLAG | (rolling ^ checksum_page(pgno, &vec![byte; page_size]))
        })
    }

    fn compact(inputs: Vec<Vec<u8>>) -> Result<Vec<u8>> {
        let readers = inputs.into_iter().map(Cursor::new).collect();
        let mut compactor = Compactor::new(Vec::new(), readers);
        compactor.compact()?;
        Ok(compactor.into_writer())
    }

    #[test]
    fn single_file_is_an_exact_copy() {
        let source = input(
            header(512, 1, 1, 1, 1000, 0),
            &[(1, b'1')],
            0xeb1a999231044ddd,
        );
        assert_eq!(compact(vec![source.clone()]).unwrap(), source);
    }

    #[test]
    fn newest_page_wins_and_output_header_spans_inputs() {
        let checksum1 = 0x8a249272ad9f7dea;
        let first = input(
            header(1024, 3, 1, 1, 1000, 0),
            &[(1, 0x81), (2, 0x82), (3, 0x83)],
            checksum1,
        );
        let second = input(
            header(1024, 3, 2, 2, 2000, checksum1),
            &[(1, 0x91), (3, 0x93)],
            checksum1,
        );

        let output = compact(vec![first, second]).unwrap();
        let expected = hex::decode(concat!(
            "4c54583100000000000004000000000300000000000000010000000000000002",
            "00000000000007d0000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "000000000000000100010000001a1f910100ffffffdc000200000200b0919191",
            "91919191919191910000000200010000001a1f820100ffffffdc000200000200",
            "b082828282828282828282820000000300010000001a1f930100ffffffdc0002",
            "00000200b093939393939393939393930000000000000164240288012403ac01",
            "2400000000000000000c8a249272ad9f7dea91206f60138b3298",
        ))
        .expect("fixture hex");
        assert_eq!(output, expected, "complete output must match Go v0.5.2");
        let decoded = decode_file(&output).unwrap();
        assert_eq!(decoded.header.min_txid, TXID(1));
        assert_eq!(decoded.header.max_txid, TXID(2));
        assert_eq!(decoded.header.timestamp, 2000);
        assert_eq!(
            decode_file_pages(&output).unwrap(),
            vec![
                (1, vec![0x91; 1024]),
                (2, vec![0x82; 1024]),
                (3, vec![0x93; 1024]),
            ]
        );
    }

    #[test]
    fn pages_beyond_the_final_commit_are_dropped() {
        let first = input(
            header(1024, 3, 2, 3, 1000, CHECKSUM_FLAG | 2),
            &[(3, 0x83)],
            CHECKSUM_FLAG | 3,
        );
        let second = input(
            header(1024, 2, 4, 5, 2000, CHECKSUM_FLAG | 4),
            &[(1, 0x91)],
            CHECKSUM_FLAG | 5,
        );

        let output = compact(vec![first, second]).unwrap();
        assert_eq!(decode_file(&output).unwrap().header.commit, 2);
        assert_eq!(
            decode_file_pages(&output).unwrap(),
            vec![(1, vec![0x91; 1024])]
        );
    }

    #[test]
    fn rejects_missing_mismatched_and_non_contiguous_inputs() {
        let mut empty = Compactor::<Vec<u8>, Cursor<Vec<u8>>>::new(Vec::new(), Vec::new());
        assert!(empty.compact().is_err());

        let mut missing_header = Compactor::new(Vec::new(), vec![Cursor::new(Vec::new())]);
        assert!(missing_header.compact().is_err());

        let a = input(
            header(512, 1, 1, 1, 1000, 0),
            &[(1, 0x81)],
            CHECKSUM_FLAG | 1,
        );
        let b = input(
            header(1024, 1, 2, 2, 1000, CHECKSUM_FLAG | 1),
            &[(1, 0x91)],
            CHECKSUM_FLAG | 2,
        );
        assert!(compact(vec![a, b]).is_err());

        let a = input(
            header(1024, 1, 1, 2, 1000, 0),
            &[(1, 0x81)],
            snapshot_checksum(1024, &[(1, 0x81)]),
        );
        let b = input(
            header(1024, 1, 4, 4, 1000, CHECKSUM_FLAG | 2),
            &[(1, 0x91)],
            CHECKSUM_FLAG | 1,
        );
        let readers = vec![Cursor::new(a), Cursor::new(b)];
        let mut strict = Compactor::new(Vec::new(), readers);
        assert!(strict.compact().is_err());
    }

    #[test]
    fn rejects_an_input_with_an_invalid_trailer() {
        let mut source = input(
            header(512, 1, 1, 1, 1000, 0),
            &[(1, b'1')],
            0xeb1a999231044ddd,
        );
        source.pop();

        let mut compactor = Compactor::new(Vec::new(), vec![Cursor::new(source)]);
        assert!(compactor.compact().is_err());
    }

    #[test]
    fn can_allow_non_contiguous_inputs() {
        let a = input(
            header(1024, 1, 1, 2, 1000, 0),
            &[(1, 0x81)],
            snapshot_checksum(1024, &[(1, 0x81)]),
        );
        let b = input(
            header(1024, 1, 4, 4, 1000, CHECKSUM_FLAG | 2),
            &[(1, 0x91)],
            CHECKSUM_FLAG | 1,
        );
        let readers = vec![Cursor::new(a), Cursor::new(b)];
        let mut compactor = Compactor::new(Vec::new(), readers);
        compactor.allow_non_contiguous_txids = true;
        compactor.compact().unwrap();
    }

    #[test]
    fn reports_progress() {
        let source = input(
            header(1024, 3, 1, 1, 1000, 0),
            &[(1, 0x81), (2, 0x82), (3, 0x83)],
            0x8a249272ad9f7dea,
        );
        let mut compactor = Compactor::new(Vec::new(), vec![Cursor::new(source)]);
        assert!(compactor.status().is_zero());
        assert_eq!(compactor.status().fraction(), 0.0);
        compactor.compact().unwrap();
        assert_eq!(
            compactor.status(),
            CompactorStatus {
                page_number: 3,
                total: 3,
            }
        );
        assert_eq!(compactor.status().fraction(), 1.0);
    }

    #[test]
    fn compacts_legacy_frame_inputs() {
        let fixture_dir = format!(
            "{}/tests/fixtures/golden/replica/ltx/0",
            env!("CARGO_MANIFEST_DIR")
        );
        let inputs = (1..=6)
            .map(|txid| {
                std::fs::read(format!("{fixture_dir}/{txid:016x}-{txid:016x}.ltx"))
                    .expect("read legacy fixture")
            })
            .collect::<Vec<_>>();

        // Litestream v0.5.11 wrote the pre-v0.5.2 frame representation.
        let first_page_flags = u16::from_be_bytes([inputs[0][104], inputs[0][105]]);
        assert_eq!(first_page_flags, 0);

        let readers = inputs.into_iter().map(Cursor::new).collect();
        let mut compactor = Compactor::new(Vec::new(), readers);
        compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
        compactor.compact().expect("compact legacy inputs");
        let output = compactor.into_writer();
        let decoded = decode_file(&output).expect("verify v0.5.2 output");
        assert_eq!(decoded.header.min_txid, TXID(1));
        assert_eq!(decoded.header.max_txid, TXID(6));
        assert!(!decoded.pgnos.is_empty());
    }
}
