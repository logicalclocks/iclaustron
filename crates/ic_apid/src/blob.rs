// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! Blob columns, which hold MySQL's BLOB, TEXT, JSON and GEOMETRY: the
//! head a main row keeps, and where each byte of a value lies. New: the
//! C had no blobs.
//!
//! A blob column's value is kept in two places. The main row holds a
//! head and the value's first bytes, its inline bytes; the rest lies in
//! rows of the column's parts table, a part of a fixed size in each.
//! The data node knows nothing joining them: the client writes the head
//! and the parts in step, in the rounds chapter 06's phase 12 gives each
//! operation. Only the second version of the head is read and written.
//!
//! - **The head** is 16 bytes, little-endian: a 16-bit size, which
//!   counts the bytes after it (14 and the inline bytes) and is the
//!   column's 2-byte length; 16 bits reserved; a 32-bit part key id; the
//!   value's whole length in 64 bits. The inline bytes follow it, as
//!   many as the length or the inline size allows, the smaller. The
//!   reserved field and the part key id are written 0 and not read.
//! - **The sizes** are kept in the column's description: the inline
//!   size in its precision, the part size in its scale, the stripe in
//!   its length. A part size of 0 means no parts table: the value must
//!   fit inline, or it is refused with 4266.
//! - **Part k** holds the bytes from `inline + k * part`, a whole part
//!   of them but the last. A value of length `L` past the inline size
//!   has `(L - inline) / part` parts, rounded up.
//! - **A part's bytes** are a variable-sized value behind a 2-byte
//!   length for a parts table in memory, or a fixed-size one, padded to
//!   the part size with 0x00 (binary) or 0x20 (text), for one on disk.
//!
//! A part that is missing, or a head or part whose sizes disagree with
//! the value's, is a damaged value, 4267.
//!
//! This module is pure: it works out sizes and encodes and decodes,
//! and is tested on its own. Verify: chapter 05 §6.10.

use ic_port::IcError;
use ic_signals::table_props;
use ic_signals::table_props::ColumnProps;

/// A value longer than its blob column holds without a parts table.
pub const IC_NDB_ERROR_BLOB_TOO_LONG: i32 = 4266;
/// A blob value whose head and parts disagree, or a part missing.
pub const IC_NDB_ERROR_BLOB_DAMAGED: i32 = 4267;

/// Bytes of the head, before the inline bytes.
pub const IC_BLOB_HEAD_BYTES: usize = 16;
/// What the head's size field counts besides the inline bytes: the
/// head less the field itself.
const IC_BLOB_HEAD_REST: usize = 14;
/// Bytes of a part's length in a parts table kept in memory.
const IC_PART_LENGTH_BYTES: usize = 2;
/// What pads a part stored on disk: binary, and text.
const IC_PART_PAD_BINARY: u8 = 0x00;
const IC_PART_PAD_TEXT: u8 = 0x20;

/// A blob column's sizes, as its description gives them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobSizes {
  /// Bytes kept in the main row after the head.
  pub inline: u32,
  /// Bytes in each part; 0 for a column with no parts table.
  pub part: u32,
  /// Parts in each stripe; 0 keeps every part in its row's partition.
  pub stripe: u32,
}

impl BlobSizes {
  /// The sizes of a blob column of the second version; `None` for any
  /// other column, a blob of the first version among them.
  pub fn of(column: &ColumnProps) -> Option<BlobSizes> {
    if !column.is_blob() {
      return None;
    }
    if column.array_kind != table_props::IC_ARRAY_KIND_LONG_VARSIZE {
      return None;
    }
    Some(BlobSizes {
      inline: column.precision,
      part: column.scale,
      stripe: column.length,
    })
  }

  /// True when the column has a parts table.
  pub fn has_parts(self) -> bool {
    self.part > 0
  }

  /// True when a value of this length can be stored.
  pub fn holds(self, length: u64) -> bool {
    self.has_parts() || length <= self.inline as u64
  }

  /// How many of a value's bytes are kept inline.
  pub fn inline_len(self, length: u64) -> usize {
    if length < self.inline as u64 {
      length as usize
    } else {
      self.inline as usize
    }
  }

  /// How many parts a value of this length has.
  pub fn part_count(self, length: u64) -> u64 {
    if !self.has_parts() || length <= self.inline as u64 {
      return 0;
    }
    (length - self.inline as u64).div_ceil(self.part as u64)
  }

  /// The part a byte past the inline ones lies in. Only for a column
  /// with parts and a position at or past the inline size.
  pub fn part_of(self, position: u64) -> u64 {
    (position - self.inline as u64) / self.part as u64
  }

  /// Where part `k` begins in the value.
  pub fn part_offset(self, k: u64) -> u64 {
    self.inline as u64 + k * self.part as u64
  }

  /// How many of the value's bytes part `k` holds: a whole part, but
  /// for the last. Only for a part the value has.
  pub fn part_len(self, length: u64, k: u64) -> usize {
    let left = length - self.part_offset(k);
    if left < self.part as u64 {
      left as usize
    } else {
      self.part as usize
    }
  }
}

/// The main row's value of a blob column, the head and the inline
/// bytes, into `out`, which is emptied first. Refused with 4266 when
/// the column has no parts table and the value does not fit inline.
pub fn encode_main_value(
  sizes: BlobSizes,
  value: &[u8],
  out: &mut Vec<u8>,
) -> Result<(), IcError> {
  let length = value.len() as u64;
  if !sizes.holds(length) {
    return Err(IcError::new(IC_NDB_ERROR_BLOB_TOO_LONG));
  }
  let inline_len = sizes.inline_len(length);
  let size = match u16::try_from(IC_BLOB_HEAD_REST + inline_len) {
    Ok(size) => size,
    Err(_) => return Err(IcError::new(IC_NDB_ERROR_BLOB_TOO_LONG)),
  };
  out.clear();
  out.extend_from_slice(&size.to_le_bytes());
  // The reserved field and the part key id, both 0.
  out.extend_from_slice(&[0u8; 6]);
  out.extend_from_slice(&length.to_le_bytes());
  out.extend_from_slice(&value[..inline_len]);
  Ok(())
}

/// A main row's value of a blob column, as read: the value's whole
/// length and its inline bytes. Damaged (4267) when the head's sizes
/// disagree with each other or with the column's.
pub fn decode_main_value(
  sizes: BlobSizes,
  value: &[u8],
) -> Result<(u64, &[u8]), IcError> {
  if value.len() < IC_BLOB_HEAD_BYTES {
    return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
  }
  let size = u16::from_le_bytes([value[0], value[1]]) as usize;
  let mut length_bytes = [0u8; 8];
  length_bytes.copy_from_slice(&value[8..IC_BLOB_HEAD_BYTES]);
  let length = u64::from_le_bytes(length_bytes);
  let inline = &value[IC_BLOB_HEAD_BYTES..];
  if size != value.len() - 2 {
    return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
  }
  if inline.len() != sizes.inline_len(length) || !sizes.holds(length) {
    return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
  }
  Ok((length, inline))
}

/// How a parts table keeps a part's bytes, from its data column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartData {
  /// Variable-sized, behind a 2-byte length: a parts table in memory.
  Var,
  /// Fixed at the part size, padded with 0x00: binary, on disk.
  FixedBinary,
  /// Fixed at the part size, padded with spaces: text, on disk.
  FixedText,
}

impl PartData {
  /// How the parts table's data column keeps a part; `None` for a
  /// column of another type.
  pub fn of(column: &ColumnProps) -> Option<PartData> {
    match column.type_code {
      table_props::IC_TYPE_LONGVARBINARY => Some(PartData::Var),
      table_props::IC_TYPE_LONGVARCHAR => Some(PartData::Var),
      table_props::IC_TYPE_BINARY => Some(PartData::FixedBinary),
      table_props::IC_TYPE_CHAR => Some(PartData::FixedText),
      _ => None,
    }
  }

  /// What pads a fixed-size part.
  fn pad(self) -> u8 {
    match self {
      PartData::FixedText => IC_PART_PAD_TEXT,
      _ => IC_PART_PAD_BINARY,
    }
  }
}

/// A part's value for its data column, into `out`, which is emptied
/// first: its bytes behind their length, or padded to the part size.
pub fn encode_part(
  data: PartData,
  part_size: u32,
  bytes: &[u8],
  out: &mut Vec<u8>,
) {
  out.clear();
  match data {
    PartData::Var => {
      out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
      out.extend_from_slice(bytes);
    }
    PartData::FixedBinary | PartData::FixedText => {
      out.extend_from_slice(bytes);
      out.resize(part_size as usize, data.pad());
    }
  }
}

/// A part's bytes from its data column's value, as read, when the value
/// says the part holds `expected` of them. Damaged (4267) when it holds
/// another number.
pub fn decode_part(
  data: PartData,
  expected: usize,
  value: &[u8],
) -> Result<&[u8], IcError> {
  match data {
    PartData::Var => {
      if value.len() < IC_PART_LENGTH_BYTES {
        return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
      }
      let len = u16::from_le_bytes([value[0], value[1]]) as usize;
      let bytes = &value[IC_PART_LENGTH_BYTES..];
      if len != bytes.len() || len != expected {
        return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
      }
      Ok(bytes)
    }
    PartData::FixedBinary | PartData::FixedText => {
      if value.len() < expected {
        return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
      }
      Ok(&value[..expected])
    }
  }
}

/// A part's key: the main row's key words, then the part number, as the
/// parts table's key is the main table's key columns copied and the
/// part number after them (for a stripe of 0). Into `out`, which is
/// emptied first.
pub fn part_key(main_key: &[u32], part: u32, out: &mut Vec<u32>) {
  out.clear();
  out.extend_from_slice(main_key);
  out.push(part);
}

#[cfg(test)]
mod tests {
  use super::*;

  /// MySQL's TEXT: 256 inline bytes, parts of 2 000.
  const TEXT: BlobSizes = BlobSizes {
    inline: 256,
    part: 2000,
    stripe: 0,
  };
  /// MySQL's TINYTEXT: 256 inline bytes and no parts table.
  const TINY: BlobSizes = BlobSizes {
    inline: 256,
    part: 0,
    stripe: 0,
  };

  fn value_of(length: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    for i in 0..length {
      value.push((i % 251) as u8);
    }
    value
  }

  #[test]
  fn blob_parts_at_every_boundary() {
    // (length, parts, length of the last part)
    let cases: [(u64, u64, usize); 9] = [
      (0, 0, 0),
      (1, 0, 0),
      (255, 0, 0),
      (256, 0, 0),
      (257, 1, 1),
      (2256, 1, 2000),
      (2257, 2, 1),
      (5000, 3, 744),
      (1 << 20, 525, 1_048_576 - 256 - 524 * 2000),
    ];
    for (length, parts, last) in cases {
      assert_eq!(TEXT.part_count(length), parts, "length {length}");
      if parts > 0 {
        assert_eq!(TEXT.part_len(length, parts - 1), last);
        let first = if parts == 1 { last } else { 2000 };
        assert_eq!(TEXT.part_len(length, 0), first);
        let end = TEXT.part_offset(parts - 1) + last as u64;
        assert_eq!(end, length);
      }
    }
    assert_eq!(TEXT.part_of(256), 0);
    assert_eq!(TEXT.part_of(2255), 0);
    assert_eq!(TEXT.part_of(2256), 1);
    assert_eq!(TINY.part_count(10_000), 0);
    assert_eq!(TEXT.inline_len(100), 100);
    assert_eq!(TEXT.inline_len(5000), 256);
  }

  #[test]
  fn blob_head_round_trip() {
    let mut out = Vec::new();
    for length in [0usize, 1, 255, 256, 257, 2256, 2257, 5000] {
      let value = value_of(length);
      encode_main_value(TEXT, &value, &mut out).expect("encodes");
      let inline_len = length.min(256);
      assert_eq!(out.len(), IC_BLOB_HEAD_BYTES + inline_len);
      // The size field is the column's 2-byte length.
      let size = u16::from_le_bytes([out[0], out[1]]) as usize;
      assert_eq!(size, out.len() - 2);
      assert_eq!(&out[2..8], &[0u8; 6]);
      let (got_length, inline) =
        decode_main_value(TEXT, &out).expect("decodes");
      assert_eq!(got_length, length as u64);
      assert_eq!(inline, &value[..inline_len]);
    }
  }

  #[test]
  fn blob_head_bytes_as_laid_out() {
    let mut out = Vec::new();
    encode_main_value(TEXT, b"abc", &mut out).expect("encodes");
    let expected: [u8; 19] = [
      17, 0, // size: 14 + 3
      0, 0, // reserved
      0, 0, 0, 0, // part key id
      3, 0, 0, 0, 0, 0, 0, 0, // length
      b'a', b'b', b'c',
    ];
    assert_eq!(out, expected);
  }

  #[test]
  fn blob_too_long_without_parts_table() {
    let mut out = Vec::new();
    encode_main_value(TINY, &value_of(256), &mut out).expect("fits");
    let refused = encode_main_value(TINY, &value_of(257), &mut out);
    assert_eq!(
      refused.expect_err("refused").code,
      IC_NDB_ERROR_BLOB_TOO_LONG
    );
  }

  #[test]
  fn blob_damaged_heads() {
    let mut out = Vec::new();
    encode_main_value(TEXT, &value_of(5000), &mut out).expect("encodes");
    // Too short for a head.
    let short = decode_main_value(TEXT, &out[..10]);
    assert_eq!(short.expect_err("short").code, IC_NDB_ERROR_BLOB_DAMAGED);
    // A size that is not the value's.
    let mut bad_size = out.clone();
    bad_size[0] = bad_size[0].wrapping_add(1);
    let refused = decode_main_value(TEXT, &bad_size);
    assert_eq!(refused.expect_err("size").code, IC_NDB_ERROR_BLOB_DAMAGED);
    // Fewer inline bytes than the length calls for.
    let mut cut = out.clone();
    cut.truncate(out.len() - 1);
    let size = (cut.len() - 2) as u16;
    cut[0..2].copy_from_slice(&size.to_le_bytes());
    let refused = decode_main_value(TEXT, &cut);
    assert_eq!(refused.expect_err("cut").code, IC_NDB_ERROR_BLOB_DAMAGED);
  }

  #[test]
  fn blob_parts_in_memory_and_on_disk() {
    let mut out = Vec::new();
    let bytes = value_of(744);
    encode_part(PartData::Var, 2000, &bytes, &mut out);
    assert_eq!(out.len(), 746);
    assert_eq!(decode_part(PartData::Var, 744, &out).expect("var"), &bytes);
    let refused = decode_part(PartData::Var, 745, &out);
    assert_eq!(refused.expect_err("var").code, IC_NDB_ERROR_BLOB_DAMAGED);

    let text = PartData::FixedText;
    encode_part(text, 2000, &bytes, &mut out);
    assert_eq!(out.len(), 2000);
    assert_eq!(out[1999], 0x20);
    assert_eq!(decode_part(text, 744, &out).expect("fixed"), &bytes);
  }

  #[test]
  fn blob_part_key_is_main_key_then_part() {
    let mut key = Vec::new();
    part_key(&[7, 8], 3, &mut key);
    assert_eq!(key, [7, 8, 3]);
  }
}
