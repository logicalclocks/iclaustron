// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! Blob columns, which hold MySQL's BLOB, TEXT, JSON and GEOMETRY: the
//! head a main row keeps, where each byte of a value lies, the parts
//! table a column is bound with, and the values a program reads and
//! writes. New: the C had no blobs.
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
//! - **The sizes** are never assumed: they are read from the
//!   dictionary's description of the table, the inline size from the
//!   column's precision, the part size from its scale, the stripe from
//!   its length. MySQL gives each type its own, and the parts table's
//!   data column must agree with the part size, or the column is not
//!   used. A part size of 0 means no parts table: the value must fit
//!   inline, or it is refused with 4266.
//! - **The longest value** a write takes is its MySQL type's (the
//!   author, 2026-11-12), refused with 4266 past it: the type is told
//!   by the sizes, which a MySQL server gives each type, and a column
//!   of other sizes counts as LONG ([`BlobSizes::max_length`]). A value
//!   read is held only to what its column can keep.
//! - **Part k** holds the bytes from `inline + k * part`, a whole part
//!   of them but the last. A value of length `L` past the inline size
//!   has `(L - inline) / part` parts, rounded up.
//! - **The parts table** is `<database>/def/NDB$BLOB_<table id>_<column
//!   id>`: the main table's key columns, copied, then `NDB$PART`, the
//!   part number, `NDB$PKID`, always 0, and `NDB$DATA`, the part's bytes,
//!   behind a 2-byte length for a parts table in memory, or fixed at the
//!   part size and padded with 0x00 (binary) or 0x20 (text) for one on
//!   disk. Binding a table binds its parts tables with it
//!   ([`BlobColumn`]).
//!
//! A part that is missing, or a head or part whose sizes disagree with
//! the value's, is a damaged value, 4267.
//!
//! **A value belongs to its query**, NULL included: a blob field in a
//! record takes no room in the row and has no null bit, and
//! [`BlobValues`] holds each blob field's value apart from the row.
//!
//! **A BLOB query goes in rounds** (chapter 06, phase 12): its own
//! operation on the main row, and the part operations beside it, which
//! the driver plans round by round ([`plan_part_ops`]) and runs on
//! the connection (`blob_run`).
//!
//! The functions here are pure: they work out sizes, check
//! descriptions, encode and decode, plan, and are tested on their own.
//! Verify: chapter 05 §6.10.

use std::sync::Arc;

use ic_port::err;
use ic_port::IcError;
use ic_signals::column_header;
use ic_signals::table_props;
use ic_signals::table_props::ColumnProps;
use ic_signals::table_props::TableProps;

use crate::dict_cache::TableDef;
use crate::dict_client;
use crate::record::Record;
use crate::row_codec::push_padded;

/// A value longer than a write takes: its MySQL type's limit, or what a
/// column without a parts table holds inline ([`BlobSizes::holds`]).
pub const IC_NDB_ERROR_BLOB_TOO_LONG: i32 = 4266;
/// A blob value whose head and parts disagree, or a part missing.
pub const IC_NDB_ERROR_BLOB_DAMAGED: i32 = 4267;

/// The longest value of each MySQL blob type, in bytes: TINYBLOB and
/// TINYTEXT, behind a 1-byte length.
pub const IC_MYSQL_TINY_MAX: u64 = 255;
/// BLOB and TEXT, behind a 2-byte length.
pub const IC_MYSQL_BLOB_MAX: u64 = 65_535;
/// MEDIUMBLOB and MEDIUMTEXT, behind a 3-byte length.
pub const IC_MYSQL_MEDIUM_MAX: u64 = 16_777_215;
/// LONGBLOB and LONGTEXT, JSON and GEOMETRY, behind a 4-byte length.
pub const IC_MYSQL_LONG_MAX: u64 = 4_294_967_295;

/// The inline size a MySQL server gives every blob type but JSON, and
/// the part sizes by which it tells them apart: none for the TINY
/// types, 2 000 for TEXT and BLOB, 4 000 for the MEDIUM types. These
/// tell a column's type; they are never taken for its sizes, which are
/// always read from its description.
const IC_MYSQL_INLINE: u32 = 256;
const IC_MYSQL_BLOB_PART: u32 = 2000;
const IC_MYSQL_MEDIUM_PART: u32 = 4000;

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

/// How a parts table's name begins, before the table and column ids.
const IC_PARTS_TABLE_PREFIX: &str = "NDB$BLOB_";
/// The parts table's own columns, after the main table's key columns.
pub const IC_PARTS_COLUMN_PART: &str = "NDB$PART";
/// The part key id column, always 0.
pub const IC_PARTS_COLUMN_PKID: &str = "NDB$PKID";
/// The column holding a part's bytes.
pub const IC_PARTS_COLUMN_DATA: &str = "NDB$DATA";
/// Columns a parts table has besides the main table's key columns.
const IC_PARTS_OWN_COLUMNS: usize = 3;

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
  /// The sizes of a blob column of the second version, from its
  /// description; `None` for any other column, a blob of the first
  /// version among them.
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

  /// The longest value a MySQL server holds in a column of these sizes
  /// (the author, 2026-11-12): the TINY types' 255 bytes for the sizes
  /// a MySQL server gives them, the TEXT and BLOB types' 65 535 for
  /// theirs, the MEDIUM types' 16 MiB less a byte for theirs, and LONG's
  /// 4 GiB less a byte for any other sizes, LONG's, JSON's and those a
  /// column comment changed among them.
  pub fn mysql_max_length(self) -> u64 {
    if self.inline != IC_MYSQL_INLINE || self.stripe != 0 {
      return IC_MYSQL_LONG_MAX;
    }
    match self.part {
      0 => IC_MYSQL_TINY_MAX,
      IC_MYSQL_BLOB_PART => IC_MYSQL_BLOB_MAX,
      IC_MYSQL_MEDIUM_PART => IC_MYSQL_MEDIUM_MAX,
      _ => IC_MYSQL_LONG_MAX,
    }
  }

  /// The longest value a write takes: its MySQL type's, and for a
  /// column without parts no more than its inline size.
  pub fn max_length(self) -> u64 {
    let mysql = self.mysql_max_length();
    if !self.has_parts() && (self.inline as u64) < mysql {
      return self.inline as u64;
    }
    mysql
  }

  /// True when a write takes a value of this length.
  pub fn holds(self, length: u64) -> bool {
    length <= self.max_length()
  }

  /// True when the column can keep a value of this length, whatever its
  /// MySQL type: what a value read is held to.
  fn keeps(self, length: u64) -> bool {
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
/// the value is longer than a write takes ([`BlobSizes::holds`]).
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
/// disagree with each other or with what the column can keep; a value
/// longer than its MySQL type, which another program may have written,
/// is read whole.
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
  if inline.len() != sizes.inline_len(length) || !sizes.keeps(length) {
    return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
  }
  Ok((length, inline))
}

/// The main row's value of a blob column, as a write carries it in its
/// column section: the column's header, then the head and the inline
/// bytes padded to words, appended to `out`. The head is four whole
/// words, so the inline bytes are packed straight from the value. Refused
/// with 4266 as [`encode_main_value`] is.
pub fn push_main_value(
  sizes: BlobSizes,
  column_id: u32,
  value: &[u8],
  out: &mut Vec<u32>,
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
  // The column's length is its 2-byte length and what that counts.
  out.push(column_header::column_header(column_id, 2 + size as u32));
  let mut head = [0u8; IC_BLOB_HEAD_BYTES];
  head[0..2].copy_from_slice(&size.to_le_bytes());
  // The reserved field and the part key id stay 0.
  head[8..IC_BLOB_HEAD_BYTES].copy_from_slice(&length.to_le_bytes());
  push_padded(out, &head);
  push_padded(out, &value[..inline_len]);
  Ok(())
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
  /// What pads a fixed-size part.
  fn pad(self) -> u8 {
    match self {
      PartData::FixedText => IC_PART_PAD_TEXT,
      _ => IC_PART_PAD_BINARY,
    }
  }
}

/// A parts table's data column, as its description gives it: how it
/// keeps a part's bytes, and how many it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartColumn {
  /// How a part's bytes are kept.
  pub data: PartData,
  /// The most bytes a part holds: the part size.
  pub size: u32,
}

impl PartColumn {
  /// The data column of a parts table, from its description; `None`
  /// for a column of a type no parts table has.
  pub fn of(column: &ColumnProps) -> Option<PartColumn> {
    let data = match column.type_code {
      table_props::IC_TYPE_LONGVARBINARY => PartData::Var,
      table_props::IC_TYPE_LONGVARCHAR => PartData::Var,
      table_props::IC_TYPE_BINARY => PartData::FixedBinary,
      table_props::IC_TYPE_CHAR => PartData::FixedText,
      _ => return None,
    };
    Some(PartColumn {
      data,
      size: column.length,
    })
  }
}

/// A part's value for its data column, into `out`, which is emptied
/// first: its bytes behind their length, or padded to the part size.
pub fn encode_part(column: PartColumn, bytes: &[u8], out: &mut Vec<u8>) {
  out.clear();
  match column.data {
    PartData::Var => {
      out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
      out.extend_from_slice(bytes);
    }
    PartData::FixedBinary | PartData::FixedText => {
      out.extend_from_slice(bytes);
      out.resize(column.size as usize, column.data.pad());
    }
  }
}

/// A part's bytes from its data column's value, as read, when the value
/// says the part holds `expected` of them. Damaged (4267) when it holds
/// another number.
pub fn decode_part(
  column: PartColumn,
  expected: usize,
  value: &[u8],
) -> Result<&[u8], IcError> {
  match column.data {
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
      if value.len() < expected || expected > column.size as usize {
        return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
      }
      Ok(&value[..expected])
    }
  }
}

/// A part's value for its data column, as a write carries it: the
/// column's header, then the value [`encode_part`] makes, padded to
/// words, appended to `out`. `scratch` is where the value is made, kept
/// by the caller for its allocation.
pub fn push_part_value(
  column: PartColumn,
  column_id: u32,
  bytes: &[u8],
  scratch: &mut Vec<u8>,
  out: &mut Vec<u32>,
) {
  encode_part(column, bytes, scratch);
  out.push(column_header::column_header(
    column_id,
    scratch.len() as u32,
  ));
  push_padded(out, scratch);
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

/// The internal name of a blob column's parts table.
pub fn parts_table_name(
  database: &str,
  table_id: u32,
  column_id: u32,
) -> String {
  let own = format!("{}{}_{}", IC_PARTS_TABLE_PREFIX, table_id, column_id);
  dict_client::internal_name(database, &own)
}

/// Why a blob column cannot be read or written: what phase 12 leaves
/// out (chapter 06, "Not in this phase"), or a parts table that is not
/// as the column's description says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobRefusal {
  /// The first version of the blob layout.
  FirstVersion,
  /// Parts striped over partitions: a stripe other than 0.
  Striped,
  /// The table is not placed by a hash map: partitioned by the user.
  NotHashMapped,
  /// The table's rows have a lifetime.
  TimeToLive,
  /// The table is a ring buffer.
  RingBuffer,
  /// The column has parts, and its parts table is not there.
  NoPartsTable,
  /// The parts table's columns are not those the column calls for.
  PartsTableDiffers,
}

impl BlobRefusal {
  /// What it means, for a message.
  pub fn text(self) -> &'static str {
    match self {
      BlobRefusal::FirstVersion => "a blob of the first version",
      BlobRefusal::Striped => "a blob whose parts are striped",
      BlobRefusal::NotHashMapped => "a table partitioned by the user",
      BlobRefusal::TimeToLive => "a table whose rows have a lifetime",
      BlobRefusal::RingBuffer => "a ring buffer table",
      BlobRefusal::NoPartsTable => "a blob whose parts table is missing",
      BlobRefusal::PartsTableDiffers => {
        "a blob whose parts table is not as its column says"
      }
    }
  }
}

/// Why no blob column of the table can be used, if one cannot: the
/// table's own options that phase 12 leaves out.
pub fn table_refusal(main: &TableProps) -> Option<BlobRefusal> {
  if main.fragment_kind != table_props::IC_FRAGMENT_KIND_HASH_MAP {
    return Some(BlobRefusal::NotHashMapped);
  }
  if main.ttl_seconds != table_props::IC_NULL_PTR {
    return Some(BlobRefusal::TimeToLive);
  }
  if main.ring_size != table_props::IC_NULL_PTR {
    return Some(BlobRefusal::RingBuffer);
  }
  None
}

/// A blob column's sizes, or why it cannot be used whatever its parts
/// table: the first version, or parts striped.
pub fn column_sizes(column: &ColumnProps) -> Result<BlobSizes, BlobRefusal> {
  let sizes = match BlobSizes::of(column) {
    Some(sizes) => sizes,
    None => return Err(BlobRefusal::FirstVersion),
  };
  if sizes.stripe != 0 {
    return Err(BlobRefusal::Striped);
  }
  Ok(sizes)
}

/// Check a blob column's parts table against the main table and the
/// column, and give its data column. The parts table must have the
/// main table's key columns, copied in order, then the part number as
/// a key, the part key id, and a data column of the column's kind
/// (binary for a BLOB, characters for a TEXT) holding exactly the part
/// size the column's description gives; each column's id is its place,
/// as a MySQL server makes it.
pub fn check_parts_table(
  main: &TableProps,
  column: &ColumnProps,
  sizes: BlobSizes,
  parts: &TableProps,
) -> Result<PartColumn, BlobRefusal> {
  let differs = BlobRefusal::PartsTableDiffers;
  let mut keys: usize = 0;
  for main_key in &main.attributes {
    if !main_key.primary_key {
      continue;
    }
    let copy = match parts.attributes.get(keys) {
      Some(copy) => copy,
      None => return Err(differs),
    };
    let same = copy.primary_key
      && copy.column_id == keys as u32
      && copy.type_code == main_key.type_code
      && copy.length == main_key.length
      && copy.precision == main_key.precision
      && copy.scale == main_key.scale
      && copy.charset == main_key.charset;
    if !same {
      return Err(differs);
    }
    keys += 1;
  }
  if parts.attributes.len() != keys + IC_PARTS_OWN_COLUMNS {
    return Err(differs);
  }
  let part = &parts.attributes[keys];
  let pkid = &parts.attributes[keys + 1];
  let data = &parts.attributes[keys + 2];
  // Each column's id is its place, as the part operations name them.
  let ids_ok = part.column_id == keys as u32
    && pkid.column_id == keys as u32 + 1
    && data.column_id == keys as u32 + 2;
  let part_ok = part.name == IC_PARTS_COLUMN_PART
    && part.primary_key
    && part.type_code == table_props::IC_TYPE_UNSIGNED;
  let pkid_ok = pkid.name == IC_PARTS_COLUMN_PKID
    && !pkid.primary_key
    && pkid.type_code == table_props::IC_TYPE_UNSIGNED;
  if !ids_ok || !part_ok || !pkid_ok || data.name != IC_PARTS_COLUMN_DATA {
    return Err(differs);
  }
  let part_column = match PartColumn::of(data) {
    Some(part_column) => part_column,
    None => return Err(differs),
  };
  // A TEXT's parts are characters, a BLOB's bytes.
  let text_data = data.is_char_type();
  let text_column = column.type_code == table_props::IC_TYPE_TEXT;
  if text_data != text_column || part_column.size != sizes.part {
    return Err(differs);
  }
  Ok(part_column)
}

/// A blob column of a bound table: its sizes, and its parts table bound
/// with the table, or why it cannot be used.
#[derive(Clone)]
pub struct BlobColumn {
  column_id: u32,
  sizes: Option<BlobSizes>,
  parts: Option<Arc<TableDef>>,
  part_column: Option<PartColumn>,
  refusal: Option<BlobRefusal>,
}

impl std::fmt::Debug for BlobColumn {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "BlobColumn({}, {:?}", self.column_id, self.sizes)?;
    if let Some(parts) = &self.parts {
      write!(f, ", parts {}", parts.name())?;
    }
    if let Some(refusal) = self.refusal {
      write!(f, ", not usable: {}", refusal.text())?;
    }
    write!(f, ")")
  }
}

impl BlobColumn {
  /// A column that can be read and written: with its parts table and
  /// that table's data column, or with neither for part size 0.
  pub(crate) fn usable(
    column_id: u32,
    sizes: BlobSizes,
    parts: Option<(Arc<TableDef>, PartColumn)>,
  ) -> BlobColumn {
    let (parts, part_column) = match parts {
      Some((parts, part_column)) => (Some(parts), Some(part_column)),
      None => (None, None),
    };
    BlobColumn {
      column_id,
      sizes: Some(sizes),
      parts,
      part_column,
      refusal: None,
    }
  }

  /// A column that cannot be used, and why.
  pub(crate) fn refused(
    column_id: u32,
    sizes: Option<BlobSizes>,
    refusal: BlobRefusal,
  ) -> BlobColumn {
    BlobColumn {
      column_id,
      sizes,
      parts: None,
      part_column: None,
      refusal: Some(refusal),
    }
  }

  /// The column's id in its table.
  pub fn column_id(&self) -> u32 {
    self.column_id
  }

  /// Its sizes, from its description; `None` for the first version.
  pub fn sizes(&self) -> Option<BlobSizes> {
    self.sizes
  }

  /// Its parts table, bound with the table; `None` for a column with
  /// part size 0 and for one that cannot be used.
  pub fn parts_table(&self) -> Option<&Arc<TableDef>> {
    self.parts.as_ref()
  }

  /// The parts table's data column, as its description gives it.
  pub fn part_column(&self) -> Option<PartColumn> {
    self.part_column
  }

  /// True if the column can be read and written.
  pub fn is_usable(&self) -> bool {
    self.refusal.is_none()
  }

  /// Why it cannot be, if it cannot.
  pub fn refusal(&self) -> Option<BlobRefusal> {
    self.refusal
  }
}

/// One blob field's value.
#[derive(Clone, Debug)]
struct BlobSlot {
  /// Its column's id.
  field_id: u32,
  /// The column's sizes, from the table's description.
  sizes: BlobSizes,
  /// The bytes, when not NULL; kept, with their capacity, when the
  /// value is set NULL or cleared.
  value: Vec<u8>,
  null: bool,
}

/// The values of a record's blob fields, kept apart from its row: a
/// blob field takes no room in the row and has no null bit, so its
/// value, NULL included, is here. Each field is named by its position
/// in the record, as a field mask counts. A value is NULL until set.
///
/// A query's values and those the synchronous helpers take are this
/// holder; a buffer keeps its capacity when a value is set again or
/// cleared, so that a run after the first allocates nothing.
#[derive(Clone, Debug)]
pub struct BlobValues {
  table: Arc<TableDef>,
  slots: Vec<BlobSlot>,
  /// By position in the record, the field's place in `slots`, or
  /// `u32::MAX` for a field that is not a blob.
  slot_by_position: Vec<u32>,
}

impl BlobValues {
  /// A holder for the blob fields of `rec`, every value NULL.
  pub fn new(rec: &Record) -> BlobValues {
    let mut slots: Vec<BlobSlot> = Vec::new();
    let mut slot_by_position: Vec<u32> = Vec::new();
    for position in 0..rec.num_fields() {
      let field = match rec.field(position) {
        Some(field) => field,
        None => break,
      };
      let column = rec.table().blob_column(field.field_id());
      let sizes = match column.and_then(BlobColumn::sizes) {
        Some(sizes) if field.is_blob() => sizes,
        _ => continue,
      };
      slot_by_position.resize(position as usize + 1, u32::MAX);
      slot_by_position[position as usize] = slots.len() as u32;
      slots.push(BlobSlot {
        field_id: field.field_id(),
        sizes,
        value: Vec::new(),
        null: true,
      });
    }
    BlobValues {
      table: Arc::clone(rec.table()),
      slots,
      slot_by_position,
    }
  }

  /// How many blob fields there are.
  pub fn len(&self) -> usize {
    self.slots.len()
  }

  /// True if the record has no blob field.
  pub fn is_empty(&self) -> bool {
    self.slots.is_empty()
  }

  /// True if the holder was made for a record of the same blob fields
  /// as `rec`, over the same version of its table.
  pub fn fits(&self, rec: &Record) -> bool {
    let table = rec.table();
    if self.table.table_id() != table.table_id()
      || self.table.table_version() != table.table_version()
    {
      return false;
    }
    let mut count: usize = 0;
    for position in 0..rec.num_fields() {
      let blob = match rec.field(position) {
        Some(field) => field.is_blob(),
        None => false,
      };
      if blob {
        if self.slot(position).is_err() {
          return false;
        }
        count += 1;
      }
    }
    count == self.slots.len()
  }

  /// The field at `position`'s value: `None` when it is NULL.
  /// `IC_ERROR_NO_SUCH_FIELD` when the position names no blob field.
  pub fn blob(&self, position: u32) -> Result<Option<&[u8]>, IcError> {
    let slot = &self.slots[self.slot(position)?];
    if slot.null {
      return Ok(None);
    }
    Ok(Some(slot.value.as_slice()))
  }

  /// Set the field at `position` to a copy of `value`. Refused with
  /// 4266 when the value is longer than a write takes: its MySQL
  /// type's limit, or a column without parts' inline size.
  pub fn set_blob(
    &mut self,
    position: u32,
    value: &[u8],
  ) -> Result<(), IcError> {
    let index = self.slot(position)?;
    let slot = &mut self.slots[index];
    if !slot.sizes.holds(value.len() as u64) {
      return Err(IcError::new(IC_NDB_ERROR_BLOB_TOO_LONG));
    }
    slot.value.clear();
    slot.value.extend_from_slice(value);
    slot.null = false;
    Ok(())
  }

  /// Set the field at `position` NULL.
  pub fn set_blob_null(&mut self, position: u32) -> Result<(), IcError> {
    let index = self.slot(position)?;
    self.slots[index].null = true;
    Ok(())
  }

  /// The buffer of the field at `position`, to fill in place; the field
  /// is no longer NULL, and holds what the buffer holds when it is
  /// written. The buffer holds the last value set or read, or nothing.
  /// A value longer than a write takes is refused when it is written.
  pub fn blob_buf_mut(
    &mut self,
    position: u32,
  ) -> Result<&mut Vec<u8>, IcError> {
    let index = self.slot(position)?;
    let slot = &mut self.slots[index];
    slot.null = false;
    Ok(&mut slot.value)
  }

  /// Set every value NULL, keeping each buffer's capacity.
  pub fn clear(&mut self) {
    for slot in &mut self.slots {
      slot.value.clear();
      slot.null = true;
    }
  }

  /// The place in `slots` of the blob field at `position`.
  fn slot(&self, position: u32) -> Result<usize, IcError> {
    match self.slot_by_position.get(position as usize) {
      Some(&index) if index != u32::MAX => Ok(index as usize),
      _ => Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    }
  }

  /// A read has brought the head of the blob field at `position`: a
  /// value of `length` bytes, the first of them `inline`, the rest to
  /// come from its parts ([`part_buf`](Self::part_buf)); or NULL. The
  /// buffer keeps its capacity. Damaged (4267) when the inline bytes
  /// are more than the length.
  pub(crate) fn read_head(
    &mut self,
    position: u32,
    head: Option<(u64, &[u8])>,
  ) -> Result<(), IcError> {
    let index = self.slot(position)?;
    let slot = &mut self.slots[index];
    slot.value.clear();
    match head {
      Some((length, inline)) => {
        if (inline.len() as u64) > length {
          return Err(IcError::new(IC_NDB_ERROR_BLOB_DAMAGED));
        }
        slot.value.resize(length as usize, 0);
        slot.value[..inline.len()].copy_from_slice(inline);
        slot.null = false;
      }
      None => slot.null = true,
    }
    Ok(())
  }

  /// Where a part read puts its bytes: `len` of the `index`th blob
  /// field's, in record order, from `offset`; `None` past the value.
  pub(crate) fn part_buf(
    &mut self,
    index: usize,
    offset: usize,
    len: usize,
  ) -> Option<&mut [u8]> {
    let slot = self.slots.get_mut(index)?;
    slot.value.get_mut(offset..offset.checked_add(len)?)
  }

  /// The sizes of the blob field at `position`.
  pub(crate) fn sizes_at(&self, position: u32) -> Result<BlobSizes, IcError> {
    Ok(self.slots[self.slot(position)?].sizes)
  }

  /// The `index`th blob field, in record order: its column's id and
  /// sizes, and its value, `None` for NULL.
  pub(crate) fn nth(&self, index: usize) -> Option<BlobField<'_>> {
    let slot = self.slots.get(index)?;
    let mut value: Option<&[u8]> = None;
    if !slot.null {
      value = Some(slot.value.as_slice());
    }
    Some(BlobField {
      field_id: slot.field_id,
      sizes: slot.sizes,
      value,
    })
  }
}

/// One blob field of a [`BlobValues`], as [`BlobValues::nth`] gives it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BlobField<'a> {
  /// Its column's id.
  pub field_id: u32,
  /// Its column's sizes.
  pub sizes: BlobSizes,
  /// Its value; `None` for NULL.
  pub value: Option<&'a [u8]>,
}

// ---- The planner ----

/// What a BLOB query does, as its rounds are planned (chapter 06,
/// phase 12, "How each operation goes").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunKind {
  /// An insert that aborts on error: its parts in its first round,
  /// beside the main row, since no one else can lock a row that is not
  /// there yet.
  Insert,
  /// An insert that ignores errors: the main row alone in its first
  /// round, and its parts once it went in, so that a row that was
  /// there already keeps its own parts.
  InsertIgnoring,
  /// A write: the old values' heads read, with an exclusive lock and
  /// errors ignored, beside the main row's write in the first round;
  /// the new parts written and the old ones past them deleted after.
  Write,
  /// An update: as a write, the heads read with the update's abort
  /// option, on a row that must be there.
  Update,
  /// A delete: the heads read first, then the main row and every part
  /// deleted.
  Delete,
  /// A read that locks the main row, shared or exclusive, to the
  /// commit: then the parts, with simple reads.
  LockedRead,
  /// A committed read: the main row read committed first, which is all
  /// for an inline value; a value with parts is read again with a
  /// shared lock and its lock reference, then its parts, and unlocked.
  CommittedRead,
  /// A read that takes the shared lock and its lock reference first,
  /// as a committed read asked to lock first and a simple read do: then
  /// the parts, and the unlock.
  LockFirstRead,
}

impl RunKind {
  /// True when the first round reads the old values' heads, for the
  /// values whose columns have parts.
  pub fn reads_heads(self) -> bool {
    matches!(self, RunKind::Write | RunKind::Update | RunKind::Delete)
  }

  /// True for a read.
  pub fn is_read(self) -> bool {
    matches!(
      self,
      RunKind::LockedRead | RunKind::CommittedRead | RunKind::LockFirstRead
    )
  }
}

/// One blob value's parts, as the rounds write and delete them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueParts {
  /// Its column's sizes.
  pub sizes: BlobSizes,
  /// The new value's length; 0 for NULL, and for a delete.
  pub length: u64,
  /// The next of its parts to write.
  pub next: u64,
  /// How many parts the old value has, once its head has been read.
  pub old_parts: u64,
  /// The next of the old value's parts to delete.
  pub next_delete: u64,
  /// A part to delete, there or not, past the new value's last: when
  /// the row was not there as its head was read, a row inserted before
  /// the write reached it may have left parts the write did not write
  /// over (chapter 06, phase 12).
  pub probe: Option<u64>,
  /// Its parts are read, not written: a value a read brought.
  pub read: bool,
}

impl ValueParts {
  /// A new value of `length` bytes, none of its parts written yet, and
  /// no old value.
  pub fn new(sizes: BlobSizes, length: u64) -> ValueParts {
    ValueParts {
      sizes,
      length,
      next: 0,
      old_parts: 0,
      next_delete: 0,
      probe: None,
      read: false,
    }
  }

  /// A value of `length` bytes a read brought the head of: its parts
  /// are to be read.
  pub fn read_value(sizes: BlobSizes, length: u64) -> ValueParts {
    let mut value = ValueParts::new(sizes, length);
    value.read = true;
    value
  }

  /// How many parts the new value has.
  pub fn parts(&self) -> u64 {
    self.sizes.part_count(self.length)
  }

  /// The old value as its head gave it, `old_length` bytes: its parts
  /// past the new value's last are to be deleted.
  pub fn old_value(&mut self, old_length: u64) {
    self.old_parts = self.sizes.part_count(old_length);
    self.next_delete = self.parts();
  }

  /// The row was not there as the head was read: the part past the new
  /// value's last is probed.
  pub fn no_row(&mut self) {
    if self.sizes.has_parts() {
      self.probe = Some(self.parts());
    }
  }

  /// A probed part was there, and is deleted: the next is probed too,
  /// until one is not.
  pub fn probe_found(&mut self, part: u64) {
    self.probe = Some(part + 1);
  }

  /// True once every write, delete and probe is planned.
  pub fn planned(&self) -> bool {
    self.next >= self.parts()
      && self.next_delete >= self.old_parts
      && self.probe.is_none()
  }
}

/// What is done to a part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartOpKind {
  /// Read, with a simple read: the lock taken and let go of in the
  /// read, since the main row's lock does not fully protect the parts.
  Read,
  /// Written, with the write operation.
  Write,
  /// Deleted: it is there.
  Delete,
  /// Deleted if it is there, its absence ignored.
  Probe,
}

/// A part operation of a round: the value's place among the query's
/// values, the part's number, and what is done to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartOp {
  /// The value, by its place among the query's values.
  pub value: usize,
  /// The part.
  pub part: u64,
  /// What is done to it.
  pub kind: PartOpKind,
}

/// Plan the next round's part operations into `out`, emptied first:
/// value by value, its reads or writes, then its deletes, then its
/// probe, while the round stays within `budget` bytes, at least one
/// operation. A read or a write costs its part's bytes, a delete or a
/// probe a whole part's
/// (ours to choose: the operations of a round stay as few as a round of
/// writes has, whatever is deleted). True when the round is the run's
/// last: nothing is left after it, and it probes nothing, since a part
/// found calls for another.
pub fn plan_part_ops(
  values: &mut [ValueParts],
  budget: u64,
  out: &mut Vec<PartOp>,
) -> bool {
  out.clear();
  let mut bytes: u64 = 0;
  let mut probes = false;
  for (index, value) in values.iter_mut().enumerate() {
    let parts = value.parts();
    let mut kind = PartOpKind::Write;
    if value.read {
      kind = PartOpKind::Read;
    }
    while value.next < parts {
      let cost = value.sizes.part_len(value.length, value.next) as u64;
      if !out.is_empty() && bytes + cost > budget {
        return false;
      }
      out.push(PartOp {
        value: index,
        part: value.next,
        kind,
      });
      bytes += cost;
      value.next += 1;
    }
    let whole = value.sizes.part as u64;
    while value.next_delete < value.old_parts {
      if !out.is_empty() && bytes + whole > budget {
        return false;
      }
      out.push(PartOp {
        value: index,
        part: value.next_delete,
        kind: PartOpKind::Delete,
      });
      bytes += whole;
      value.next_delete += 1;
    }
    if let Some(part) = value.probe {
      if !out.is_empty() && bytes + whole > budget {
        return false;
      }
      out.push(PartOp {
        value: index,
        part,
        kind: PartOpKind::Probe,
      });
      bytes += whole;
      value.probe = None;
      probes = true;
    }
  }
  !probes
}

/// Plan a run's first round into `out`: the part operations that go
/// beside the main row's operation, which only an insert that aborts
/// on error has. True when it may be the run's last round, so that a
/// commit may ride on it: an insert's whose parts all fit it, one that
/// ignores errors with no parts, any other with no column with parts,
/// and a committed read, which is done in one round when its values
/// are inline.
pub fn plan_first_round(
  kind: RunKind,
  values: &mut [ValueParts],
  budget: u64,
  out: &mut Vec<PartOp>,
) -> bool {
  out.clear();
  match kind {
    RunKind::Insert => plan_part_ops(values, budget, out),
    RunKind::InsertIgnoring => values.iter().all(ValueParts::planned),
    RunKind::CommittedRead => true,
    _ => !reads_any_head(values),
  }
}

/// True when some value's column has parts, whose old value's head is
/// then read.
pub fn reads_any_head(values: &[ValueParts]) -> bool {
  values.iter().any(|value| value.sizes.has_parts())
}

/// The column section of a read of the columns `ids`, asked for by
/// bitmask, into `out`, emptied first: the read of the old values'
/// heads, and of a part's bytes.
pub fn masked_read_info(ids: &[u32], out: &mut Vec<u32>) {
  out.clear();
  let mut max_id: u32 = 0;
  for id in ids {
    max_id = max_id.max(*id);
  }
  let mask_words = (max_id / 32 + 1) as usize;
  out.push(column_header::column_header(
    crate::row_codec::IC_COL_READ_MASKED,
    4 * mask_words as u32,
  ));
  out.resize(1 + mask_words, 0);
  for id in ids {
    out[1 + (*id / 32) as usize] |= 1 << (*id % 32);
  }
}

/// What [`walk_packed`] gives each column read: its description, and its
/// value as a request carries it, from a row of lifetime `'row`, `None`
/// for NULL. `'f` is the closure's own, which may borrow what it fills.
pub type EachColumn<'row, 'f> =
  dyn FnMut(&ColumnProps, Option<&'row [u8]>) -> Result<(), IcError> + 'f;

/// Walk a packed row of `table`, as a read by bitmask gets it back:
/// `each` is given each column read, by its description, with its
/// value as a request carries it, a variable-sized one behind its
/// length bytes, `None` for NULL. The words the row took. Bit columns,
/// which no blob read asks for, are refused, as is a row not as asked
/// for (`IC_ERROR_INCONSISTENT_DATA`).
pub fn walk_packed<'a>(
  table: &TableDef,
  words: &'a [u32],
  each: &mut EachColumn<'a, '_>,
) -> Result<usize, IcError> {
  let bad = IcError::new(err::IC_ERROR_INCONSISTENT_DATA);
  let header = match words.first() {
    Some(header) => *header,
    None => return Err(bad),
  };
  if column_header::column_id(header) != crate::row_codec::IC_COL_READ_MASKED {
    return Err(bad);
  }
  let mask_words = (column_header::byte_size(header) / 4) as usize;
  if words.len() < 1 + mask_words {
    return Err(bad);
  }
  let mask = &words[1..1 + mask_words];
  let data = crate::node_connect::words_as_bytes(&words[1 + mask_words..]);
  let total_bits = 32 * mask_words;
  let mut pos: usize = 0;
  let mut bit: usize = 0;
  let mut attr_id: u32 = 0;
  while bit < total_bits {
    let rest = mask[bit / 32] >> (bit % 32);
    if rest == 0 {
      let skip = 32 - bit % 32;
      bit += skip;
      attr_id += skip as u32;
      continue;
    }
    let zeros = rest.trailing_zeros() as usize;
    bit += zeros + 1;
    let id = attr_id + zeros as u32;
    attr_id = id + 1;
    let attr = match table.field(id) {
      Some(attr) => attr,
      None => return Err(bad),
    };
    // After a nullable column read, one more bit: set for NULL.
    if attr.nullable {
      let null = bit < total_bits && (mask[bit / 32] >> (bit % 32)) & 1 != 0;
      bit += 1;
      if null {
        each(attr, None)?;
        continue;
      }
    }
    if attr.size_bits_log2 == table_props::IC_COLUMN_SIZE_BIT {
      return Err(bad);
    }
    // A wide type starts on a word, a byte array on the next byte.
    if attr.size_bits_log2 >= table_props::IC_COLUMN_SIZE_32 {
      pos = pos.next_multiple_of(4);
    }
    let len = match attr.array_kind {
      table_props::IC_ARRAY_KIND_SHORT_VARSIZE => match data.get(pos) {
        Some(n) => 1 + *n as usize,
        None => return Err(bad),
      },
      table_props::IC_ARRAY_KIND_LONG_VARSIZE => {
        if pos + 2 > data.len() {
          return Err(bad);
        }
        2 + data[pos] as usize + 256 * data[pos + 1] as usize
      }
      _ => attr.max_byte_size() as usize,
    };
    if pos + len > data.len() {
      return Err(bad);
    }
    each(attr, Some(&data[pos..pos + len]))?;
    pos += len;
  }
  Ok(1 + mask_words + pos.div_ceil(4))
}

/// The heads a read of blob columns of `table` brought, a packed row as
/// a read by bitmask gets it back: each column read, by its id, with
/// its value's length, `None` for NULL, into `out`, emptied first. The
/// words the row took, what else the read asked for coming after. A
/// head that disagrees with its column's sizes is a damaged value
/// (4267); a row not as asked for is `IC_ERROR_INCONSISTENT_DATA`.
pub fn parse_heads(
  table: &TableDef,
  words: &[u32],
  out: &mut Vec<(u32, Option<u64>)>,
) -> Result<usize, IcError> {
  out.clear();
  let mut each =
    |attr: &ColumnProps, value: Option<&[u8]>| -> Result<(), IcError> {
      let id = attr.column_id;
      let sizes = match table.blob_column(id).and_then(BlobColumn::sizes) {
        Some(sizes) => sizes,
        None => return Err(IcError::new(err::IC_ERROR_INCONSISTENT_DATA)),
      };
      match value {
        Some(value) => {
          let (length, _) = decode_main_value(sizes, value)?;
          out.push((id, Some(length)));
        }
        None => out.push((id, None)),
      }
      Ok(())
    };
  walk_packed(table, words, &mut each)
}

/// A part's value as a simple read of its parts table's data column
/// brought it, a packed row of `parts`, the data column being
/// `data_id`: the column's value, its length bytes first for a part
/// kept in memory, for [`decode_part`]. A row without it is
/// `IC_ERROR_INCONSISTENT_DATA`.
pub fn parse_part_row<'a>(
  parts: &TableDef,
  data_id: u32,
  words: &'a [u32],
) -> Result<&'a [u8], IcError> {
  let mut found: Option<&'a [u8]> = None;
  let mut each =
    |attr: &ColumnProps, value: Option<&'a [u8]>| -> Result<(), IcError> {
      if attr.column_id == data_id {
        found = value;
      }
      Ok(())
    };
  walk_packed(parts, words, &mut each)?;
  match found {
    Some(value) => Ok(value),
    None => Err(IcError::new(err::IC_ERROR_INCONSISTENT_DATA)),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::executor::KeyOp;
  use crate::executor::OpShape;
  use crate::query::ApidQuery;
  use crate::query::ReadKind;
  use crate::query::WriteKind;
  use ic_signals::prop_stream::PropertyWriter;
  use ic_signals::table_props::parse_table_info;
  use ic_signals::table_props::IC_TP_COLUMN_ARRAY_KIND;
  use ic_signals::table_props::IC_TP_COLUMN_COUNT;
  use ic_signals::table_props::IC_TP_COLUMN_END;
  use ic_signals::table_props::IC_TP_COLUMN_ID;
  use ic_signals::table_props::IC_TP_COLUMN_IN_KEY;
  use ic_signals::table_props::IC_TP_COLUMN_LENGTH;
  use ic_signals::table_props::IC_TP_COLUMN_NAME;
  use ic_signals::table_props::IC_TP_COLUMN_NULLABLE;
  use ic_signals::table_props::IC_TP_COLUMN_PRECISION;
  use ic_signals::table_props::IC_TP_COLUMN_SCALE;
  use ic_signals::table_props::IC_TP_COLUMN_TYPE;
  use ic_signals::table_props::IC_TP_FRAGMENT_KIND;
  use ic_signals::table_props::IC_TP_RING_SIZE;
  use ic_signals::table_props::IC_TP_TABLE_NAME;
  use ic_signals::table_props::IC_TP_TTL_SECONDS;
  use ic_signals::table_props::IC_TYPE_BINARY;
  use ic_signals::table_props::IC_TYPE_BLOB;
  use ic_signals::table_props::IC_TYPE_CHAR;
  use ic_signals::table_props::IC_TYPE_INT;
  use ic_signals::table_props::IC_TYPE_LONGVARBINARY;
  use ic_signals::table_props::IC_TYPE_LONGVARCHAR;
  use ic_signals::table_props::IC_TYPE_TEXT;
  use ic_signals::table_props::IC_TYPE_UNSIGNED;

  /// The character set the text columns are given.
  const UTF8MB4: u32 = 255;

  /// A column as a description gives it: name, type, length, precision
  /// (with the character set above it), scale, array kind, key and
  /// nullable.
  struct Col {
    name: &'static str,
    type_code: u32,
    length: u32,
    precision: u32,
    scale: u32,
    array_kind: u32,
    key: bool,
    nullable: bool,
  }

  fn col(name: &'static str, type_code: u32, length: u32) -> Col {
    // Kept as a data node keeps the type: a long variable-sized one
    // behind two length bytes.
    let long_var =
      type_code == IC_TYPE_LONGVARCHAR || type_code == IC_TYPE_LONGVARBINARY;
    let array_kind = if long_var {
      table_props::IC_ARRAY_KIND_LONG_VARSIZE
    } else {
      table_props::IC_ARRAY_KIND_FIXED
    };
    Col {
      name,
      type_code,
      length,
      precision: 0,
      scale: 0,
      array_kind,
      key: false,
      nullable: false,
    }
  }

  /// A blob column of the second version, with the inline size and
  /// part size given, as a MySQL server describes its blob types.
  fn blob_col(name: &'static str, text: bool, inline: u32, part: u32) -> Col {
    let type_code = if text { IC_TYPE_TEXT } else { IC_TYPE_BLOB };
    let charset = if text { UTF8MB4 } else { 0 };
    Col {
      name,
      type_code,
      length: 0,
      precision: inline | (charset << 16),
      scale: part,
      array_kind: table_props::IC_ARRAY_KIND_LONG_VARSIZE,
      key: false,
      nullable: true,
    }
  }

  /// A table described with these columns and table properties.
  fn described(name: &str, cols: &[Col], props: &[(u16, u32)]) -> TableProps {
    let mut w = PropertyWriter::new();
    w.add_string(IC_TP_TABLE_NAME, name);
    for (key, value) in props {
      w.add_u32(*key, *value);
    }
    w.add_u32(IC_TP_COLUMN_COUNT, cols.len() as u32);
    for (id, c) in cols.iter().enumerate() {
      w.add_string(IC_TP_COLUMN_NAME, c.name);
      w.add_u32(IC_TP_COLUMN_ID, id as u32);
      w.add_u32(IC_TP_COLUMN_TYPE, c.type_code);
      w.add_u32(IC_TP_COLUMN_LENGTH, c.length);
      w.add_u32(IC_TP_COLUMN_PRECISION, c.precision);
      w.add_u32(IC_TP_COLUMN_SCALE, c.scale);
      w.add_u32(IC_TP_COLUMN_ARRAY_KIND, c.array_kind);
      w.add_u32(IC_TP_COLUMN_IN_KEY, c.key as u32);
      w.add_u32(IC_TP_COLUMN_NULLABLE, c.nullable as u32);
      w.add_u32(IC_TP_COLUMN_END, 0);
    }
    parse_table_info(w.words()).expect("parsed")
  }

  fn key(name: &'static str) -> Col {
    let mut c = col(name, IC_TYPE_INT, 1);
    c.key = true;
    c
  }

  /// `id INT` key, `t TEXT`, `b LONGBLOB`, `tiny TINYBLOB`.
  fn main_table() -> TableProps {
    described(
      "db/def/t",
      &[
        key("id"),
        blob_col("t", true, 256, 2000),
        blob_col("b", false, 256, 13948),
        blob_col("tiny", false, 256, 0),
      ],
      &[],
    )
  }

  /// A parts table for the main table, its data column `data` given.
  fn parts_table(data: Col) -> TableProps {
    let mut part = col(IC_PARTS_COLUMN_PART, IC_TYPE_UNSIGNED, 1);
    part.key = true;
    described(
      "db/def/NDB$BLOB_7_1",
      &[
        key("id"),
        part,
        col(IC_PARTS_COLUMN_PKID, IC_TYPE_UNSIGNED, 1),
        data,
      ],
      &[],
    )
  }

  /// The sizes of the main table's column, as its description gives.
  fn sizes_of(main: &TableProps, name: &str) -> BlobSizes {
    let column = main.attribute(name).expect("the column");
    BlobSizes::of(column).expect("a blob of the second version")
  }

  fn value_of(length: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    for i in 0..length {
      value.push((i % 251) as u8);
    }
    value
  }

  #[test]
  fn blob_sizes_come_from_the_description() {
    let main = main_table();
    let t = sizes_of(&main, "t");
    assert_eq!((t.inline, t.part, t.stripe), (256, 2000, 0));
    let b = sizes_of(&main, "b");
    assert_eq!((b.inline, b.part), (256, 13948));
    assert!(!sizes_of(&main, "tiny").has_parts());
    // The character set above the precision is not the inline size.
    assert_eq!(main.attribute("t").expect("t").charset, UTF8MB4);
    // A blob of the first version keeps no 2-byte length: not ours.
    let mut v1 = blob_col("v1", false, 256, 2000);
    v1.array_kind = table_props::IC_ARRAY_KIND_FIXED;
    let old = described("db/def/o", &[key("id"), v1], &[]);
    let column = old.attribute("v1").expect("v1");
    assert_eq!(BlobSizes::of(column), None);
    assert_eq!(column_sizes(column), Err(BlobRefusal::FirstVersion));
    let int = main.attribute("id").expect("id");
    assert_eq!(BlobSizes::of(int), None);
  }

  #[test]
  fn blob_parts_at_every_boundary() {
    let text = sizes_of(&main_table(), "t");
    let (inline, part) = (text.inline as u64, text.part as u64);
    // (length, parts, length of the last part)
    let cases: [(u64, u64, u64); 9] = [
      (0, 0, 0),
      (1, 0, 0),
      (inline - 1, 0, 0),
      (inline, 0, 0),
      (inline + 1, 1, 1),
      (inline + part, 1, part),
      (inline + part + 1, 2, 1),
      (inline + 2 * part + 744, 3, 744),
      (1 << 20, 525, (1 << 20) - inline - 524 * part),
    ];
    for (length, parts, last) in cases {
      assert_eq!(text.part_count(length), parts, "length {length}");
      if parts > 0 {
        assert_eq!(text.part_len(length, parts - 1) as u64, last);
        let first = if parts == 1 { last } else { part };
        assert_eq!(text.part_len(length, 0) as u64, first);
        let end = text.part_offset(parts - 1) + last;
        assert_eq!(end, length);
      }
    }
    assert_eq!(text.part_of(inline), 0);
    assert_eq!(text.part_of(inline + part - 1), 0);
    assert_eq!(text.part_of(inline + part), 1);
    let tiny = sizes_of(&main_table(), "tiny");
    assert_eq!(tiny.part_count(10_000), 0);
    assert_eq!(text.inline_len(100), 100);
    assert_eq!(text.inline_len(5000), inline as usize);
  }

  #[test]
  fn blob_head_round_trip() {
    let text = sizes_of(&main_table(), "t");
    let inline = text.inline as usize;
    let part = text.part as usize;
    let mut out = Vec::new();
    for length in [0, 1, inline - 1, inline, inline + 1, inline + part, 5000] {
      let value = value_of(length);
      encode_main_value(text, &value, &mut out).expect("encodes");
      let inline_len = length.min(inline);
      assert_eq!(out.len(), IC_BLOB_HEAD_BYTES + inline_len);
      // The size field is the column's 2-byte length.
      let size = u16::from_le_bytes([out[0], out[1]]) as usize;
      assert_eq!(size, out.len() - 2);
      assert_eq!(&out[2..8], &[0u8; 6]);
      let (got_length, got_inline) =
        decode_main_value(text, &out).expect("decodes");
      assert_eq!(got_length, length as u64);
      assert_eq!(got_inline, &value[..inline_len]);
    }
  }

  #[test]
  fn blob_head_bytes_as_laid_out() {
    let mut out = Vec::new();
    let text = sizes_of(&main_table(), "t");
    encode_main_value(text, b"abc", &mut out).expect("encodes");
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
    // A MySQL TINYBLOB holds 255 bytes, though its column has 256
    // inline.
    let tiny = sizes_of(&main_table(), "tiny");
    assert_eq!(tiny.inline, 256);
    let mut out = Vec::new();
    encode_main_value(tiny, &value_of(255), &mut out).expect("fits");
    let refused = encode_main_value(tiny, &value_of(256), &mut out);
    assert_eq!(
      refused.expect_err("refused").code,
      IC_NDB_ERROR_BLOB_TOO_LONG
    );
    // A value read is held only to what its column keeps: one of 256
    // bytes, which another program may have written, reads whole.
    let mut written = Vec::new();
    written.extend_from_slice(&(14u16 + 256).to_le_bytes());
    written.extend_from_slice(&[0u8; 6]);
    written.extend_from_slice(&256u64.to_le_bytes());
    written.extend_from_slice(&value_of(256));
    let (length, inline) = decode_main_value(tiny, &written).expect("read");
    assert_eq!((length, inline.len()), (256, 256));
  }

  #[test]
  fn blob_longest_values_are_their_mysql_types() {
    let sizes = |inline: u32, part: u32| BlobSizes {
      inline,
      part,
      stripe: 0,
    };
    // As a MySQL server sizes each type (chapter 05 §6.10).
    assert_eq!(sizes(256, 0).max_length(), IC_MYSQL_TINY_MAX);
    assert_eq!(sizes(256, 2000).max_length(), IC_MYSQL_BLOB_MAX);
    assert_eq!(sizes(256, 4000).max_length(), IC_MYSQL_MEDIUM_MAX);
    assert_eq!(sizes(256, 13948).max_length(), IC_MYSQL_LONG_MAX);
    // JSON, and sizes a column comment changed, count as LONG; without
    // parts, no more than the inline size.
    assert_eq!(sizes(4000, 8100).max_length(), IC_MYSQL_LONG_MAX);
    assert_eq!(sizes(512, 2000).max_length(), IC_MYSQL_LONG_MAX);
    assert_eq!(sizes(256, 13948).mysql_max_length(), IC_MYSQL_LONG_MAX);
    assert_eq!(sizes(100, 0).max_length(), 100);
    // Each limit is taken, and a byte past it refused.
    for (inline, part, most) in [
      (256, 0, IC_MYSQL_TINY_MAX),
      (256, 2000, IC_MYSQL_BLOB_MAX),
      (256, 4000, IC_MYSQL_MEDIUM_MAX),
      (256, 13948, IC_MYSQL_LONG_MAX),
    ] {
      assert!(sizes(inline, part).holds(most));
      assert!(!sizes(inline, part).holds(most + 1));
    }
  }

  #[test]
  fn blob_damaged_heads() {
    let text = sizes_of(&main_table(), "t");
    let mut out = Vec::new();
    encode_main_value(text, &value_of(5000), &mut out).expect("encodes");
    // Too short for a head.
    let short = decode_main_value(text, &out[..10]);
    assert_eq!(short.expect_err("short").code, IC_NDB_ERROR_BLOB_DAMAGED);
    // A size that is not the value's.
    let mut bad_size = out.clone();
    bad_size[0] = bad_size[0].wrapping_add(1);
    let refused = decode_main_value(text, &bad_size);
    assert_eq!(refused.expect_err("size").code, IC_NDB_ERROR_BLOB_DAMAGED);
    // Fewer inline bytes than the length calls for.
    let mut cut = out.clone();
    cut.truncate(out.len() - 1);
    let size = (cut.len() - 2) as u16;
    cut[0..2].copy_from_slice(&size.to_le_bytes());
    let refused = decode_main_value(text, &cut);
    assert_eq!(refused.expect_err("cut").code, IC_NDB_ERROR_BLOB_DAMAGED);
  }

  #[test]
  fn blob_parts_in_memory_and_on_disk() {
    let main = main_table();
    let text = main.attribute("t").expect("t");
    let sizes = sizes_of(&main, "t");
    let mut data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_LONGVARCHAR, sizes.part);
    data.precision = UTF8MB4 << 16;
    let var = check_parts_table(&main, text, sizes, &parts_table(data))
      .expect("in memory");
    assert_eq!(
      var,
      PartColumn {
        data: PartData::Var,
        size: sizes.part
      }
    );
    let mut out = Vec::new();
    let bytes = value_of(744);
    encode_part(var, &bytes, &mut out);
    assert_eq!(out.len(), 746);
    assert_eq!(decode_part(var, 744, &out).expect("var"), &bytes);
    let refused = decode_part(var, 745, &out);
    assert_eq!(refused.expect_err("var").code, IC_NDB_ERROR_BLOB_DAMAGED);

    // On disk: fixed at the part size the parts table's column has.
    let mut data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_CHAR, sizes.part);
    data.precision = UTF8MB4 << 16;
    let fixed = check_parts_table(&main, text, sizes, &parts_table(data))
      .expect("on disk");
    assert_eq!(fixed.data, PartData::FixedText);
    encode_part(fixed, &bytes, &mut out);
    assert_eq!(out.len(), sizes.part as usize);
    assert_eq!(out[out.len() - 1], 0x20);
    assert_eq!(decode_part(fixed, 744, &out).expect("fixed"), &bytes);

    let b = main.attribute("b").expect("b");
    let b_sizes = sizes_of(&main, "b");
    let data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_BINARY, b_sizes.part);
    let binary = check_parts_table(&main, b, b_sizes, &parts_table(data))
      .expect("binary on disk");
    encode_part(binary, b"xy", &mut out);
    assert_eq!(out.len(), b_sizes.part as usize);
    assert_eq!(out[2], 0x00);
  }

  #[test]
  fn blob_parts_table_must_be_as_the_column_says() {
    let main = main_table();
    let text = main.attribute("t").expect("t");
    let sizes = sizes_of(&main, "t");
    let differs = Err(BlobRefusal::PartsTableDiffers);
    // Parts of another size than the column's scale.
    let mut data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_LONGVARCHAR, 4000);
    data.precision = UTF8MB4 << 16;
    let parts = parts_table(data);
    assert_eq!(check_parts_table(&main, text, sizes, &parts), differs);
    // Bytes where a TEXT keeps characters.
    let data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_LONGVARBINARY, sizes.part);
    let parts = parts_table(data);
    assert_eq!(check_parts_table(&main, text, sizes, &parts), differs);
    // A key column that is not the main table's.
    let mut part = col(IC_PARTS_COLUMN_PART, IC_TYPE_UNSIGNED, 1);
    part.key = true;
    let mut wide = key("id");
    wide.type_code = table_props::IC_TYPE_BIGINT;
    let mut data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_LONGVARCHAR, sizes.part);
    data.precision = UTF8MB4 << 16;
    let pkid = col(IC_PARTS_COLUMN_PKID, IC_TYPE_UNSIGNED, 1);
    let parts = described("db/def/x", &[wide, part, pkid, data], &[]);
    assert_eq!(check_parts_table(&main, text, sizes, &parts), differs);
    // A column too many.
    let mut part = col(IC_PARTS_COLUMN_PART, IC_TYPE_UNSIGNED, 1);
    part.key = true;
    let mut data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_LONGVARCHAR, sizes.part);
    data.precision = UTF8MB4 << 16;
    let pkid = col(IC_PARTS_COLUMN_PKID, IC_TYPE_UNSIGNED, 1);
    let extra = col("NDB$DIST", IC_TYPE_UNSIGNED, 1);
    let parts =
      described("db/def/x", &[key("id"), extra, part, pkid, data], &[]);
    assert_eq!(check_parts_table(&main, text, sizes, &parts), differs);
  }

  #[test]
  fn blob_tables_and_columns_left_out() {
    assert_eq!(table_refusal(&main_table()), None);
    let cols = [key("id"), blob_col("t", true, 256, 2000)];
    let user = described("db/def/u", &cols, &[(IC_TP_FRAGMENT_KIND, 8)]);
    assert_eq!(table_refusal(&user), Some(BlobRefusal::NotHashMapped));
    let ttl = described("db/def/l", &cols, &[(IC_TP_TTL_SECONDS, 60)]);
    assert_eq!(table_refusal(&ttl), Some(BlobRefusal::TimeToLive));
    let ring = described("db/def/r", &cols, &[(IC_TP_RING_SIZE, 1000)]);
    assert_eq!(table_refusal(&ring), Some(BlobRefusal::RingBuffer));
    let mut striped = blob_col("s", false, 256, 2000);
    striped.length = 4;
    let main = described("db/def/s", &[key("id"), striped], &[]);
    let column = main.attribute("s").expect("s");
    assert_eq!(column_sizes(column), Err(BlobRefusal::Striped));
  }

  /// The main table bound as a table is: `t` with its parts table, `b`
  /// without the one it calls for, `tiny` with none.
  fn bound_main_table() -> Arc<TableDef> {
    let main = main_table();
    let t = main.attribute("t").expect("t");
    let sizes = column_sizes(t).expect("t's sizes");
    let mut data = col(IC_PARTS_COLUMN_DATA, IC_TYPE_LONGVARCHAR, sizes.part);
    data.precision = UTF8MB4 << 16;
    let parts = parts_table(data);
    let part_column =
      check_parts_table(&main, t, sizes, &parts).expect("the parts table");
    let parts = Arc::new(TableDef::new(parts, None));
    let tiny = main.attribute("tiny").expect("tiny");
    let b = main.attribute("b").expect("b");
    let columns = vec![
      BlobColumn::usable(t.column_id, sizes, Some((parts, part_column))),
      BlobColumn::refused(
        b.column_id,
        BlobSizes::of(b),
        BlobRefusal::NoPartsTable,
      ),
      BlobColumn::usable(
        tiny.column_id,
        column_sizes(tiny).expect("tiny's sizes"),
        None,
      ),
    ];
    Arc::new(TableDef::new(main, None).with_blob_columns(columns))
  }

  #[test]
  fn blob_values_are_kept_apart_from_the_row() {
    let def = bound_main_table();
    let blob = def.blob_column(1).expect("t");
    assert!(blob.is_usable());
    assert_eq!(blob.part_column().expect("t's parts").size, 2000);
    assert!(!def.blob_column(2).expect("b").is_usable());
    // id, t, tiny: positions 0, 1 and 2.
    let rec = Record::default_for(&def, Some(&[0, 1, 3])).expect("record");
    let mut values = BlobValues::new(&rec);
    assert_eq!(values.len(), 2);
    assert!(values.fits(&rec));
    let no_such = values.blob(0).expect_err("not a blob");
    assert_eq!(no_such.code, err::IC_ERROR_NO_SUCH_FIELD);
    assert_eq!(values.blob(1).expect("t"), None);
    let text = value_of(5000);
    values.set_blob(1, &text).expect("set t");
    assert_eq!(values.blob(1).expect("t"), Some(&text[..]));
    // tiny has no parts table: no more than its MySQL type's 255 bytes.
    let tiny = def.blob_column(3).expect("tiny").sizes().expect("sizes");
    let too_long = value_of(tiny.max_length() as usize + 1);
    let refused = values.set_blob(2, &too_long).expect_err("too long");
    assert_eq!(refused.code, IC_NDB_ERROR_BLOB_TOO_LONG);
    values.set_blob(2, &too_long[1..]).expect("fits");
    values.set_blob_null(1).expect("NULL");
    assert_eq!(values.blob(1).expect("t"), None);
    // The buffer still holds the last value set.
    let buf = values.blob_buf_mut(1).expect("t's buffer");
    assert_eq!(buf.len(), text.len());
    buf.clear();
    buf.extend_from_slice(b"in place");
    assert_eq!(values.blob(1).expect("t"), Some(&b"in place"[..]));
    values.clear();
    assert_eq!(values.blob(1).expect("t"), None);
    assert_eq!(values.blob(2).expect("tiny"), None);
    assert!(values.blob_buf_mut(1).expect("t").capacity() >= 5000);
    // Made for another set of blob fields.
    let other = Record::default_for(&def, Some(&[0, 1])).expect("record");
    assert!(!values.fits(&other));
    assert!(BlobValues::new(&other).fits(&other));
  }

  #[test]
  fn blob_values_go_in_a_write_as_the_main_row_keeps_them() {
    let def = bound_main_table();
    // id, t, tiny: positions 0, 1 and 2.
    let rec = Record::default_for(&def, Some(&[0, 1, 3])).expect("record");
    let mut row = vec![0u8; rec.row_size() as usize];
    let at = rec.field(0).expect("id").offset() as usize;
    row[at..at + 4].copy_from_slice(&7u32.to_le_bytes());
    let mut values = BlobValues::new(&rec);
    let text = value_of(300);
    values.set_blob(1, &text).expect("t");
    let mut out = Vec::new();
    crate::row_codec::write_attr_info_append(
      &rec,
      &row,
      false,
      Some(&values),
      &mut out,
    )
    .expect("written");
    // id; t's head and inline bytes; tiny NULL.
    let mut want = vec![column_header::column_header(0, 4)];
    want.extend_from_slice(&padded(&7u32.to_le_bytes()));
    let sizes = values.sizes_at(1).expect("t's sizes");
    push_main_value(sizes, 1, &text, &mut want).expect("t");
    want.push(column_header::column_header(3, 0));
    assert_eq!(out, want);
    // Without the values, a blob field cannot be written.
    let refused = crate::row_codec::write_attr_info(&rec, &row, false);
    assert_eq!(
      refused.expect_err("no values").code,
      err::IC_ERROR_NOT_SUPPORTED
    );
    // Each field as the run walks them.
    let t = values.nth(0).expect("t");
    assert_eq!((t.field_id, t.value), (1, Some(&text[..])));
    let tiny = values.nth(1).expect("tiny");
    assert_eq!((tiny.field_id, tiny.value), (3, None));
    assert!(values.nth(2).is_none());
  }

  #[test]
  fn blob_parts_table_name() {
    assert_eq!(parts_table_name("ic_it", 13, 1), "ic_it/def/NDB$BLOB_13_1");
  }

  #[test]
  fn blob_part_key_is_main_key_then_part() {
    let mut key = Vec::new();
    part_key(&[7, 8], 3, &mut key);
    assert_eq!(key, [7, 8, 3]);
  }

  /// `bytes` padded to words, as a request carries them.
  fn padded(bytes: &[u8]) -> Vec<u32> {
    let mut out = Vec::new();
    push_padded(&mut out, bytes);
    out
  }

  #[test]
  fn blob_main_value_as_a_write_carries_it() {
    let text = sizes_of(&main_table(), "t");
    let inline = text.inline as usize;
    let mut bytes = Vec::new();
    for length in [0, 1, 3, inline - 1, inline, inline + 1, 5000] {
      let value = value_of(length);
      encode_main_value(text, &value, &mut bytes).expect("encodes");
      let mut out = vec![99];
      push_main_value(text, 5, &value, &mut out).expect("pushed");
      // The column's length is the head and the inline bytes.
      let mut want =
        vec![99, column_header::column_header(5, bytes.len() as u32)];
      want.extend_from_slice(&padded(&bytes));
      assert_eq!(out, want, "length {length}");
    }
    let tiny = sizes_of(&main_table(), "tiny");
    let mut out = Vec::new();
    let refused = push_main_value(tiny, 3, &value_of(inline + 1), &mut out);
    assert_eq!(refused.expect_err("4266").code, IC_NDB_ERROR_BLOB_TOO_LONG);
  }

  #[test]
  fn blob_part_value_as_a_write_carries_it() {
    let var = PartColumn {
      data: PartData::Var,
      size: 2000,
    };
    let mut scratch = Vec::new();
    let mut out = Vec::new();
    push_part_value(var, 3, b"abcde", &mut scratch, &mut out);
    // A 2-byte length, then the bytes: 7 bytes in two words.
    let mut want = vec![column_header::column_header(3, 7)];
    want.extend_from_slice(&padded(b"\x05\x00abcde"));
    assert_eq!(out, want);
    let fixed = PartColumn {
      data: PartData::FixedText,
      size: 8,
    };
    out.clear();
    push_part_value(fixed, 3, b"abc", &mut scratch, &mut out);
    let mut want = vec![column_header::column_header(3, 8)];
    want.extend_from_slice(&padded(b"abc     "));
    assert_eq!(out, want);
  }

  /// The rounds a run of `kind` takes for values of these lengths, and
  /// the parts each round writes.
  fn rounds_of(
    kind: RunKind,
    sizes: BlobSizes,
    lengths: &[u64],
    budget: u64,
  ) -> Vec<usize> {
    let mut values: Vec<ValueParts> = Vec::new();
    for length in lengths {
      values.push(ValueParts::new(sizes, *length));
    }
    let mut out = Vec::new();
    let mut last = plan_first_round(kind, &mut values, budget, &mut out);
    let mut rounds = vec![out.len()];
    while !last {
      last = plan_part_ops(&mut values, budget, &mut out);
      assert!(!out.is_empty(), "a round writes a part at least");
      rounds.push(out.len());
    }
    for value in &values {
      assert!(value.planned());
    }
    rounds
  }

  #[test]
  fn blob_rounds_of_an_insert() {
    let text = sizes_of(&main_table(), "t");
    let (inline, part) = (text.inline as u64, text.part as u64);
    let mib: u64 = 1 << 20;
    let insert = RunKind::Insert;
    let ignoring = RunKind::InsertIgnoring;
    // An inline value, or none: one round with no parts.
    for length in [0, 1, inline - 1, inline] {
      assert_eq!(rounds_of(insert, text, &[length], mib), [0]);
      assert_eq!(rounds_of(ignoring, text, &[length], mib), [0]);
    }
    // Parts beside the main row; once it went in when ignoring errors.
    assert_eq!(rounds_of(insert, text, &[inline + 1], mib), [1]);
    assert_eq!(rounds_of(insert, text, &[inline + part], mib), [1]);
    assert_eq!(rounds_of(insert, text, &[inline + part + 1], mib), [2]);
    assert_eq!(rounds_of(ignoring, text, &[inline + 1], mib), [0, 1]);
    // 1 MiB: 525 parts, the last of 320 bytes, within one round.
    assert_eq!(rounds_of(insert, text, &[mib], mib), [525]);
    // 10 MiB: 5 243 parts, 524 a round, the last round three.
    let mut want = vec![524; 10];
    want.push(3);
    assert_eq!(rounds_of(insert, text, &[10 * mib], mib), want);
    // Values in order; a round shared between them.
    let two =
      rounds_of(insert, text, &[inline + 3 * part, inline + part], 5000);
    assert_eq!(two, [2, 2]);
    // A round smaller than a part still writes one.
    assert_eq!(rounds_of(insert, text, &[inline + 3 * part], 1), [1, 1, 1]);
  }

  /// A part operation, for comparing plans.
  fn op(value: usize, part: u64, kind: PartOpKind) -> PartOp {
    PartOp { value, part, kind }
  }

  #[test]
  fn blob_round_writes_value_by_value_part_by_part() {
    let text = sizes_of(&main_table(), "t");
    let (inline, part) = (text.inline as u64, text.part as u64);
    let mut values = [
      ValueParts::new(text, inline + 2 * part),
      ValueParts::new(text, 0),
      ValueParts::new(text, inline + 1),
    ];
    let mut out = Vec::new();
    assert!(plan_part_ops(&mut values, 1 << 20, &mut out));
    let write = PartOpKind::Write;
    assert_eq!(out, [op(0, 0, write), op(0, 1, write), op(2, 0, write)]);
    // Nothing is left: a further round plans nothing.
    assert!(plan_part_ops(&mut values, 1 << 20, &mut out));
    assert!(out.is_empty());
  }

  #[test]
  fn blob_a_value_grown_shrunk_or_gone_deletes_the_old_parts_past_it() {
    let text = sizes_of(&main_table(), "t");
    let (inline, part) = (text.inline as u64, text.part as u64);
    let (write, delete) = (PartOpKind::Write, PartOpKind::Delete);
    let mut out = Vec::new();
    // Shrunk from three parts to one: one written, two deleted.
    let mut shrunk = [ValueParts::new(text, inline + 1)];
    shrunk[0].old_value(inline + 3 * part);
    assert!(plan_part_ops(&mut shrunk, 1 << 20, &mut out));
    assert_eq!(out, [op(0, 0, write), op(0, 1, delete), op(0, 2, delete)]);
    // Grown from one to two: both written, nothing deleted.
    let mut grown = [ValueParts::new(text, inline + part + 1)];
    grown[0].old_value(inline + 5);
    assert!(plan_part_ops(&mut grown, 1 << 20, &mut out));
    assert_eq!(out, [op(0, 0, write), op(0, 1, write)]);
    // Set NULL or empty, or deleted: every old part deleted.
    let mut gone = [ValueParts::new(text, 0)];
    gone[0].old_value(inline + part + 1);
    assert!(plan_part_ops(&mut gone, 1 << 20, &mut out));
    assert_eq!(out, [op(0, 0, delete), op(0, 1, delete)]);
    // Inline before and after: nothing to do.
    let mut inline_only = [ValueParts::new(text, 10)];
    inline_only[0].old_value(20);
    assert!(plan_part_ops(&mut inline_only, 1 << 20, &mut out));
    assert!(out.is_empty());
    // A delete costs a whole part: two parts' worth a round.
    let mut many = [ValueParts::new(text, 0)];
    many[0].old_value(inline + 5 * part);
    assert!(!plan_part_ops(&mut many, 2 * part, &mut out));
    assert_eq!(out, [op(0, 0, delete), op(0, 1, delete)]);
    assert!(!plan_part_ops(&mut many, 2 * part, &mut out));
    assert!(plan_part_ops(&mut many, 2 * part, &mut out));
    assert_eq!(out, [op(0, 4, delete)]);
  }

  #[test]
  fn blob_a_write_that_found_no_row_probes_until_a_part_is_missing() {
    let text = sizes_of(&main_table(), "t");
    let (inline, part) = (text.inline as u64, text.part as u64);
    let (write, probe) = (PartOpKind::Write, PartOpKind::Probe);
    let mut out = Vec::new();
    let mut values = [ValueParts::new(text, inline + part)];
    values[0].no_row();
    // The new part, and a probe past it: not the last round.
    assert!(!plan_part_ops(&mut values, 1 << 20, &mut out));
    assert_eq!(out, [op(0, 0, write), op(0, 1, probe)]);
    // The probed part was there: the next is probed.
    values[0].probe_found(1);
    assert!(!plan_part_ops(&mut values, 1 << 20, &mut out));
    assert_eq!(out, [op(0, 2, probe)]);
    // It was not: nothing is left.
    assert!(plan_part_ops(&mut values, 1 << 20, &mut out));
    assert!(out.is_empty());
    // A column without parts probes nothing.
    let tiny = sizes_of(&main_table(), "tiny");
    let mut plain = [ValueParts::new(tiny, 10)];
    plain[0].no_row();
    assert!(plan_part_ops(&mut plain, 1 << 20, &mut out));
    assert!(out.is_empty());
  }

  #[test]
  fn blob_first_rounds_of_each_kind() {
    let text = sizes_of(&main_table(), "t");
    let tiny = sizes_of(&main_table(), "tiny");
    let mut out = Vec::new();
    for kind in [RunKind::Write, RunKind::Update, RunKind::Delete] {
      // Heads to read: the first round is never the last, and has no
      // part operation.
      let mut values = [ValueParts::new(text, 5000), ValueParts::new(tiny, 1)];
      assert!(!plan_first_round(kind, &mut values, 1 << 20, &mut out));
      assert!(out.is_empty());
      assert!(reads_any_head(&values));
      // Only columns without parts: one round, no head read.
      let mut plain = [ValueParts::new(tiny, 1)];
      assert!(plan_first_round(kind, &mut plain, 1 << 20, &mut out));
      assert!(!reads_any_head(&plain));
    }
    // A read that locks may find parts to read: its first round is not
    // the last, unless no column has parts. A committed read's may be,
    // when its values are inline.
    for kind in [RunKind::LockedRead, RunKind::LockFirstRead] {
      let mut values = [ValueParts::new(text, 0)];
      assert!(!plan_first_round(kind, &mut values, 1 << 20, &mut out));
      let mut plain = [ValueParts::new(tiny, 0)];
      assert!(plan_first_round(kind, &mut plain, 1 << 20, &mut out));
    }
    let mut values = [ValueParts::new(text, 0)];
    let committed = RunKind::CommittedRead;
    assert!(plan_first_round(committed, &mut values, 1 << 20, &mut out));
    assert!(out.is_empty());
  }

  #[test]
  fn blob_parts_read_go_in_rounds_as_writes_do() {
    let text = sizes_of(&main_table(), "t");
    let (inline, part) = (text.inline as u64, text.part as u64);
    let read = PartOpKind::Read;
    let mut values = [
      ValueParts::read_value(text, inline + 2 * part + 5),
      ValueParts::read_value(text, 10),
    ];
    let mut out = Vec::new();
    // Three parts, two parts' bytes a round.
    assert!(!plan_part_ops(&mut values, 2 * part, &mut out));
    assert_eq!(out, [op(0, 0, read), op(0, 1, read)]);
    assert!(plan_part_ops(&mut values, 2 * part, &mut out));
    assert_eq!(out, [op(0, 2, read)]);
  }

  #[test]
  fn blob_a_part_read_is_parsed_from_its_row() {
    let def = bound_main_table();
    let parts = def.blob_column(1).and_then(BlobColumn::parts_table);
    let parts = parts.expect("t's parts table");
    let column = def.blob_column(1).and_then(BlobColumn::part_column);
    let column = column.expect("t's data column");
    // NDB$DATA, column 3, read alone by bitmask: a 2-byte length, then
    // the bytes.
    let bytes = value_of(744);
    let mut encoded = Vec::new();
    encode_part(column, &bytes, &mut encoded);
    let masked = crate::row_codec::IC_COL_READ_MASKED;
    let mut info = Vec::new();
    masked_read_info(&[3], &mut info);
    assert_eq!(info, [column_header::column_header(masked, 4), 1 << 3]);
    let mut words = info.clone();
    words.extend_from_slice(&padded(&encoded));
    let value = parse_part_row(parts, 3, &words).expect("the part");
    assert_eq!(value, &encoded[..]);
    assert_eq!(decode_part(column, 744, value).expect("decoded"), &bytes);
    // A row without the column read is not the part.
    let mut other = info.clone();
    other[1] = 0;
    let refused = parse_part_row(parts, 3, &other).expect_err("no part");
    assert_eq!(refused.code, err::IC_ERROR_INCONSISTENT_DATA);
  }

  #[test]
  fn blob_a_read_puts_heads_in_the_values_and_parts_after() {
    let def = bound_main_table();
    // id, t, tiny: positions 0, 1 and 2; t and tiny nullable.
    let rec = Record::default_for(&def, Some(&[0, 1, 3])).expect("record");
    let mut values = BlobValues::new(&rec);
    let text = value_of(300);
    let sizes = values.sizes_at(1).expect("t's sizes");
    let mut head = Vec::new();
    encode_main_value(sizes, &text, &mut head).expect("t's head");
    // id read; t read, not NULL; b not read; tiny read, NULL. The INT
    // first, on a word, then t's bytes at the next byte.
    let masked = crate::row_codec::IC_COL_READ_MASKED;
    let mut words = vec![column_header::column_header(masked, 4)];
    words.push(0b11_0011);
    let mut data = 7u32.to_le_bytes().to_vec();
    data.extend_from_slice(&head);
    words.extend_from_slice(&padded(&data));
    let mut row = vec![0u8; rec.row_size() as usize];
    let used = crate::row_codec::unpack_row_with(
      &rec,
      &words,
      &mut row,
      Some(&mut values),
    )
    .expect("unpacked");
    assert_eq!(used, words.len());
    let at = rec.field(0).expect("id").offset() as usize;
    assert_eq!(row[at..at + 4], 7u32.to_le_bytes());
    // The head's length, the inline bytes, the rest to come.
    let got = values.blob(1).expect("t").expect("not NULL");
    assert_eq!(got.len(), 300);
    assert_eq!(&got[..256], &text[..256]);
    assert_eq!(values.blob(2).expect("tiny"), None);
    let rest = values.part_buf(0, 256, 44).expect("part 0's place");
    rest.copy_from_slice(&text[256..]);
    assert_eq!(values.blob(1).expect("t"), Some(&text[..]));
    assert!(values.part_buf(0, 256, 45).is_none());
    // Without the values, a blob field cannot be read.
    let refused = crate::row_codec::unpack_row(&rec, &words, &mut row);
    assert_eq!(
      refused.expect_err("no values").code,
      err::IC_ERROR_NOT_SUPPORTED
    );
  }

  #[test]
  fn blob_heads_are_read_by_bitmask_and_parsed() {
    let def = bound_main_table();
    let mut info = Vec::new();
    masked_read_info(&[1, 3], &mut info);
    let masked = crate::row_codec::IC_COL_READ_MASKED;
    assert_eq!(info, [column_header::column_header(masked, 4), 0b1010]);
    // t, read and not NULL; tiny, read and NULL. A column takes a bit,
    // read or not, and a nullable one read its NULL bit after it: t at
    // bit 1, its NULL bit 2, b, not read, at bit 3, tiny at bit 4, its
    // NULL bit 5, set.
    let sizes = def.blob_column(1).and_then(BlobColumn::sizes);
    let sizes = sizes.expect("t's sizes");
    let mut value = Vec::new();
    encode_main_value(sizes, &value_of(5000), &mut value).expect("t");
    let mut words = vec![column_header::column_header(masked, 4)];
    words.push(0b11_0010);
    words.extend_from_slice(&padded(&value));
    let mut heads = Vec::new();
    // What a read through a unique index asks for after the heads
    // follows them: the words they took say where.
    let used = words.len();
    words.push(0xABCD);
    let took = parse_heads(&def, &words, &mut heads).expect("parsed");
    assert_eq!(took, used);
    assert_eq!(heads, [(1, Some(5000)), (3, None)]);
    words.truncate(used);
    // A head that disagrees with its column is a damaged value: a
    // length of 100, which would leave 100 bytes inline, not 256.
    let mut bad = words.clone();
    bad[4] = 100;
    let refused = parse_heads(&def, &bad, &mut heads).expect_err("damaged");
    assert_eq!(refused.code, IC_NDB_ERROR_BLOB_DAMAGED);
    // A row not read by bitmask is not ours.
    let refused = parse_heads(&def, &[7], &mut heads).expect_err("not ours");
    assert_eq!(refused.code, err::IC_ERROR_INCONSISTENT_DATA);
  }

  #[test]
  fn values_lent_to_a_query_come_back_uncopied() {
    let def = bound_main_table();
    let key_rec = Record::default_for(&def, Some(&[0])).expect("key");
    // id, t, tiny: positions 0, 1 and 2.
    let rec = Record::default_for(&def, Some(&[0, 1, 3])).expect("record");
    let mut query = ApidQuery::new(&def, &key_rec, &rec).expect("a query");
    let mut values = BlobValues::new(&rec);
    values.set_blob(1, &value_of(5000)).expect("t");
    let bytes = values.blob(1).expect("t").map(<[u8]>::as_ptr);
    query.swap_blob_values(&mut values).expect("lent");
    // The query holds the value itself, the holder the query's own.
    assert_eq!(query.blob(1).expect("t").map(<[u8]>::as_ptr), bytes);
    assert_eq!(values.blob(1).expect("t"), None);
    query.swap_blob_values(&mut values).expect("given back");
    assert_eq!(values.blob(1).expect("t").map(<[u8]>::as_ptr), bytes);
    assert_eq!(query.blob(1).expect("t"), None);
    // A holder for other blob fields is not the query's to take.
    let other = Record::default_for(&def, Some(&[0, 1])).expect("record");
    let mut wrong = BlobValues::new(&other);
    let refused = query.swap_blob_values(&mut wrong).expect_err("refused");
    assert_eq!(refused.code, err::IC_ERROR_RECORD_LAYOUT);
    // Without blob fields, an empty holder is nothing to lend.
    let mut plain = ApidQuery::new(&def, &key_rec, &key_rec).expect("plain");
    let mut empty = BlobValues::new(&key_rec);
    plain.swap_blob_values(&mut empty).expect("nothing to swap");
  }

  #[test]
  fn an_executor_operation_holds_its_shapes_values() {
    let def = bound_main_table();
    let key_rec = Record::default_for(&def, Some(&[0])).expect("key");
    let rec = Record::default_for(&def, Some(&[0, 1, 3])).expect("record");
    let shape = OpShape::new(&def, &key_rec, &rec).expect("a shape");
    let mut op = KeyOp::write(&shape, WriteKind::Insert);
    assert_eq!(op.blob(1).expect("t"), None);
    op.set_blob(1, b"text").expect("t");
    assert_eq!(op.blob(1).expect("t"), Some(&b"text"[..]));
    op.blob_buf_mut(2)
      .expect("tiny")
      .extend_from_slice(b"in place");
    assert_eq!(op.blob(2).expect("tiny"), Some(&b"in place"[..]));
    op.set_blob_null(1).expect("NULL");
    assert_eq!(op.blob(1).expect("t"), None);
    let not_blob = op.blob(0).expect_err("id is no blob");
    assert_eq!(not_blob.code, err::IC_ERROR_NO_SUCH_FIELD);
    assert!(!op.blob_lock_first() && !op.blob_probe());
    op.set_blob_lock_first(true);
    op.set_blob_probe(true);
    assert!(op.blob_lock_first() && op.blob_probe());
    // A shape without blob fields holds none, and one with them reads
    // nothing after a program, as it runs none.
    let plain = OpShape::new(&def, &key_rec, &key_rec).expect("a shape");
    let read = KeyOp::read(&plain, ReadKind::Committed);
    assert!(read.blob_values().is_none());
    let none = read.blob(1).expect_err("no blob fields");
    assert_eq!(none.code, err::IC_ERROR_NO_SUCH_FIELD);
    let refused = shape.reading_after(&key_rec).expect_err("refused");
    assert_eq!(refused.code, err::IC_ERROR_NOT_SUPPORTED);
  }
}
