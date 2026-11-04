// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! Fuzz tests of this crate's decoders (chapter 08, "Fuzzing"). A plain
//! `cargo test` leaves them out; `cargo xtask fuzz` runs them.
//!
//! Each test hands `ic_port::fuzz::run` seeds made by our own encoders
//! and a target that gives one input to the decoders. The target throws
//! away what a decoder returns: an error is the right answer to bytes
//! that make no sense, so only a panic, a hang or a slow case fails.

use ic_port::fuzz;

use crate::blocks;
use crate::bundle;
use crate::bundle::BundlePart;
use crate::collation_desc::CollationDescConf;
use crate::collation_desc::CollationDescRef;
use crate::collation_desc::Contraction;
use crate::collation_desc::WeightRun;
use crate::collation_desc::IC_COLLATION_ALL_SENT;
use crate::coord_alloc::CoordAllocConf;
use crate::coord_alloc::CoordAllocRef;
use crate::coord_alloc::CoordFreeConf;
use crate::coord_alloc::CoordFreeRef;
use crate::header;
use crate::header::FragmentState;
use crate::header::SignalHeader;
use crate::key_ops::KeyOpConf;
use crate::key_ops::KeyOpRef;
use crate::key_ops::RollbackConf;
use crate::key_ops::RowDataRep;
use crate::key_ops::TakeoverConf;
use crate::key_ops::TakeoverRef;
use crate::key_ops::TransAbortedRep;
use crate::key_ops::TransCommitConf;
use crate::key_ops::TransEndRef;
use crate::key_ops::IC_KEY_OP_CONF_DIRTY_READ_BIT;
use crate::list_objects;
use crate::list_objects::ListObjectsConf;
use crate::membership::HeartbeatConf;
use crate::membership::HeartbeatRef;
use crate::membership::NodeLostDoneRep;
use crate::membership::NodeLostRep;
use crate::membership::NodeState;
use crate::membership::StartLevel;
use crate::prop_stream::PropertyReader;
use crate::prop_stream::PropertyWriter;
use crate::scans;
use crate::scans::ScanConf;
use crate::scans::ScanRef;
use crate::scans::ScanRowKeyRep;
use crate::scans::IC_SCAN_CONF_END_OF_DATA;
use crate::scans::IC_SCAN_RECEIVER_DONE;
use crate::table_change::TableChangedRep;
use crate::table_desc;
use crate::table_desc::TableDescConf;
use crate::table_desc::TableDescRef;
use crate::table_props::*;

// ---- The header ----

/// Every message in the words, as the receive path walks them: the
/// length peeked at, the message decoded, its sections looked at.
fn header_decode(input: &[u8]) {
  let mut words: Vec<u32> = Vec::new();
  fuzz::words_of(input, &mut words);
  let mut at: usize = 0;
  while at < words.len() {
    let _ = header::peek_message_len(&words[at..]);
    let message = match header::decode(&words[at..]) {
      Ok(message) => message,
      Err(_) => return,
    };
    // One past the most sections there are, to see that it is refused.
    let mut i: usize = 0;
    while i <= header::IC_MAX_SECTIONS {
      if let Some(section) = message.section(i) {
        let _ = header::compute_checksum(section);
      }
      i += 1;
    }
    let _ = header::compute_checksum(message.data);
    at += message.total_words;
  }
}

/// A message as `header::encode` writes it, in bytes.
fn message_bytes(
  head: &SignalHeader,
  data: &[u32],
  sections: &[&[u32]],
  use_checksum: bool,
) -> Vec<u8> {
  let mut words: Vec<u32> = Vec::new();
  header::encode(head, data, sections, use_checksum, &mut words)
    .expect("encode");
  fuzz::bytes_of(&words)
}

/// A signal too large for one message, as `header::encode_train` writes
/// it, in bytes.
fn train_bytes(
  head: &SignalHeader,
  data: &[u32],
  sections: &[&[u32]],
  use_checksum: bool,
) -> Vec<u8> {
  let mut words: Vec<u32> = Vec::new();
  header::encode_train(head, data, sections, use_checksum, 77, &mut words)
    .expect("encode");
  fuzz::bytes_of(&words)
}

/// A header from a user thread to the coordinator.
fn a_header(signal_id: Option<u32>, state: FragmentState) -> SignalHeader {
  let mut head = SignalHeader::new(
    12,
    blocks::api_block_of_thread(3),
    blocks::IC_BLOCK_COORDINATOR,
  );
  head.signal_id = signal_id;
  head.fragment_state = state;
  head.trace = 7;
  head.prio = 1;
  head
}

fn header_seeds() -> Vec<Vec<u8>> {
  let plain = a_header(None, FragmentState::Whole);
  let with_id = a_header(Some(0xDEAD_BEEF), FragmentState::Whole);
  let last = a_header(None, FragmentState::Last);
  let key = [0x11u32, 0x22, 0x33];
  let attr = [0x44u32; 20];
  let small = [5u32, 6];
  let big: Vec<u32> = vec![0xABu32; 9000];
  // Several messages back to back, as one read brings them.
  let mut stream = message_bytes(&plain, &[1, 2], &[], false);
  stream.extend_from_slice(&message_bytes(&with_id, &[3], &[&key], true));
  stream.extend_from_slice(&message_bytes(&last, &[4, 5, 6], &[], false));
  vec![
    message_bytes(&plain, &[1, 2, 3, 4, 5], &[], false),
    message_bytes(&with_id, &[9; 8], &[&key, &attr], true),
    message_bytes(&plain, &[7], &[&key, &attr, &small], true),
    message_bytes(&with_id, &[], &[&key], false),
    message_bytes(&last, &[0x55; 25], &[], true),
    stream,
    train_bytes(&with_id, &[11, 12, 13], &[&key, &big], true),
    train_bytes(&plain, &[14], &[&big], false),
  ]
}

#[test]
#[ignore = "a fuzz run: cargo xtask fuzz"]
fn fuzz_header_decode() {
  let seeds = header_seeds();
  fuzz::run("header_decode", &seeds, header_decode);
}

// ---- Bundled signals ----

/// The words as a bundled signal's data: taken apart both ways, and
/// each part's words looked at where it says they lie.
fn bundle_unpack(input: &[u8]) {
  let mut words: Vec<u32> = Vec::new();
  fuzz::words_of(input, &mut words);
  let _ = bundle::unpack(&words);
  let mut parts: Vec<BundlePart> = Vec::new();
  if bundle::unpack_into(&words, &mut parts).is_err() {
    return;
  }
  for part in &parts {
    let _ = header::compute_checksum(&words[part.start..part.start + part.len]);
  }
}

/// Parts packed as the data of a bundled signal: for each, its header
/// word with the block it is for, then its words.
fn packed(parts: &[(u16, &[u32])]) -> Vec<u8> {
  let mut words: Vec<u32> = Vec::new();
  for (block, data) in parts {
    words.push(((*block as u32) << 16) | (data.len() as u32 - 3));
    words.extend_from_slice(data);
  }
  fuzz::bytes_of(&words)
}

fn bundle_seeds() -> Vec<Vec<u8>> {
  let five = [1u32, 2, 3, 4, 5];
  let seven = [10u32, 11, 12, 13, 14, 15, 16];
  let three = [7u32, 8, 9];
  let most = [0x5Au32; bundle::IC_BUNDLE_MAX_PART_WORDS];
  // KEY_OP_CONF of one operation each, as a coordinator bundles them.
  let conf = [0x10u32, 0, 1, 0x1234, 0x1, 0x20, 3];
  vec![
    packed(&[(0x8001, &five), (0x8002, &seven)]),
    packed(&[(0x8003, &three)]),
    packed(&[(0x8004, &most)]),
    packed(&[(0x8001, &conf), (0x8002, &conf), (0x8003, &conf)]),
    Vec::new(),
  ]
}

#[test]
#[ignore = "a fuzz run: cargo xtask fuzz"]
fn fuzz_bundle_unpack() {
  let seeds = bundle_seeds();
  fuzz::run("bundle_unpack", &seeds, bundle_unpack);
}

// ---- Property streams ----

/// The words as properties, read to the end or to the first error.
fn property_stream(input: &[u8]) {
  let mut words: Vec<u32> = Vec::new();
  fuzz::words_of(input, &mut words);
  let mut reader = PropertyReader::new(&words);
  while let Ok(Some(property)) = reader.read_next() {
    let _ = property.as_u32();
  }
}

fn property_seeds() -> Vec<Vec<u8>> {
  let mut mixed = PropertyWriter::new();
  mixed.add_string(1, "ictest/def/t1");
  mixed.add_u32(2, 17);
  mixed.add_u64(154, 0x0000_0001_0000_0002);
  mixed.add_binary(1021, &[1, 2, 3, 4, 5]);
  mixed.add_u32(9876, 5);
  // Text and bytes of every length the padding to words treats apart.
  let mut lengths = PropertyWriter::new();
  let mut len: usize = 0;
  while len < 9 {
    let text = "x".repeat(len);
    lengths.add_string(1000 + len as u16, &text);
    lengths.add_binary(2000 + len as u16, text.as_bytes());
    len += 1;
  }
  let mut numbers = PropertyWriter::new();
  let mut key: u16 = 0;
  while key < 32 {
    numbers.add_u32(key, (key as u32) << 20);
    numbers.add_u64(key + 100, u64::MAX - key as u64);
    key += 1;
  }
  vec![
    fuzz::bytes_of(mixed.words()),
    fuzz::bytes_of(lengths.words()),
    fuzz::bytes_of(numbers.words()),
    fuzz::bytes_of(&table_words(&COLUMNS_SIMPLE)),
  ]
}

#[test]
#[ignore = "a fuzz run: cargo xtask fuzz"]
fn fuzz_property_stream() {
  let seeds = property_seeds();
  fuzz::run("property_stream", &seeds, property_stream);
}

// ---- Table and hash map descriptions ----

/// The words as a table description and as a hash map description, and
/// what a reader of either asks of the result.
fn table_info(input: &[u8]) {
  let mut words: Vec<u32> = Vec::new();
  fuzz::words_of(input, &mut words);
  if let Ok(table) = parse_table_info(&words) {
    let _ = table.database();
    let _ = table.table_name();
    let _ = table.version_upper();
    let _ = table.version_lower();
    let _ = table.fragments_with_nodes();
    let _ = table.nodes_of_fragment(0);
    let _ = table.nodes_of_fragment(table.fragment_count);
    for attr in &table.attributes {
      let _ = attr.max_byte_size();
      let _ = attr.is_blob();
    }
  }
  if let Ok(map) = parse_hash_map_info(&words) {
    let _ = map.fragment_count();
    let _ = map.fragment_of(u32::MAX);
  }
}

/// A column of a table description seed.
struct Column {
  name: &'static str,
  type_code: u32,
  length: u32,
  precision: u32,
  scale: u32,
  charset: u32,
  array_kind: u32,
  key: bool,
  nullable: bool,
}

/// `id INT` key, `name VARCHAR(20)` key, `t TINYINT NULL`,
/// `big BIGINT NULL`, `b BIT(10) NULL`, `n INT`: the table of
/// ic_apid's row codec tests.
const COLUMNS_SIMPLE: [Column; 6] = [
  Column {
    name: "id",
    type_code: IC_TYPE_INT,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: true,
    nullable: false,
  },
  Column {
    name: "name",
    type_code: IC_TYPE_VARCHAR,
    length: 20,
    precision: 0,
    scale: 0,
    charset: 8,
    array_kind: IC_ARRAY_KIND_SHORT_VARSIZE,
    key: true,
    nullable: false,
  },
  Column {
    name: "t",
    type_code: IC_TYPE_TINYINT,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "big",
    type_code: IC_TYPE_BIGINT,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "b",
    type_code: IC_TYPE_BIT,
    length: 10,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "n",
    type_code: IC_TYPE_INT,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: false,
  },
];

/// Types whose size takes a precision, a scale or a multiplier: a
/// `BIGINT UNSIGNED` key, `DECIMAL(10,2)`, `CHAR(10)`,
/// `VARCHAR(1000)` stored long, `TIMESTAMP(3)`, `BLOB`,
/// `DATETIME(6)`, `YEAR`, `MEDIUMINT` and `TIME`.
const COLUMNS_SIZED: [Column; 10] = [
  Column {
    name: "k",
    type_code: IC_TYPE_BIGUNSIGNED,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: true,
    nullable: false,
  },
  Column {
    name: "d",
    type_code: IC_TYPE_DECIMAL,
    length: 1,
    precision: 10,
    scale: 2,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "c",
    type_code: IC_TYPE_CHAR,
    length: 10,
    precision: 0,
    scale: 0,
    charset: 8,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "lv",
    type_code: IC_TYPE_LONGVARCHAR,
    length: 1000,
    precision: 0,
    scale: 0,
    charset: 8,
    array_kind: IC_ARRAY_KIND_LONG_VARSIZE,
    key: false,
    nullable: true,
  },
  Column {
    name: "ts",
    type_code: IC_TYPE_TIMESTAMP2,
    length: 1,
    precision: 3,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: false,
  },
  Column {
    name: "bl",
    type_code: IC_TYPE_BLOB,
    length: 1,
    precision: 256,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_LONG_VARSIZE,
    key: false,
    nullable: true,
  },
  Column {
    name: "dt",
    type_code: IC_TYPE_DATETIME2,
    length: 1,
    precision: 6,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "y",
    type_code: IC_TYPE_YEAR,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "mi",
    type_code: IC_TYPE_MEDIUMINT,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
  Column {
    name: "tm",
    type_code: IC_TYPE_TIME,
    length: 1,
    precision: 0,
    scale: 0,
    charset: 0,
    array_kind: IC_ARRAY_KIND_FIXED,
    key: false,
    nullable: true,
  },
];

/// A table description as a data node sends it, with these columns, a
/// hash map, replicas for two fragments and, on the last column, a
/// default value.
fn table_words(columns: &[Column]) -> Vec<u32> {
  let mut w = PropertyWriter::new();
  w.add_string(IC_TP_TABLE_NAME, "ictest/def/t1");
  w.add_u32(IC_TP_TABLE_ID, 17);
  w.add_u32(IC_TP_TABLE_VERSION, 16_777_217);
  w.add_u32(IC_TP_TABLE_KIND, IC_OBJECT_KIND_TABLE);
  let mut keys: u32 = 0;
  let mut nullable: u32 = 0;
  for column in columns {
    keys += column.key as u32;
    nullable += column.nullable as u32;
  }
  w.add_u32(IC_TP_KEY_COLUMN_COUNT, keys);
  w.add_u32(IC_TP_NULLABLE_COUNT, nullable);
  w.add_u32(IC_TP_COLUMN_COUNT, columns.len() as u32);
  w.add_u32(IC_TP_HASH_MAP_ID, 1);
  w.add_u32(IC_TP_HASH_MAP_VERSION, 7);
  w.add_u32(IC_TP_FRAGMENT_COUNT, 2);
  w.add_u32(IC_TP_READ_BACKUP, 1);
  // Two replicas, two fragments: log part 0 with nodes 1 and 2, log
  // part 1 with nodes 2 and 1, all big-endian.
  let replicas: [u16; 8] = [2, 2, 0, 1, 2, 1, 2, 1];
  let mut bytes: Vec<u8> = Vec::new();
  for value in replicas {
    bytes.extend_from_slice(&value.to_be_bytes());
  }
  w.add_u32(IC_TP_REPLICA_MAP_LEN, bytes.len() as u32);
  w.add_binary(IC_TP_REPLICA_MAP, &bytes);
  let mut id: usize = 0;
  while id < columns.len() {
    let column = &columns[id];
    w.add_string(IC_TP_COLUMN_NAME, column.name);
    w.add_u32(IC_TP_COLUMN_ID, id as u32);
    w.add_u32(IC_TP_COLUMN_TYPE, column.type_code);
    w.add_u32(IC_TP_COLUMN_LENGTH, column.length);
    w.add_u32(IC_TP_COLUMN_IN_KEY, column.key as u32);
    w.add_u32(IC_TP_COLUMN_NULLABLE, column.nullable as u32);
    let precision = (column.charset << 16) | column.precision;
    w.add_u32(IC_TP_COLUMN_PRECISION, precision);
    w.add_u32(IC_TP_COLUMN_SCALE, column.scale);
    w.add_u32(IC_TP_COLUMN_ARRAY_KIND, column.array_kind);
    if id + 1 == columns.len() {
      // A default of four bytes behind its four-byte header.
      let mut value: Vec<u8> = ((2u32 << 16) | 4).to_be_bytes().to_vec();
      value.extend_from_slice(&42u32.to_le_bytes());
      w.add_binary(IC_TP_COLUMN_DEFAULT, &value);
    }
    w.add_u32(IC_TP_COLUMN_END, 0);
    id += 1;
  }
  w.words().to_vec()
}

/// A hash map description with these buckets.
fn hash_map_words(buckets: &[u16]) -> Vec<u32> {
  let mut bytes: Vec<u8> = Vec::new();
  for bucket in buckets {
    bytes.extend_from_slice(&bucket.to_ne_bytes());
  }
  let mut w = PropertyWriter::new();
  w.add_string(IC_HMP_NAME, "DEFAULT-HASHMAP-4-2");
  w.add_u32(IC_HMP_BUCKETS, (2 * buckets.len()) as u32);
  w.add_binary(IC_HMP_VALUES, &bytes);
  w.add_u32(IC_TP_HASH_MAP_ID, 1);
  w.add_u32(IC_TP_HASH_MAP_VERSION, 7);
  w.words().to_vec()
}

fn table_seeds() -> Vec<Vec<u8>> {
  let mut many = [0u16; 64];
  let mut i: usize = 0;
  while i < many.len() {
    many[i] = (i % 4) as u16;
    i += 1;
  }
  vec![
    fuzz::bytes_of(&table_words(&COLUMNS_SIMPLE)),
    fuzz::bytes_of(&table_words(&COLUMNS_SIZED)),
    fuzz::bytes_of(&table_words(&COLUMNS_SIMPLE[..1])),
    fuzz::bytes_of(&hash_map_words(&[0, 1, 0, 1])),
    fuzz::bytes_of(&hash_map_words(&many)),
  ]
}

#[test]
#[ignore = "a fuzz run: cargo xtask fuzz"]
fn fuzz_table_info() {
  let seeds = table_seeds();
  fuzz::run("table_info", &seeds, table_info);
}

// ---- Signal data ----

/// The words of one input split as a signal brings them: the first word
/// says where, in its low half how many words of data follow and in
/// its high half how many of the rest are section 0; section 1 is what
/// is left.
fn split_signal(words: &[u32]) -> (&[u32], &[u32], &[u32]) {
  if words.is_empty() {
    return (&[], &[], &[]);
  }
  let control = words[0];
  let rest = &words[1..];
  let data_len = (control & 0xFFFF) as usize % (rest.len() + 1);
  let (data, sections) = rest.split_at(data_len);
  let section0_len = (control >> 16) as usize % (sections.len() + 1);
  let (section0, section1) = sections.split_at(section0_len);
  (data, section0, section1)
}

/// Every signal decoder on the same words.
fn signal_data(input: &[u8]) {
  let mut words: Vec<u32> = Vec::new();
  fuzz::words_of(input, &mut words);
  let (data, section0, section1) = split_signal(&words);
  transaction_signals(data, section0);
  scan_signals(data, section0);
  dictionary_signals(data, section0, section1);
  membership_signals(data, section0);
}

fn transaction_signals(data: &[u32], section0: &[u32]) {
  let _ = CoordAllocConf::decode(data);
  let _ = CoordAllocRef::decode(data);
  let _ = CoordFreeConf::decode(data);
  let _ = CoordFreeRef::decode(data);
  if let Ok(conf) = KeyOpConf::decode(data) {
    for op in conf.operations() {
      let _ = op.is_dirty_read();
      let _ = op.reading_node();
    }
    let _ = conf.is_committed();
    let _ = conf.needs_commit_ack();
  }
  let _ = KeyOpRef::decode(data);
  let _ = TransCommitConf::decode(data);
  let _ = TransEndRef::decode(data);
  let _ = RollbackConf::decode(data);
  let _ = TakeoverConf::decode(data);
  let _ = TakeoverRef::decode(data);
  let _ = TransAbortedRep::decode(data);
  let _ = RowDataRep::decode(data);
  let _ = RowDataRep::row(data, None);
  let _ = RowDataRep::row(data, Some(section0));
}

fn scan_signals(data: &[u32], section0: &[u32]) {
  for extended in [false, true] {
    if let Ok(conf) = ScanConf::decode(data, None, extended) {
      look_at_scan_conf(&conf);
    }
    if let Ok(conf) = ScanConf::decode(data, Some(section0), extended) {
      look_at_scan_conf(&conf);
    }
  }
  let _ = ScanRef::decode(data);
  if let Ok(rep) = ScanRowKeyRep::decode(data) {
    let _ = rep.fragment();
    let _ = rep.op_number();
    let _ = scans::takeover_info(rep.row_word);
  }
  let _ = ScanRowKeyRep::key(data, None);
  let _ = ScanRowKeyRep::key(data, Some(section0));
}

fn look_at_scan_conf(conf: &ScanConf) {
  for batch in conf.batches() {
    let _ = batch.is_done();
  }
  let _ = conf.end_of_data();
  let _ = conf.confirms_close();
}

fn dictionary_signals(data: &[u32], section0: &[u32], section1: &[u32]) {
  let _ = ListObjectsConf::decode(data);
  if let Ok(objects) = list_objects::parse_listed_objects(section0, section1) {
    for object in &objects {
      let _ = object.short_name();
    }
  }
  let _ = list_objects::parse_listed_objects(section0, &[]);
  let _ = TableDescConf::decode(data);
  let _ = TableDescRef::decode(data);
  if let Ok(rep) = TableChangedRep::decode(data, section0) {
    let _ = rep.is_drop();
  }
  if let Ok(conf) = CollationDescConf::decode(data, section0) {
    let _ = conf.is_no_pad();
    let _ = conf.has_more();
    let mut runs: Vec<WeightRun> = Vec::new();
    let mut contractions: Vec<Contraction> = Vec::new();
    if conf.weights(section1, &mut runs, &mut contractions).is_ok() {
      // The last code point of each run, the one furthest stepped.
      let mut out: Vec<u8> = Vec::new();
      for run in &runs {
        run.weight_of(run.count - 1, &mut out);
        run.encoded_of(run.count - 1, &mut out);
      }
    }
  }
  if let Ok(refusal) = CollationDescRef::decode(data) {
    let _ = refusal.is_temporary();
  }
}

fn membership_signals(data: &[u32], section0: &[u32]) {
  if let Ok(state) = NodeState::decode(data) {
    let _ = state.start_level.is_started();
    let _ = state.connected_node_ids();
  }
  if let Ok(conf) = HeartbeatConf::decode(data) {
    let _ = conf.heartbeat_interval_ms();
    let _ = conf.node_state.is_connected_to(u32::MAX);
  }
  if let Ok(refusal) = HeartbeatRef::decode(data) {
    let _ = refusal.reason();
  }
  let _ = NodeLostRep::decode(data, section0);
  let _ = NodeLostDoneRep::decode(data);
}

/// A seed for the signal decoders: the word that splits the rest as
/// [`split_signal`] reads it, then the data and the two sections.
fn signal_seed(data: &[u32], section0: &[u32], section1: &[u32]) -> Vec<u8> {
  let control = data.len() as u32 | ((section0.len() as u32) << 16);
  let mut words: Vec<u32> = vec![control];
  words.extend_from_slice(data);
  words.extend_from_slice(section0);
  words.extend_from_slice(section1);
  fuzz::bytes_of(&words)
}

/// Bytes as the words that carry them, the last padded with zeros.
fn padded_words(bytes: &[u8]) -> Vec<u32> {
  let mut padded: Vec<u8> = bytes.to_vec();
  padded.resize(bytes.len().next_multiple_of(4), 0);
  let mut words: Vec<u32> = Vec::new();
  fuzz::words_of(&padded, &mut words);
  words
}

/// Names as section 1 of a list of objects carries them: for each, its
/// length in bytes with its NUL, then its bytes padded to words.
fn listed_names(names: &[&str]) -> Vec<u32> {
  let mut words: Vec<u32> = Vec::new();
  for name in names {
    let mut bytes: Vec<u8> = name.as_bytes().to_vec();
    bytes.push(0);
    words.push(bytes.len() as u32);
    words.extend_from_slice(&padded_words(&bytes));
  }
  words
}

/// The nineteen words of a collation's description, with these name
/// lengths and these counts of weights.
fn collation_words(
  charset_len: u32,
  collation_len: u32,
  counts: [u32; 5],
) -> Vec<u32> {
  let mut data: Vec<u32> =
    vec![9, 8, 8, 47, 0x1, 1, 1, 1, 0x0101, 0x20, 1, 0, 255];
  data.push(charset_len | (collation_len << 16));
  data.extend_from_slice(&counts);
  data
}

/// Seeds of the transaction signals: what a coordinator answers.
fn transaction_seeds() -> Vec<Vec<u8>> {
  let lo: u32 = 0x1234;
  let hi: u32 = 0x1;
  let coord = blocks::number_to_ref(blocks::IC_BLOCK_COORDINATOR, 1);
  let dirty = IC_KEY_OP_CONF_DIRTY_READ_BIT | 2;
  // Committed, a marker kept, three operations, and the low word of the
  // global checkpoint at the end.
  let info = (3 << 16) | 3;
  let conf = [0x10, 5, info, lo, hi, 0x20, 4, 0x21, 6, 0x22, dirty, 9];
  // A packed row: every column read, after its header and its mask.
  let row = [(0xFFF3 << 16) | 4, 0x1FF, 7, 0x0069_6802, 12, 42];
  let mut short_row: Vec<u32> = vec![0x20, lo, hi];
  short_row.extend_from_slice(&row);
  vec![
    signal_seed(&[7, 0x40, coord], &[], &[]),
    signal_seed(&[7, 4010], &[], &[]),
    signal_seed(&[7], &[], &[]),
    signal_seed(&[7, 4012, 1234], &[], &[]),
    signal_seed(&conf, &[], &[]),
    signal_seed(&[0x10, 0, 1, lo, hi, 0x20, 3], &[], &[]),
    signal_seed(&[0x20, lo, hi, 626, 0], &[], &[]),
    signal_seed(&[0x10 | 1, lo, hi, 5, 0x77], &[], &[]),
    signal_seed(&[0x10, lo, hi, 4012, 3], &[], &[]),
    signal_seed(&[0x10, lo, hi], &[], &[]),
    signal_seed(&[0x10, lo, hi, 266, 0], &[], &[]),
    signal_seed(&short_row, &[], &[]),
    signal_seed(&[0x20, lo, hi], &row, &[]),
  ]
}

/// Seeds of the scan signals: batches in each form, the end, a refusal
/// and the key of a locked row.
fn scan_seeds() -> Vec<Vec<u8>> {
  let lo: u32 = 0x1234;
  let hi: u32 = 0x1;
  let done = IC_SCAN_RECEIVER_DONE;
  let end = IC_SCAN_CONF_END_OF_DATA;
  let row_word = (3 << 20) | (5 << 8) | 1;
  // Ten rows in 100 words; then more than 1 023 rows, in a fourth word.
  let short = [0x30, 1, lo, hi, 0, 0x500, (100 << 10) | 10];
  let long = [0x30, 1, lo, hi, 0, 0x500, (5000 << 10) | 0x3FF, 2000];
  let extended = [0, 0x500, 10, 100, 0xF, 1, done, 0, 0, 0];
  let older = [0, 0x500, 10, 100, 1, done, 0, 0];
  vec![
    signal_seed(&short, &[], &[]),
    signal_seed(&long, &[], &[]),
    signal_seed(&[0x30, end | 2, lo, hi], &extended, &[]),
    signal_seed(&[0x30, 2, lo, hi], &older, &[]),
    signal_seed(&[0x30, end, lo, hi], &[], &[]),
    signal_seed(&[0x30, lo, hi, 274, 1], &[], &[]),
    signal_seed(&[0, 2, row_word, lo, hi, 0xAA, 0xBB], &[], &[]),
    signal_seed(&[0, 2, row_word, lo, hi], &[0xAA, 0xBB], &[]),
  ]
}

/// Seeds of the dictionary's answers.
fn dictionary_seeds() -> Vec<Vec<u8>> {
  let dict = blocks::number_to_ref(blocks::IC_BLOCK_DICTIONARY, 1);
  // Two objects, online and logged, with their names.
  let flags = (4 << 24) | (1 << 20);
  let objects = [flags, 17, 2, flags, 18, 3];
  let names = listed_names(&["ictest/def/t1", "sys/def/17/uk$unique"]);
  // A table's name, padded with NULs to 128 bytes.
  let mut changed = table_desc::name_section("ictest/def/t1");
  changed.resize(32, 0);
  let all_sent = [0, 0, 0, 0, IC_COLLATION_ALL_SENT];
  let latin1 = collation_words(6, 17, all_sent);
  let latin1_names = padded_words(b"latin1\0latin1_swedish_ci\0");
  // Two runs of weights, then one contraction, as section 1.
  let mut weights: Vec<u8> = vec![0, 0, 0x61, 0, 0, 3, 0, 1, 0, 2];
  weights.extend_from_slice(&[0x61, 0x1C, 0x47]);
  weights.extend_from_slice(&[0, 0, 0x20, 0, 0, 1, 0xFF, 1, 0, 2]);
  weights.extend_from_slice(&[0x20, 0x02, 0x09]);
  let weights_len = weights.len() as u32;
  weights.resize(weights.len().next_multiple_of(4), 0);
  let contraction: [u8; 12] = [2, 0, 0, 2, 0, 0, 0x63, 0, 0, 0x68, 0x1C, 0x60];
  weights.extend_from_slice(&contraction);
  let counts = [2, weights_len, 1, contraction.len() as u32, 0x11_0001];
  let bin = collation_words(6, 10, counts);
  let bin_names = padded_words(b"latin1\0latin1_bin\0");
  vec![
    signal_seed(&[9, 2], &objects, &names),
    signal_seed(&[9, 17, 0, 120, 2, dict], &[], &[]),
    signal_seed(&[9, 0x40, 1, 0, 0, 723, 1234], &[], &[]),
    signal_seed(&[9, 0x40, 1, 0, 723], &[], &[]),
    signal_seed(&[17, 3, 1], &changed, &[]),
    signal_seed(&latin1, &latin1_names, &[]),
    signal_seed(&bin, &bin_names, &padded_words(&weights)),
    signal_seed(&[9, 9, 0, 8], &[], &[]),
  ]
}

/// Seeds of what a data node says about itself and the cluster.
fn membership_seeds() -> Vec<Vec<u8>> {
  let membership = blocks::number_to_ref(blocks::IC_BLOCK_MEMBERSHIP, 1);
  let state = NodeState {
    start_level: StartLevel::Started,
    dynamic_id: 1,
    connected_nodes: [0b110, 0, 0, 0, 0, 0, 0, 0],
    ..NodeState::default()
  };
  let heartbeat = HeartbeatConf {
    membership_ref: membership,
    version: 0x1A_0A00,
    api_heartbeat_interval: 150,
    mysql_version: 0x0009_0100,
    min_db_version: 0x1A_0A00,
    min_api_version: 0x1A_0A00,
    node_state: state,
  };
  let words = heartbeat.encode();
  // Node 2 lost, in the data or in a section.
  let bitmap = [0b100, 0, 0, 0, 0, 0, 0, 0];
  let mut lost: Vec<u32> = vec![1, 1, 2];
  lost.extend_from_slice(&bitmap);
  let coordinator = blocks::IC_BLOCK_COORDINATOR as u32;
  vec![
    signal_seed(&words, &[], &[]),
    signal_seed(&words[5..21], &[], &[]),
    signal_seed(&[membership, 0x1A_0A00, 2, 0x0009_0100], &[], &[]),
    signal_seed(&lost, &[], &[]),
    signal_seed(&[1, 1, 2], &bitmap, &[]),
    signal_seed(&[coordinator, 68, 2, 0, 1], &[], &[]),
  ]
}

#[test]
#[ignore = "a fuzz run: cargo xtask fuzz"]
fn fuzz_signal_data() {
  let mut seeds = transaction_seeds();
  seeds.extend(scan_seeds());
  seeds.extend(dictionary_seeds());
  seeds.extend(membership_seeds());
  fuzz::run("signal_data", &seeds, signal_data);
}
