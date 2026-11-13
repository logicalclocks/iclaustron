// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! `ic_read`: read one row by its primary key, as a committed read.
//!
//! ```text
//!   ic_read -c localhost:1186 -d ictest t9 1
//!   ic_read -c localhost:1186 -d ictest t9 1 --debug-level 1024
//!   ic_read -c localhost:1186 -d ictest t9 1 --partition
//!   ic_read -c localhost:1186 -d ictest t9 --index uk 10
//! ```
//!
//! After the table come the key's values, one per primary key column
//! in the table's column order. The table is bound through the
//! dictionary cache, the default record is made over it, and the row is
//! read into it and printed a column at a time.
//!
//! Key columns may be integers, or character and binary strings. The
//! row may hold any type a record can: numbers and character strings
//! are printed as such, anything else as its bytes in hexadecimal.
//! Character strings are converted from their column's character set
//! when it is utf8mb4, utf8mb3, utf16, utf16le, ucs2, utf32 or latin1;
//! a column of another set is given and printed in hexadecimal, `0x`
//! and two digits a byte.
//!
//! Integers are put in the key, and read from the row, in little-endian
//! order: the data node's own, on the machines RonDB runs on.
//!
//! `--index` reads through a unique index instead: the values are then
//! the indexed columns', in the index's order. A unique key made by a
//! MySQL server is two indexes, an ordered one under its own name and
//! the hash one under the name with `$unique` after it; either name
//! does here.
//!
//! `--partition` also works out which partition the key belongs to,
//! hashing it as the data nodes do, and asks a data node which
//! partition the row is really in, so that the two can be compared.
//!
//! Debug level 1024 traces every signal, which shows the allocate of a
//! transaction record, the request, and the replies, packed or not.
//!
//! **Blob columns.** By primary key the row is read through a query on
//! a transaction of its own, which reads the blob columns too: each is
//! printed as its length and its first bytes, and the read's rounds
//! after its first are counted (doc/rust/06, phase 12). A committed
//! read takes one round for inline values and three for values with
//! parts; `--lock-first` has it take the lock in its first round, two
//! rounds whatever the values; `--locked` reads with a shared lock held
//! to the commit. Through `--index` the blob columns are left out.
//!
//! ```text
//!   ic_read -c localhost:1186 -d ictest ttext 1
//!   ic_read -c localhost:1186 -d ictest ttext 1 --lock-first
//!   ic_read -c localhost:1186 -d ictest ttext 1 --locked
//! ```

use std::sync::Arc;

use ic_apic::mgm_client;
use ic_apid::apid_conn::ApidConnection;
use ic_apid::apid_global::ApidGlobal;
use ic_apid::dict_cache::IndexDef;
use ic_apid::dict_cache::TableDef;
use ic_apid::hash;
use ic_apid::key_op;
use ic_apid::query::QueryId;
use ic_apid::query::ReadKeyArgs;
use ic_apid::query::ReadKind;
use ic_apid::query::TransId;
use ic_apid::record::Record;
use ic_apid::text_row;
use ic_apid::transaction::CommitState;
use ic_port::options::OptionEntry;
use ic_port::options::OptionKind;
use ic_port::options::OptionParser;
use ic_port::IcError;

/// How long to wait for a read's transaction to finish.
const IC_READ_WAIT_MS: u64 = 10_000;
/// How many of a blob value's bytes are printed.
const IC_READ_BLOB_SHOWN: usize = 32;

const OPTIONS: [OptionEntry; 8] = [
  OptionEntry {
    long_name: "ndb-connectstring",
    short_name: b'c',
    kind: OptionKind::Str,
    help: "Management servers, as host:port,host:port",
  },
  OptionEntry {
    long_name: "database",
    short_name: b'd',
    kind: OptionKind::Str,
    help: "Database the table is in; the default is test",
  },
  OptionEntry {
    long_name: "node-id",
    short_name: 0,
    kind: OptionKind::Int,
    help: "Node id to ask for; 0 lets the server choose",
  },
  OptionEntry {
    long_name: "debug-level",
    short_name: 0,
    kind: OptionKind::Int,
    help: "Debug level bits; 1024 traces every signal",
  },
  OptionEntry {
    long_name: "partition",
    short_name: b'p',
    kind: OptionKind::Flag,
    help: "Also compare the key's computed partition with the row's",
  },
  OptionEntry {
    long_name: "index",
    short_name: b'i',
    kind: OptionKind::Str,
    help: "Read through this unique index; the values are its columns'",
  },
  OptionEntry {
    long_name: "lock-first",
    short_name: 0,
    kind: OptionKind::Flag,
    help: "A committed read of blob values takes its lock first",
  },
  OptionEntry {
    long_name: "locked",
    short_name: 0,
    kind: OptionKind::Flag,
    help: "Read with a shared lock, held to the commit",
  },
];

/// How a read by primary key goes.
#[derive(Clone, Copy)]
struct How {
  /// A shared lock held to the commit, not a committed read.
  locked: bool,
  /// A committed read of blob values takes its lock in its first round.
  lock_first: bool,
}

fn main() {
  std::process::exit(run());
}

fn run() -> i32 {
  let mut parser =
    OptionParser::new("ic_read", "Read one row of a table by its key.");
  parser.add_entries(&OPTIONS);
  if let Err(e) = parser.parse_env_args() {
    if e.code == ic_port::err::IC_ERROR_HELP_REQUESTED {
      return 0;
    }
    return 1;
  }
  let debug_level = parser.get_int_or("debug-level", 0) as u32;
  ic_port::debug::set_level(debug_level);
  ic_port::debug::set_screen(true);
  // Seconds since the start on every line, so that a pause can be seen.
  ic_port::debug::set_timestamp(debug_level != 0);
  let args: Vec<String> = parser.positional().to_vec();
  if args.len() < 2 {
    println!("Name a table and its key, for example: ic_read -d test t1 7");
    return 1;
  }
  let database = parser.get_string_or("database", "test");
  let connect_text =
    parser.get_string_or("ndb-connectstring", "localhost:1186");
  let mut connect_string = match ic_util::connectstring::parse(&connect_text) {
    Ok(cs) => cs,
    Err(e) => {
      report("Could not read the connectstring", &e);
      return 1;
    }
  };
  let wanted = parser.get_int_or("node-id", 0) as u32;
  if wanted != 0 {
    connect_string.node_id = Some(wanted);
  }
  let fetched =
    mgm_client::fetch_configuration(&connect_string, 30_000, Some("ic_read"));
  let (config, mgm) = match fetched {
    Ok(pair) => pair,
    Err(e) => {
      report("Could not fetch the configuration", &e);
      return 1;
    }
  };
  let mut global = match ApidGlobal::start(
    config,
    mgm,
    connect_string,
    30_000,
    Some("ic_read"),
  ) {
    Ok(global) => global,
    Err(e) => {
      report("Could not start the Data API threads", &e);
      return 1;
    }
  };
  let partition = parser.get_flag("partition");
  let index = parser.get_string_or("index", "");
  let how = How {
    locked: parser.get_flag("locked"),
    lock_first: parser.get_flag("lock-first"),
  };
  if !index.is_empty() && (how.locked || how.lock_first) {
    println!("--locked and --lock-first are for reads by primary key");
    global.stop();
    return 1;
  }
  let code = read_row(
    &global,
    &database,
    &args[0],
    &args[1..],
    partition,
    &index,
    how,
  );
  global.stop();
  code
}

fn read_row(
  global: &ApidGlobal,
  database: &str,
  table: &str,
  keys: &[String],
  partition: bool,
  index_name: &str,
  how: How,
) -> i32 {
  if global.wait_for_first_started(15_000) == 0 {
    println!("No data node is started, so there is nobody to ask");
    return 1;
  }
  let mut conn = match global.create_connection() {
    Ok(conn) => conn,
    Err(e) => {
      report("Could not make a connection", &e);
      return 1;
    }
  };
  let def = match conn.table_bind(database, table) {
    Ok(def) => def,
    Err(e) => {
      report("Could not bind the table", &e);
      return 1;
    }
  };
  let rec = match Record::default_for(&def, None) {
    Ok(rec) => rec,
    Err(e) => {
      report("Could not make a record", &e);
      return 1;
    }
  };
  let mut index: Option<Arc<IndexDef>> = None;
  let mut key_names: Vec<String> = Vec::new();
  if index_name.is_empty() {
    for attr in &def.info().attributes {
      if attr.primary_key {
        key_names.push(attr.name.clone());
      }
    }
  } else {
    let bound = match bind_unique(&mut conn, database, index_name, table) {
      Ok(bound) => bound,
      Err(e) => {
        report("Could not bind the index", &e);
        return 1;
      }
    };
    for column in &bound.info().attributes {
      if column.primary_key {
        key_names.push(column.name.clone());
      }
    }
    index = Some(bound);
  }
  let mut key_row = vec![0u8; rec.row_size() as usize];
  if let Err(text) = put_key(&rec, &key_names, keys, &mut key_row) {
    println!("{}", text);
    return 1;
  }
  let mut row = vec![0u8; rec.row_size() as usize];
  let read = match &index {
    Some(index) => {
      let read = key_op::read_unique_committed(
        &mut conn, index, &rec, &key_row, &rec, &mut row,
      );
      if let Ok(true) = read {
        print_row(&def, &rec, &row);
      }
      read
    }
    None => read_by_query(&mut conn, &def, &rec, &key_row, how),
  };
  let mut code = match read {
    Ok(true) => 0,
    Ok(false) => {
      println!("No row with that key");
      1
    }
    Err(e) => {
      report("Could not read the row", &e);
      1
    }
  };
  if partition && code == 0 && index.is_none() {
    code = compare_partition(&mut conn, &def, &rec, &key_row);
  }
  if conn.unexpected() > 0 {
    println!(
      "{} signal(s) came that nothing waited for",
      conn.unexpected()
    );
  }
  code
}

/// The row read by its primary key through a query on a transaction of
/// its own, every column read, the blob columns too, and printed. The
/// key record is the default one, which leaves the blob columns out, as
/// a key record must. False if there is no such row.
fn read_by_query(
  conn: &mut ApidConnection,
  def: &Arc<TableDef>,
  key_rec: &Record,
  key_row: &[u8],
  how: How,
) -> Result<bool, IcError> {
  let mut ids: Vec<u32> = Vec::with_capacity(def.info().attributes.len());
  for attr in &def.info().attributes {
    ids.push(attr.column_id);
  }
  let attr_rec = Record::default_for(def, Some(ids.as_slice()))?;
  let id = conn.create_query(def, key_rec, &attr_rec)?;
  let read = read_on_query(conn, def, key_rec, &attr_rec, key_row, how, id);
  let _ = conn.free_query(id);
  read
}

/// [`read_by_query`]'s read, on the query `id`.
fn read_on_query(
  conn: &mut ApidConnection,
  def: &Arc<TableDef>,
  key_rec: &Record,
  attr_rec: &Record,
  key_row: &[u8],
  how: How,
  id: QueryId,
) -> Result<bool, IcError> {
  match conn.query_mut(id) {
    Some(query) => {
      query.key_row_mut().copy_from_slice(key_row);
      if how.lock_first {
        query.set_blob_lock_first(true)?;
      }
    }
    None => return Err(IcError::new(ic_port::err::IC_ERROR_NO_SUCH_FIELD)),
  }
  let hint = conn.hint_for_key(def, key_rec, key_row);
  let trans = conn.start_transaction(hint, Some(def))?;
  let rounds_before = conn.blob_rounds();
  let kind = if how.locked {
    ReadKind::Locked
  } else {
    ReadKind::Committed
  };
  let args = ReadKeyArgs {
    kind,
    ..ReadKeyArgs::default()
  };
  let mut outcome = conn.read_key(id, trans, &args);
  if outcome.is_ok() {
    outcome = conn.commit_transaction(trans);
  }
  if outcome.is_ok() && wait_for(conn, trans).is_none() {
    outcome = Err(IcError::new(ic_port::err::IC_ERROR_TIMEOUT));
  }
  while conn.get_next_executed_query().is_some() {}
  let rounds = conn.blob_rounds() - rounds_before;
  let read = match outcome {
    Ok(()) => match conn.query(id) {
      Some(query) => match query.error() {
        Some(e) if e.code == key_op::IC_NDB_ERROR_NO_SUCH_ROW as i32 => {
          Ok(false)
        }
        Some(e) => Err(e),
        None => {
          print_query_row(def, attr_rec, query);
          Ok(true)
        }
      },
      None => Err(IcError::new(ic_port::err::IC_ERROR_NO_SUCH_FIELD)),
    },
    Err(e) => Err(e),
  };
  if attr_rec.has_blob_fields() {
    println!("{} round(s) after the first", rounds);
  }
  let _ = conn.close_transaction(trans);
  read
}

/// Send, then poll until the transaction is done or the wait runs out.
fn wait_for(conn: &mut ApidConnection, trans: TransId) -> Option<CommitState> {
  let start = ic_port::time::gethrtime();
  loop {
    if let Err(e) = conn.flush(100, false) {
      report("Could not send", &e);
    }
    if let Some(t) = conn.transaction(trans) {
      if t.is_done() {
        return Some(t.commit_state());
      }
    }
    let waited =
      ic_port::time::millis_elapsed(start, ic_port::time::gethrtime());
    if waited >= IC_READ_WAIT_MS {
      return None;
    }
  }
}

/// Each column of a query's row, a line each; a blob column as its
/// length and first bytes.
fn print_query_row(
  def: &Arc<TableDef>,
  rec: &Record,
  query: &ic_apid::query::ApidQuery,
) {
  let mut position: u32 = 0;
  while position < rec.num_fields() {
    if let Some(field) = rec.field(position) {
      if let Some(attr) = def.field(field.field_id()) {
        let text = if field.is_blob() {
          match query.blob(position) {
            Ok(value) => blob_text(value),
            Err(e) => format!("{} ({})", e.message(), e.code),
          }
        } else {
          text_row::value_text(rec, position, query.attr_row())
        };
        println!("{}: {}", attr.name, text);
      }
    }
    position += 1;
  }
}

/// A blob value as its length and its first bytes, a byte that is not
/// printable as a dot.
fn blob_text(value: Option<&[u8]>) -> String {
  let bytes = match value {
    Some(bytes) => bytes,
    None => return text_row::IC_NULL_TEXT.to_string(),
  };
  let mut shown = String::with_capacity(IC_READ_BLOB_SHOWN);
  for byte in &bytes[..bytes.len().min(IC_READ_BLOB_SHOWN)] {
    if byte.is_ascii_graphic() || *byte == b' ' {
      shown.push(*byte as char);
    } else {
      shown.push('.');
    }
  }
  let more = if bytes.len() > IC_READ_BLOB_SHOWN {
    "..."
  } else {
    ""
  };
  format!("{} bytes: {}{}", bytes.len(), shown, more)
}

/// The unique hash index of the name given: the name itself if that is
/// one, else the name with `$unique` after it, which is what a MySQL
/// server calls the hash half of a unique key.
fn bind_unique(
  conn: &mut ApidConnection,
  database: &str,
  index_name: &str,
  table: &str,
) -> Result<Arc<IndexDef>, IcError> {
  let bound = conn.index_bind(database, index_name, table)?;
  if bound.is_unique() {
    return Ok(bound);
  }
  let hash_name = format!("{}$unique", index_name);
  let bound = conn.index_bind(database, &hash_name, table)?;
  if !bound.is_unique() {
    return Err(IcError::new(ic_port::err::IC_ERROR_NOT_SUPPORTED));
  }
  Ok(bound)
}

/// The partition the key hashes to here against the one the data node
/// keeps the row in.
fn compare_partition(
  conn: &mut ApidConnection,
  def: &Arc<TableDef>,
  rec: &Record,
  key_row: &[u8],
) -> i32 {
  let computed = match hash::partition_of(def, rec, key_row) {
    Ok(partition) => partition,
    Err(e) => {
      report("Could not work out the partition", &e);
      return 1;
    }
  };
  let actual = match key_op::read_partition(conn, rec, key_row) {
    Ok(Some(partition)) => partition,
    Ok(None) => {
      println!("The row went away before its partition was read");
      return 1;
    }
    Err(e) => {
      report("Could not read the row's partition", &e);
      return 1;
    }
  };
  let mut held: Vec<String> = Vec::new();
  for node in def.info().nodes_of_fragment(computed) {
    held.push(node.to_string());
  }
  println!(
    "Partition: computed {} (held by node(s) {}), the data node says {}{}",
    computed,
    held.join(", "),
    actual,
    if computed == actual {
      ""
    } else {
      " -- THEY DIFFER"
    }
  );
  if computed == actual {
    0
  } else {
    1
  }
}

/// Put the key's values into the named columns of `row`, in order.
fn put_key(
  rec: &Record,
  names: &[String],
  keys: &[String],
  row: &mut [u8],
) -> Result<(), String> {
  let mut used: usize = 0;
  for name in names {
    if used >= keys.len() {
      return Err(format!("The key needs a value for {}", name));
    }
    let attr_id = match rec.table().field_id(name) {
      Ok(attr_id) => attr_id,
      Err(_) => return Err(format!("No column {}", name)),
    };
    let position = match rec.position_of(attr_id) {
      Some(position) => position,
      None => return Err(format!("No field for {}", name)),
    };
    text_row::put_value(rec, position, &keys[used], row)?;
    used += 1;
  }
  if used != keys.len() {
    return Err(format!(
      "The key has {} column(s), not {}",
      used,
      keys.len()
    ));
  }
  Ok(())
}

/// Each column of the row, a line each.
fn print_row(def: &Arc<TableDef>, rec: &Record, row: &[u8]) {
  let mut position: u32 = 0;
  while position < rec.num_fields() {
    if let Some(field) = rec.field(position) {
      if let Some(attr) = def.field(field.field_id()) {
        println!(
          "{}: {}",
          attr.name,
          text_row::value_text(rec, position, row)
        );
      }
    }
    position += 1;
  }
}

fn report(what: &str, error: &IcError) {
  println!("{}: {} ({})", what, error.message(), error.code);
  if mgm_client::is_refusal(error.code) {
    println!("The management server said: {}", mgm_client::last_refusal());
  }
}
