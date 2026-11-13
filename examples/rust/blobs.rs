// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! blobs: a TEXT and a LONGBLOB written, read and deleted in the three
//! ways a program runs key operations: synchronously, the values in a
//! holder; on a connection, a query holding them, by primary key and
//! through the unique index; and through an executor, each operation
//! holding its own. Every result is checked, and the run ends with "all
//! checks passed".
//!
//! ```text
//! cargo run -p ic_examples --example blobs -- [connectstring]
//! ```

use ic_apid::blob::BlobValues;
use ic_apid::executor::Executor;
use ic_apid::executor::ExecutorOptions;
use ic_apid::executor::KeyOp;
use ic_apid::executor::OpKind;
use ic_apid::executor::OpShape;
use ic_apid::executor::Request;
use ic_apid::key_op;
use ic_apid::query::ReadKeyArgs;
use ic_apid::query::ReadKind;
use ic_apid::query::WriteKeyArgs;
use ic_apid::query::WriteKind;
use ic_apid::record::Record;
use ic_examples::check;
use ic_examples::committed;
use ic_examples::connect;
use ic_examples::exit_on;
use ic_examples::put_u32;
use ic_examples::EX_DB;
use ic_port::err;
use ic_port::IcError;

/// The table: `id` its key, `code` unique, `doc TEXT`, `data LONGBLOB`.
const EX_T4: &str = "t4";
/// The hash index a MySQL server makes for UNIQUE KEY t4_code.
const EX_T4_INDEX: &str = "t4_code$unique";

/// A value of `len` bytes that differs from `seed` to `seed`, letters
/// for a TEXT.
fn value(len: usize, seed: u32, text: bool) -> Vec<u8> {
  let mut bytes = Vec::with_capacity(len);
  let mut i: u32 = 0;
  while bytes.len() < len {
    let n = i.wrapping_mul(2_654_435_761).wrapping_add(seed) >> 13;
    if text {
      bytes.push(b'a' + (n % 26) as u8);
    } else {
      bytes.push(n as u8);
    }
    i += 1;
  }
  bytes
}

/// A field's position in a record: blob values are named by it.
fn position(rec: &Record, field_id: u32) -> Result<u32, IcError> {
  match rec.position_of(field_id) {
    Some(position) => Ok(position),
    None => Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
  }
}

fn run(args: &[String]) -> Result<(), IcError> {
  let (global, mut conn) = connect(args, "ex_blobs")?;
  let table = conn.table_bind(EX_DB, EX_T4)?;
  let id = table.field_id("id")?;
  let code = table.field_id("code")?;
  let doc = table.field_id("doc")?;
  let data = table.field_id("data")?;
  // A blob field takes no room in the row: its value, NULL included,
  // is kept beside it, by the query, the operation or a holder.
  let key_rec = Record::default_for(&table, Some(&[id]))?;
  let rec = Record::default_for(&table, Some(&[id, code, doc, data]))?;
  let values_rec = Record::default_for(&table, Some(&[doc, data]))?;
  let no_query = IcError::new(err::IC_ERROR_NO_SUCH_FIELD);
  let text = value(10_000, 1, true);
  let bytes = value(100_000, 2, false);

  // 1. Synchronously, the values in a holder made for the record: the
  //    write goes in as many rounds as its parts take.
  let mut key = vec![0u8; key_rec.row_size() as usize];
  put_u32(&key_rec, &mut key, id, 1)?;
  let mut row = vec![0u8; rec.row_size() as usize];
  put_u32(&rec, &mut row, id, 1)?;
  put_u32(&rec, &mut row, code, 100)?;
  let mut values = BlobValues::new(&rec);
  values.set_blob(position(&rec, doc)?, &text)?;
  values.set_blob(position(&rec, data)?, &bytes)?;
  key_op::write_key_blobs(
    &mut conn,
    WriteKind::Write,
    &key_rec,
    &key,
    &rec,
    &row,
    &mut values,
  )?;
  values.clear();
  let found = key_op::read_committed_blobs(
    &mut conn,
    &key_rec,
    &key,
    &rec,
    &mut row,
    &mut values,
  )?;
  check(found, "row 1 is there")?;
  let read = values.blob(position(&rec, doc)?)?;
  check(read == Some(text.as_slice()), "the TEXT read back")?;
  let read = values.blob(position(&rec, data)?)?;
  check(read == Some(bytes.as_slice()), "the LONGBLOB read back")?;
  println!("written and read back: {} and {}", text.len(), bytes.len());

  // 2. On a connection: an update by primary key, the TEXT set NULL
  //    and the LONGBLOB filled in place, then a read through the
  //    unique index on code.
  let at_doc = position(&values_rec, doc)?;
  let at_data = position(&values_rec, data)?;
  let new_bytes = value(50_000, 3, false);
  let updater = conn.create_query(&table, &key_rec, &values_rec)?;
  match conn.query_mut(updater) {
    Some(query) => {
      query.key_row_mut().copy_from_slice(&key);
      query.set_blob_null(at_doc)?;
      let buf = query.blob_buf_mut(at_data)?;
      buf.clear();
      buf.extend_from_slice(&new_bytes);
    }
    None => return Err(no_query),
  }
  let hint = conn.hint_for_key(&table, &key_rec, &key);
  let trans = conn.start_transaction(hint, Some(&table))?;
  let update = WriteKeyArgs {
    kind: WriteKind::Update,
    ..WriteKeyArgs::default()
  };
  conn.write_key(updater, trans, &update)?;
  conn.commit_transaction(trans)?;
  committed(&mut conn, trans, updater)?;

  let index = conn.index_bind(EX_DB, EX_T4_INDEX, EX_T4)?;
  let code_rec = Record::default_for(&table, Some(&[code]))?;
  let reader = conn.create_unique_query(&index, &code_rec, &values_rec)?;
  match conn.query_mut(reader) {
    Some(query) => put_u32(&code_rec, query.key_row_mut(), code, 100)?,
    None => return Err(no_query),
  }
  let hint = match conn.query(reader) {
    Some(query) => conn.hint_for_unique_key(&index, &code_rec, query.key_row()),
    None => return Err(no_query),
  };
  let trans = conn.start_transaction(hint, Some(index.as_table()))?;
  // Through an index a read always locks, and holds its lock to the
  // commit.
  let read_args = ReadKeyArgs {
    kind: ReadKind::Committed,
    ..ReadKeyArgs::default()
  };
  conn.read_key(reader, trans, &read_args)?;
  conn.commit_transaction(trans)?;
  committed(&mut conn, trans, reader)?;
  match conn.query(reader) {
    Some(query) => {
      if let Some(e) = query.error() {
        return Err(e);
      }
      check(query.blob(at_doc)?.is_none(), "the TEXT now NULL")?;
      let read = query.blob(at_data)?;
      check(read == Some(new_bytes.as_slice()), "the new LONGBLOB")?;
    }
    None => return Err(no_query),
  }
  println!("updated by key, read by code: NULL and {}", new_bytes.len());

  // 3. Through an executor: a read, then the same operation made a
  //    delete, which takes the parts with the row.
  let executor = Executor::start(&global, &ExecutorOptions::default())?;
  let mut submitter = executor.submitter();
  let x_table = submitter.table_bind(EX_DB, EX_T4)?;
  let x_key = Record::default_for(&x_table, Some(&[id]))?;
  let x_values = Record::default_for(&x_table, Some(&[doc, data]))?;
  let shape = OpShape::new(&x_table, &x_key, &x_values)?;
  let mut op = KeyOp::read(&shape, ReadKind::Committed);
  put_u32(&x_key, op.key_row_mut(), id, 1)?;
  let mut request = Request::with_op(op);
  submitter.execute(&mut request)?;
  let read = request.ops()[0].blob(position(&x_values, data)?)?;
  check(read == Some(new_bytes.as_slice()), "read by the executor")?;
  request.ops_mut()[0].set_kind(OpKind::Write(WriteKind::Delete));
  submitter.execute(&mut request)?;
  let found = key_op::read_committed_blobs(
    &mut conn,
    &key_rec,
    &key,
    &rec,
    &mut row,
    &mut values,
  )?;
  check(!found, "row 1 deleted, its parts with it")?;
  println!("read and deleted through an executor");

  conn.free_query(updater)?;
  conn.free_query(reader)?;
  println!("all checks passed");
  Ok(())
}

fn main() {
  let args: Vec<String> = std::env::args().collect();
  exit_on(run(&args));
}
