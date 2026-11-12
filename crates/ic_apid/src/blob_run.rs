// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! The driver of BLOB queries on a connection (chapter 06, phase 12,
//! "The driver"). New: the C had no blobs.
//!
//! A query whose attribute record names blob fields runs in rounds: its
//! own operation on the main row, and part operations on the parts
//! tables, which are queries of the connection's own, made at the
//! query's first execution and kept for the next, a set for each blob
//! field. Their replies are routed by their word as any query's are,
//! and the transaction counts them, fails them and times them out as
//! it does every query.
//!
//! - **A round is done** when the query's own operation and every part
//!   operation of the round have completed. The query stays sent until
//!   its last round is done, then completes once, by its callback or on
//!   the executed list.
//! - **The poll plans the next round** at its end, from what the round
//!   brought ([`blob::plan_part_writes`]), and sends it there, as it
//!   sends what scans ask for. A send there sends whatever else is
//!   defined on the connection with it.
//! - **The commit is held**: a transaction counts its BLOB queries that
//!   plan a round after the one they have out, and a commit asked for
//!   goes only on the last round of the last of them (chapter 06, item
//!   3), or alone once none plans one.
//! - **A part is written with the write operation**, aborting on error,
//!   so that a part that fails rolls the transaction back. Its key is
//!   the main row's key words and the part number; its columns the main
//!   key's, the part number, the part key id 0 and the part's bytes,
//!   packed straight from the query's value (chapter 05 §6.10).
//! - **A failure ends the run**: the query completes with it, and a
//!   round that could not be defined rolls the transaction back, so
//!   that a value is never committed without its parts.
//!
//! Inserts only, so far: other writes come in 12.4, reads in 12.5.

use std::sync::Arc;

use ic_port::err;
use ic_port::IcError;
use ic_signals::column_header;
use ic_signals::header::SignalHeader;
use ic_signals::key_ops;
use ic_signals::key_ops::KeyOpFlags;
use ic_signals::key_ops::KeyOpReq;
use ic_signals::numbers;

use crate::apid_conn::ApidConnection;
use crate::blob;
use crate::blob::BlobValues;
use crate::blob::PartColumn;
use crate::blob::PartWrite;
use crate::blob::RunKind;
use crate::blob::ValueParts;
use crate::dict_cache::TableDef;
use crate::query::ApidQuery;
use crate::query::Execution;
use crate::query::PartOf;
use crate::query::QueryId;
use crate::query::QueryState;
use crate::query::TransId;
use crate::row_codec;

/// Columns a parts table has after the main key's: the part number,
/// the part key id and the part's bytes, in that order.
const IC_PARTS_OWN_COLUMNS: u32 = 3;

/// One blob field of a BLOB query, as its run writes it.
struct ColumnRun {
  /// Its column's parts table; `None` for a column without parts.
  parts: Option<Arc<TableDef>>,
  /// The parts table's data column.
  part_column: Option<PartColumn>,
  /// How many key columns the parts table copies: its part number,
  /// part key id and bytes are the columns after them.
  keys: u32,
  /// Part operations over the parts table, kept from round to round
  /// and from run to run.
  ops: Vec<QueryId>,
  /// Where the round being defined looks for its next free operation.
  cursor: usize,
}

/// The run of a BLOB query's rounds (chapter 06, phase 12), kept with
/// the query from one execution to the next.
pub(crate) struct BlobRun {
  /// True from the query's definition until it completes.
  active: bool,
  /// Counts the rounds, so that an operation of an earlier one is
  /// known for what it is.
  round: u32,
  /// The query's own operation is out and has not completed.
  main_out: bool,
  /// Part operations of the round out that have not completed.
  ops_out: u32,
  /// The round out is the last.
  last: bool,
  /// The run is counted among its transaction's BLOB queries that plan
  /// a round after the one out, which hold its commit.
  holds_commit: bool,
  /// What failed an operation of the round, if one did.
  error: Option<IcError>,
  /// Each blob value's parts, in the order of the query's blob fields.
  values: Vec<ValueParts>,
  /// Each blob field's column, in the same order.
  columns: Vec<ColumnRun>,
  /// The main row's key section.
  main_key: Vec<u32>,
  /// The main row's key as the parts tables' first columns.
  key_attrs: Vec<u32>,
  /// The part writes of the round being defined.
  writes: Vec<PartWrite>,
  /// The part operations defined for the round, to take back if the
  /// round cannot be defined whole.
  round_ops: Vec<QueryId>,
}

impl BlobRun {
  /// A run for `query`, its blob fields' columns found in its table.
  fn new(query: &ApidQuery) -> Result<BlobRun, IcError> {
    let values = match query.blob_values() {
      Some(values) => values,
      None => return Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    };
    let mut columns: Vec<ColumnRun> = Vec::with_capacity(values.len());
    let mut index: usize = 0;
    while let Some(field) = values.nth(index) {
      index += 1;
      let column = match query.table().blob_column(field.field_id) {
        Some(column) if column.is_usable() => column,
        _ => return Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED)),
      };
      let mut keys: u32 = 0;
      if let Some(parts) = column.parts_table() {
        keys = parts.num_fields().saturating_sub(IC_PARTS_OWN_COLUMNS);
      }
      if field.sizes.has_parts() && column.part_column().is_none() {
        return Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED));
      }
      columns.push(ColumnRun {
        parts: column.parts_table().cloned(),
        part_column: column.part_column(),
        keys,
        ops: Vec::new(),
        cursor: 0,
      });
    }
    Ok(BlobRun {
      active: false,
      round: 0,
      main_out: false,
      ops_out: 0,
      last: true,
      holds_commit: false,
      error: None,
      values: Vec::with_capacity(columns.len()),
      columns,
      main_key: Vec::new(),
      key_attrs: Vec::new(),
      writes: Vec::new(),
      round_ops: Vec::new(),
    })
  }

  /// Ready the run for an execution of `query` as `kind`, and plan its
  /// first round: everything that can be refused is refused here,
  /// before anything is defined.
  fn prepare(
    &mut self,
    query: &ApidQuery,
    kind: RunKind,
    budget: u64,
  ) -> Result<(), IcError> {
    let values = match query.blob_values() {
      Some(values) => values,
      None => return Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    };
    self.values.clear();
    let mut parts: u64 = 0;
    let mut index: usize = 0;
    while let Some(field) = values.nth(index) {
      index += 1;
      let mut length: u64 = 0;
      if let Some(value) = field.value {
        length = value.len() as u64;
      }
      if !field.sizes.holds(length) {
        return Err(IcError::new(blob::IC_NDB_ERROR_BLOB_TOO_LONG));
      }
      parts += field.sizes.part_count(length);
      self.values.push(ValueParts::new(field.sizes, length));
    }
    row_codec::key_info_into(
      query.key_record(),
      query.key_row(),
      &mut self.main_key,
    )?;
    row_codec::parts_key_attr_info_into(
      query.key_record(),
      query.key_row(),
      &mut self.key_attrs,
    )?;
    // A part's key is the main key and one word more.
    if parts > 0 && self.main_key.len() >= key_ops::IC_MAX_KEY_WORDS {
      return Err(IcError::new(err::IC_ERROR_KEY_TOO_LONG));
    }
    self.error = None;
    self.last =
      blob::plan_first_round(kind, &mut self.values, budget, &mut self.writes);
    Ok(())
  }

  /// Every part operation the run has made.
  fn op_ids(&self) -> Vec<QueryId> {
    let mut ids: Vec<QueryId> = Vec::new();
    for column in &self.columns {
      ids.extend_from_slice(&column.ops);
    }
    ids
  }
}

impl ApidConnection {
  /// Ready a BLOB query's run for the operation `flags` describes, and
  /// plan its first round, before the query's own operation is defined
  /// (chapter 06, phase 12). Only inserts take blob values until 12.4.
  pub(crate) fn prepare_blob_run(
    &mut self,
    qid: QueryId,
    flags: &KeyOpFlags,
  ) -> Result<(), IcError> {
    let budget = self.shared.blob_round_bytes;
    let mut kind = RunKind::Insert;
    if flags.abort_option == key_ops::IC_IGNORE_ERROR {
      kind = RunKind::InsertIgnoring;
    }
    let query = match self.queries.get_mut(qid.0) {
      Some(query) => query,
      None => return Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    };
    let mut run = match query.run.take() {
      Some(run) => run,
      None => Box::new(BlobRun::new(query)?),
    };
    let prepared = run.prepare(query, kind, budget);
    query.run = Some(run);
    prepared
  }

  /// Start a BLOB query's first round once its own operation has been
  /// defined on `tid`: its part operations defined after it, and the
  /// transaction's commit held if a round follows. On a failure the
  /// round's part operations are taken back; the query's own is the
  /// caller's to take back.
  pub(crate) fn start_blob_run(
    &mut self,
    qid: QueryId,
    tid: TransId,
  ) -> Result<(), IcError> {
    let (mut run, values) = match self.take_run(qid) {
      Some(taken) => taken,
      None => return Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    };
    run.active = true;
    run.main_out = true;
    let defined = self.define_part_writes(qid, tid, &mut run, &values);
    let mut hold = false;
    match defined {
      Ok(()) => {
        hold = !run.last;
        run.holds_commit = hold;
      }
      Err(_) => {
        for op in &run.round_ops {
          self.undefine(*op, tid);
        }
        run.active = false;
        run.main_out = false;
        run.ops_out = 0;
      }
    }
    self.put_run(qid, run, values);
    if hold {
      if let Some(trans) = self.transactions.get_mut(tid.0) {
        trans.hold_commit();
      }
    }
    defined
  }

  /// The query's run and blob values, taken out while a round is
  /// defined, so that the connection can be changed meanwhile.
  fn take_run(&mut self, qid: QueryId) -> Option<(Box<BlobRun>, BlobValues)> {
    let query = self.queries.get_mut(qid.0)?;
    let run = query.run.take()?;
    match query.take_blob_values() {
      Some(values) => Some((run, values)),
      None => {
        query.run = Some(run);
        None
      }
    }
  }

  fn put_run(&mut self, qid: QueryId, run: Box<BlobRun>, values: BlobValues) {
    if let Some(query) = self.queries.get_mut(qid.0) {
      query.run = Some(run);
      query.put_blob_values(values);
    }
  }

  /// Define the part writes `run.writes` plans, on `tid`, each packed
  /// straight from `values`, as a new round of the run.
  fn define_part_writes(
    &mut self,
    qid: QueryId,
    tid: TransId,
    run: &mut BlobRun,
    values: &BlobValues,
  ) -> Result<(), IcError> {
    run.round = run.round.wrapping_add(1);
    run.ops_out = 0;
    run.round_ops.clear();
    for column in &mut run.columns {
      column.cursor = 0;
    }
    if run.writes.is_empty() {
      return Ok(());
    }
    let (id, coord) = match self.transactions.get(tid.0) {
      Some(trans) => (trans.id(), trans.coord),
      None => return Err(IcError::new(err::IC_ERROR_TRANSACTION_ACTIVE)),
    };
    let of = PartOf {
      owner: qid,
      round: run.round,
    };
    let flags = KeyOpFlags {
      operation: key_ops::IC_OP_WRITE,
      abort_option: key_ops::IC_ABORT_ON_ERROR,
      ..KeyOpFlags::default()
    };
    let header = SignalHeader::new(
      numbers::IC_SIG_KEY_OP_REQ,
      self.block_number(),
      coord.coord_block,
    );
    let mut key = std::mem::take(&mut self.key_scratch);
    let mut attr = std::mem::take(&mut self.attr_scratch);
    let mut bytes = std::mem::take(&mut self.blob_bytes);
    let mut result: Result<(), IcError> = Ok(());
    let mut i: usize = 0;
    while i < run.writes.len() {
      let write = run.writes[i];
      i += 1;
      let slot = match self.part_slot(&mut run.columns[write.value]) {
        Ok(slot) => slot,
        Err(e) => {
          result = Err(e);
          break;
        }
      };
      let column = &run.columns[write.value];
      let (parts, part_column) = match (&column.parts, column.part_column) {
        (Some(parts), Some(part_column)) => (Arc::clone(parts), part_column),
        _ => {
          result = Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED));
          break;
        }
      };
      let value: &[u8] = values
        .nth(write.value)
        .and_then(|field| field.value)
        .unwrap_or_default();
      let sizes = run.values[write.value].sizes;
      let start = sizes.part_offset(write.part) as usize;
      let len = sizes.part_len(value.len() as u64, write.part);
      let part = write.part as u32;
      blob::part_key(&run.main_key, part, &mut key);
      attr.clear();
      attr.extend_from_slice(&run.key_attrs);
      attr.push(column_header::column_header(column.keys, 4));
      attr.push(part);
      attr.push(column_header::column_header(column.keys + 1, 4));
      attr.push(0);
      blob::push_part_value(
        part_column,
        column.keys + 2,
        &value[start..start + len],
        &mut bytes,
        &mut attr,
      );
      let request = KeyOpReq {
        coord_ptr: coord.coord_ptr,
        query_ref: slot.wire_word(),
        table_id: parts.table_id(),
        flags_word: flags.flags_word(),
        table_version: parts.table_version(),
        trans_id_lo: id as u32,
        trans_id_hi: (id >> 32) as u32,
      };
      let sections: [&[u32]; 2] = [&key, &attr];
      let staged = self.stage_signal(
        coord.node_id,
        coord.generation,
        &header,
        &request.encode(),
        &sections,
      );
      let staged = match staged {
        Ok(staged) => staged,
        Err(e) => {
          result = Err(e);
          break;
        }
      };
      if let Some(op) = self.queries.get_mut(slot.0) {
        op.begin(
          Execution {
            trans: Some(tid),
            trans_id: id,
            flags,
            staged,
            part_of: Some(of),
            ..Execution::default()
          },
          0,
          None,
          0,
        );
      }
      if let Some(trans) = self.transactions.get_mut(tid.0) {
        trans.defined.push(slot);
      }
      run.round_ops.push(slot);
      run.ops_out += 1;
    }
    self.key_scratch = key;
    self.attr_scratch = attr;
    self.blob_bytes = bytes;
    if result.is_ok() {
      self.queue_for_send(tid);
    }
    result
  }

  /// A part operation of `column` free for the round being defined: one
  /// kept from before that is idle, or a new one.
  fn part_slot(&mut self, column: &mut ColumnRun) -> Result<QueryId, IcError> {
    while column.cursor < column.ops.len() {
      let id = column.ops[column.cursor];
      column.cursor += 1;
      if let Some(op) = self.queries.get(id.0) {
        let state = op.state();
        if state == QueryState::Idle || state == QueryState::Completed {
          return Ok(id);
        }
      }
    }
    let parts = match &column.parts {
      Some(parts) => parts,
      None => return Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED)),
    };
    let id = self.insert_query(ApidQuery::internal(parts)?)?;
    column.ops.push(id);
    column.cursor = column.ops.len();
    Ok(id)
  }

  /// A BLOB query's own operation has completed: true if its run takes
  /// it from here, which then completes the query once its round is
  /// done and it has no further one.
  pub(crate) fn blob_main_done(&mut self, qid: QueryId) -> bool {
    let query = match self.queries.get_mut(qid.0) {
      Some(query) => query,
      None => return false,
    };
    match query.run.as_mut() {
      Some(run) if run.active && run.main_out => run.main_out = false,
      _ => return false,
    }
    query.set_result_len(0);
    self.blob_round_check(qid);
    true
  }

  /// A part operation has completed, with `error` if it failed.
  pub(crate) fn blob_op_done(&mut self, of: PartOf, error: Option<IcError>) {
    let run = match self.queries.get_mut(of.owner.0) {
      Some(query) => match query.run.as_mut() {
        Some(run) => run,
        None => return,
      },
      None => return,
    };
    if !run.active || run.round != of.round {
      return;
    }
    run.ops_out = run.ops_out.saturating_sub(1);
    if run.error.is_none() {
      run.error = error;
    }
    self.blob_round_check(of.owner);
  }

  /// Once a round is done: complete the query if it was the last, or if
  /// an operation failed; otherwise leave the next round to the poll.
  fn blob_round_check(&mut self, qid: QueryId) {
    let (done, finish) = match self.queries.get(qid.0) {
      Some(query) => match &query.run {
        Some(run) if run.active => (
          !run.main_out && run.ops_out == 0,
          run.last || run.error.is_some(),
        ),
        _ => return,
      },
      None => return,
    };
    if !done {
      return;
    }
    if finish {
      self.complete_blob_query(qid);
    } else {
      self.blob_due.push(qid);
    }
  }

  /// Complete a BLOB query whose rounds are over: with the error that
  /// failed one of its operations, or done.
  fn complete_blob_query(&mut self, qid: QueryId) {
    if let Some(query) = self.queries.get_mut(qid.0) {
      let mut error: Option<IcError> = None;
      if let Some(run) = &query.run {
        error = run.error;
      }
      match error {
        Some(e) => query.fail(e),
        None => query.set_state(QueryState::Completed),
      }
    }
    self.finish_query(qid);
  }

  /// End a query's run, if it has one going: the query has completed,
  /// or will be failed by its transaction's end. The commit it held may
  /// go once no other query holds it.
  pub(crate) fn end_blob_run(&mut self, qid: QueryId) {
    let (tid, held) = match self.queries.get_mut(qid.0) {
      Some(query) => {
        let tid = query.execution.trans;
        match query.run.as_mut() {
          Some(run) if run.active => {
            run.active = false;
            run.main_out = false;
            run.ops_out = 0;
            let held = run.holds_commit;
            run.holds_commit = false;
            (tid, held)
          }
          _ => return,
        }
      }
      None => return,
    };
    if !held {
      return;
    }
    if let Some(tid) = tid {
      self.release_blob_commit(tid);
    }
  }

  /// A query of `tid` plans no further round: once none does, a commit
  /// asked for goes with the next send, alone.
  fn release_blob_commit(&mut self, tid: TransId) {
    let commit_due = match self.transactions.get_mut(tid.0) {
      Some(trans) => trans.release_commit(),
      None => false,
    };
    if commit_due {
      self.queue_for_send(tid);
      self.blob_send_due = true;
    }
  }

  /// What a poll does for BLOB queries, once it has taken its replies:
  /// plan and define the next round of each whose round is done, and
  /// send them, with any commit they let go.
  pub(crate) fn drive_blobs(&mut self) {
    if !self.blob_due.is_empty() {
      let mut due = std::mem::take(&mut self.blob_due);
      for qid in &due {
        self.next_blob_round(*qid);
      }
      due.clear();
      if self.blob_due.is_empty() {
        self.blob_due = due;
      }
    }
    if self.blob_send_due {
      self.blob_send_due = false;
      // A send that fails fails its transactions at the next poll, as
      // their link is gone.
      let _ = self.send_queries(false);
    }
  }

  /// The next round of a query whose round is done and was not its
  /// last.
  fn next_blob_round(&mut self, qid: QueryId) {
    let tid = match self.queries.get(qid.0) {
      Some(query) => match (&query.run, query.execution.trans) {
        (Some(run), Some(tid))
          if run.active
            && !run.main_out
            && run.ops_out == 0
            && query.state() == QueryState::Sent =>
        {
          tid
        }
        _ => return,
      },
      None => return,
    };
    let (goes_on, ended, trans_error) = match self.transactions.get(tid.0) {
      Some(trans) => (trans.takes_blob_rounds(), trans.ended(), trans.error()),
      None => return,
    };
    if !goes_on {
      // A rollback asked for, or under way, fails the query as the
      // transaction ends; one that has ended already did, unless it
      // ended without it.
      self.end_blob_run(qid);
      if ended {
        let error = match trans_error {
          Some(e) => e,
          None => IcError::new(err::IC_ERROR_TRANSACTION_ROLLED_BACK),
        };
        if let Some(query) = self.queries.get_mut(qid.0) {
          query.fail(error);
        }
        self.finish_query(qid);
      }
      return;
    }
    let (mut run, values) = match self.take_run(qid) {
      Some(taken) => taken,
      None => return,
    };
    let budget = self.shared.blob_round_bytes;
    run.last = blob::plan_part_writes(&mut run.values, budget, &mut run.writes);
    let defined = self.define_part_writes(qid, tid, &mut run, &values);
    let mut release = false;
    if defined.is_ok() && run.last && run.holds_commit {
      run.holds_commit = false;
      release = true;
    }
    self.put_run(qid, run, values);
    match defined {
      Ok(()) => {
        if release {
          self.release_blob_commit(tid);
        }
        self.blob_rounds += 1;
        self.blob_send_due = true;
      }
      Err(e) => {
        // The value's head is in, its parts are not: the transaction
        // must not commit.
        self.force_rollback(tid);
        if let Some(query) = self.queries.get_mut(qid.0) {
          query.fail(e);
        }
        self.finish_query(qid);
        self.blob_send_due = true;
      }
    }
  }

  /// Free a query's part operations with it. Refused while one is
  /// defined or out (`IC_ERROR_TRANSACTION_ACTIVE`).
  pub(crate) fn free_blob_ops(&mut self, qid: QueryId) -> Result<(), IcError> {
    let ops = match self.queries.get(qid.0) {
      Some(query) => match &query.run {
        Some(run) => run.op_ids(),
        None => return Ok(()),
      },
      None => return Ok(()),
    };
    for op in &ops {
      if let Some(query) = self.queries.get(op.0) {
        let state = query.state();
        if state == QueryState::Defined || state == QueryState::Sent {
          return Err(IcError::new(err::IC_ERROR_TRANSACTION_ACTIVE));
        }
      }
    }
    for op in ops {
      self.queries.remove(op.0);
    }
    Ok(())
  }
}
