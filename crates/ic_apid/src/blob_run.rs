// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! The driver of BLOB queries on a connection (chapter 06, phase 12,
//! "The driver"). New: the C had no blobs.
//!
//! A BLOB query, one whose attribute record names blob fields or a
//! delete from a table with blob columns, runs in rounds. Every request
//! of it is an operation of the connection's own, made at the query's
//! first execution and kept for the next: the main row's operation, the
//! read of the old values' heads, and the part operations, a set over
//! each parts table. Their replies are routed by their word as any
//! query's are, and the transaction counts them, fails them and times
//! them out as it does every query. The query itself goes on no wire:
//! it is defined and sent with its transaction, and completes once, by
//! its callback or on the executed list, when its last round is done.
//!
//! - **The rounds** are chapter 06's: an insert's main row with its
//!   parts; a write's or an update's heads read beside the main row's
//!   write, then the new parts written and the old ones past them
//!   deleted; a delete's heads read, then the main row and every part
//!   deleted. A column without parts takes no head read. A locked read
//!   reads the main row with its lock, then its parts with simple
//!   reads; a committed read reads it committed first, all for an
//!   inline value, else again with a shared lock and its lock
//!   reference, then the parts, and unlocks it unless a commit is asked
//!   for; one that locks first, as a simple read does, takes the lock
//!   and its reference in its first round. A committed read reads
//!   committed first only when it is its transaction's only operation,
//!   decided as the transaction is sent; it holds the commit back from
//!   its first round, so that a value with parts can be read again
//!   with the locking reads, a new transaction at the data nodes.
//! - **The poll plans the next round** at its end, from what the round
//!   brought ([`blob::plan_part_ops`]), and sends it there, as it sends
//!   what scans ask for. A send there sends whatever else is defined on
//!   the connection with it.
//! - **The commit is held**: a transaction counts its BLOB queries that
//!   plan a round after the one they have out, and a commit asked for
//!   goes only on the last round of the last of them (chapter 06, item
//!   3), or alone once none plans one.
//! - **Two BLOB queries on one row** (one table, the same key words) in
//!   one transaction run one after the other, in the order they were
//!   defined; the second's first round waits for the first's end.
//! - **A part is written with the write operation**, aborting on error,
//!   so that a part that fails rolls the transaction back. Its key is
//!   the main row's key words and the part number; its columns the main
//!   key's, the part number, the part key id 0 and the part's bytes,
//!   packed straight from the query's value (chapter 05 §6.10).
//! - **A write that finds no row** deletes no part. With the query's
//!   probe flag (`ApidQuery::set_blob_probe`, off by default, the
//!   author's choice) it probes the part past its new value's last,
//!   deleting it if it is there, and the next while one is: an insert
//!   of the row between the head read and the write may otherwise leave
//!   its parts behind, past the value's length. A round with a probe is
//!   not the last, so such a write commits a round later.
//! - **Through a unique index** (chapter 06, item 7) the first round
//!   reads the row, or for an update or a delete its heads, through the
//!   index with the operation's lock, and the primary key's columns
//!   after, each by its header; from the second round on the run goes
//!   by that key. A read through an index always locks, and holds its
//!   lock to the commit. A query whose values have no parts goes
//!   through the index alone, in one round.
//! - **A failure ends the run**: the query completes with it, and a
//!   round that could not be defined rolls the transaction back, so
//!   that a value is never committed without its parts.

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
use crate::apid_conn::CoordRecord;
use crate::blob;
use crate::blob::BlobSizes;
use crate::blob::BlobValues;
use crate::blob::PartColumn;
use crate::blob::PartOp;
use crate::blob::PartOpKind;
use crate::blob::RunKind;
use crate::blob::ValueParts;
use crate::dict_cache::IndexDef;
use crate::dict_cache::TableDef;
use crate::key_op::IC_NDB_ERROR_NO_SUCH_ROW;
use crate::node_connect::words_as_bytes;
use crate::query::ApidQuery;
use crate::query::Execution;
use crate::query::PartOf;
use crate::query::QueryId;
use crate::query::QueryState;
use crate::query::TransId;
use crate::row_codec;
use crate::row_codec::push_padded;
use crate::row_codec::IC_COL_LOCK_REF;
use crate::row_codec::IC_LOCK_REF_WORDS;

/// Columns a parts table has after the main key's: the part number,
/// the part key id and the part's bytes, in that order.
const IC_PARTS_OWN_COLUMNS: u32 = 3;

/// One blob column of the table, as runs use it: its parts table and
/// the operations made over it.
struct ColumnRun {
  /// Its id in the table.
  column_id: u32,
  /// Its sizes.
  sizes: BlobSizes,
  /// Its parts table; `None` for a column without parts.
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

/// What an operation of a run is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpRole {
  /// The main row's own: the query's insert, write, update or delete.
  Main,
  /// The read of the old values' heads.
  Heads,
  /// A part's read, write, delete or probe.
  Part(PartOp),
  /// The unlock of the main row a read locked, which is not waited for.
  Unlock,
}

/// A value of a run: its column, by its place among the table's, and
/// its place among the query's blob values, which a delete's have not.
#[derive(Clone, Copy, Debug)]
struct Target {
  column: usize,
  slot: Option<usize>,
}

/// Where a round's operations go: the transaction, its id as the data
/// nodes know it, and its coordinator.
#[derive(Clone, Copy)]
struct RoundTo {
  tid: TransId,
  trans_id: u64,
  coord: CoordRecord,
}

/// One operation to define.
struct OpReq<'a> {
  slot: QueryId,
  of: PartOf,
  table: &'a TableDef,
  /// The unique index the request goes through, the key its key.
  index: Option<&'a IndexDef>,
  flags: KeyOpFlags,
  is_read: bool,
  key: &'a [u32],
  attr: &'a [u32],
  /// The distribution key word an unlock carries.
  dist_key: Option<u32>,
}

/// The run of a BLOB query's rounds (chapter 06, phase 12), kept with
/// the query from one execution to the next.
pub(crate) struct BlobRun {
  /// True from the query's definition until it completes.
  active: bool,
  /// An earlier BLOB query on the same row, in the same transaction,
  /// whose end the first round waits for.
  after: Option<QueryId>,
  /// True once the first round has been defined.
  started: bool,
  /// A committed read whose first round waits for its transaction's
  /// send, which tells whether it is the transaction's only operation:
  /// only then is it read committed first (the author, 2026-11-12).
  undecided: bool,
  /// What the query does.
  kind: RunKind,
  /// A write that finds no row probes for parts a racing insert left.
  probe: bool,
  /// Counts the rounds, so that an operation of an earlier one is
  /// known for what it is.
  round: u32,
  /// Operations of the round out that have not completed.
  ops_out: u32,
  /// The round out is the last.
  last: bool,
  /// The run is counted among its transaction's BLOB queries that plan
  /// a round after the one out, which hold its commit.
  holds_commit: bool,
  /// The heads are read in the next round.
  heads_due: bool,
  /// The main row's operation goes in the next round: in the first,
  /// but for a delete that reads heads, whose main row goes once they
  /// have come.
  main_due: bool,
  /// What failed an operation, and what its refusal said beyond its
  /// code.
  error: Option<(IcError, u32)>,
  /// The main row's read asks for its lock reference.
  main_locks: bool,
  /// The lock reference a read brought, which an unlock names.
  lock_ref: Option<[u32; IC_LOCK_REF_WORDS]>,
  /// The next round lets the main row's lock go, as a read that locked
  /// it does once its parts are read, while no commit is asked for.
  unlock_due: bool,
  /// The main row's operation: its flags, and its column section.
  main_flags: KeyOpFlags,
  main_attr: Vec<u32>,
  /// The column section of the read of the heads, and what it brought.
  heads_info: Vec<u32>,
  heads: Vec<(u32, Option<u64>)>,
  /// Each value's parts, and its column and slot.
  values: Vec<ValueParts>,
  targets: Vec<Target>,
  /// The table's usable blob columns, in attribute order.
  columns: Vec<ColumnRun>,
  /// The main table, and the operations made over it, kept.
  main_table: Arc<TableDef>,
  main_ops: Vec<QueryId>,
  main_cursor: usize,
  /// The main row's key section, and the key as the parts tables'
  /// first columns.
  main_key: Vec<u32>,
  key_attrs: Vec<u32>,
  /// The unique index the query goes through (chapter 06, item 7),
  /// and whether `main_key` is the primary key yet: until the first
  /// round has read it, the main row's requests go through the index,
  /// `main_key` holding the index's key.
  index: Option<Arc<IndexDef>>,
  key_known: bool,
  /// The headers of the primary key's columns, asked for after what the
  /// first round reads through the index.
  pk_info: Vec<u32>,
  /// The part operations of the round being defined.
  plan: Vec<PartOp>,
  /// The operations of the round out, by the index their `PartOf`
  /// carries, and what each is for.
  round_ops: Vec<(QueryId, OpRole)>,
}

impl BlobRun {
  /// A run for a query over `table`, with its usable blob columns.
  fn new(table: &Arc<TableDef>) -> BlobRun {
    let mut columns: Vec<ColumnRun> = Vec::new();
    for column in table.blob_columns() {
      let sizes = match column.sizes() {
        Some(sizes) if column.is_usable() => sizes,
        _ => continue,
      };
      let mut keys: u32 = 0;
      if let Some(parts) = column.parts_table() {
        keys = parts.num_fields().saturating_sub(IC_PARTS_OWN_COLUMNS);
      }
      columns.push(ColumnRun {
        column_id: column.column_id(),
        sizes,
        parts: column.parts_table().cloned(),
        part_column: column.part_column(),
        keys,
        ops: Vec::new(),
        cursor: 0,
      });
    }
    BlobRun {
      active: false,
      after: None,
      started: false,
      undecided: false,
      kind: RunKind::Insert,
      probe: false,
      round: 0,
      ops_out: 0,
      last: true,
      holds_commit: false,
      heads_due: false,
      main_due: false,
      error: None,
      main_locks: false,
      lock_ref: None,
      unlock_due: false,
      main_flags: KeyOpFlags::default(),
      main_attr: Vec::new(),
      heads_info: Vec::new(),
      heads: Vec::new(),
      values: Vec::new(),
      targets: Vec::new(),
      columns,
      main_table: Arc::clone(table),
      main_ops: Vec::new(),
      main_cursor: 0,
      main_key: Vec::new(),
      key_attrs: Vec::new(),
      index: None,
      key_known: true,
      pk_info: Vec::new(),
      plan: Vec::new(),
      round_ops: Vec::new(),
    }
  }

  /// The place among the columns of the one with id `column_id`.
  fn column_of(&self, column_id: u32) -> Option<usize> {
    let mut index: usize = 0;
    while index < self.columns.len() {
      if self.columns[index].column_id == column_id {
        return Some(index);
      }
      index += 1;
    }
    None
  }

  /// Ready the run for an execution of `query` as `flags` say, and
  /// plan its first round: everything that can be refused is refused
  /// here, before anything is defined.
  fn prepare(
    &mut self,
    query: &ApidQuery,
    flags: &KeyOpFlags,
    budget: u64,
  ) -> Result<(), IcError> {
    let ignores = flags.abort_option == key_ops::IC_IGNORE_ERROR;
    let committed = flags.dirty;
    let simple = flags.simple && !flags.dirty;
    let lock_first = simple || (committed && query.blob_lock_first());
    self.kind = match flags.operation {
      key_ops::IC_OP_INSERT if ignores => RunKind::InsertIgnoring,
      key_ops::IC_OP_INSERT => RunKind::Insert,
      key_ops::IC_OP_WRITE => RunKind::Write,
      key_ops::IC_OP_UPDATE => RunKind::Update,
      key_ops::IC_OP_DELETE => RunKind::Delete,
      key_ops::IC_OP_READ if lock_first => RunKind::LockFirstRead,
      key_ops::IC_OP_READ if committed => RunKind::CommittedRead,
      key_ops::IC_OP_READ | key_ops::IC_OP_READ_EXCLUSIVE => {
        RunKind::LockedRead
      }
      _ => return Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED)),
    };
    self.probe = query.blob_probe();
    self.values.clear();
    self.targets.clear();
    if self.kind == RunKind::Delete {
      // Every blob column's parts go with the row.
      let mut index: usize = 0;
      while index < self.columns.len() {
        let sizes = self.columns[index].sizes;
        self.values.push(ValueParts::new(sizes, 0));
        self.targets.push(Target {
          column: index,
          slot: None,
        });
        index += 1;
      }
    } else {
      let blobs = match query.blob_values() {
        Some(blobs) => blobs,
        None => return Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
      };
      let mut slot: usize = 0;
      while let Some(field) = blobs.nth(slot) {
        let column = match self.column_of(field.field_id) {
          Some(column) => column,
          None => return Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED)),
        };
        // A read's values are what it brings; a write's what it sends.
        let mut length: u64 = 0;
        if let Some(value) = field.value {
          if !self.kind.is_read() {
            length = value.len() as u64;
          }
        }
        if !field.sizes.holds(length) {
          return Err(IcError::new(blob::IC_NDB_ERROR_BLOB_TOO_LONG));
        }
        self.values.push(ValueParts::new(field.sizes, length));
        self.targets.push(Target {
          column,
          slot: Some(slot),
        });
        slot += 1;
      }
    }
    let has_parts = blob::reads_any_head(&self.values);
    // Through a unique index, the key is the index's until the first
    // round has read the primary key's columns, which it asks for only
    // when there are parts to go by it.
    self.index = query.index().cloned();
    self.key_known = self.index.is_none();
    self.pk_info.clear();
    match &self.index {
      Some(index) => {
        row_codec::index_key_info_into(
          index,
          query.key_record(),
          query.key_row(),
          &mut self.main_key,
        )?;
        self.key_attrs.clear();
        if has_parts {
          for attr in &self.main_table.info().attributes {
            if attr.primary_key {
              self
                .pk_info
                .push(column_header::column_header(attr.column_id, 0));
            }
          }
        }
      }
      None => {
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
      }
    }
    // A part's key is the main key and one word more.
    if has_parts
      && self.key_known
      && self.main_key.len() >= key_ops::IC_MAX_KEY_WORDS
    {
      return Err(IcError::new(err::IC_ERROR_KEY_TOO_LONG));
    }
    self.main_flags = KeyOpFlags {
      operation: flags.operation,
      abort_option: flags.abort_option,
      batch_safe: flags.batch_safe,
      batch_unsafe: flags.batch_unsafe,
      simple: flags.simple,
      dirty: flags.dirty,
      committed_base: flags.committed_base,
      no_wait: flags.no_wait,
      ..KeyOpFlags::default()
    };
    self.main_locks = false;
    self.lock_ref = None;
    self.unlock_due = false;
    // The main row's values, the blob columns' heads and inline bytes
    // among them; an update leaves the key columns out, a delete has
    // none. A read asks for the attribute record's columns, the blob
    // columns among them, a read that locks first its lock reference
    // too, unless no column has parts, when the read is the program's
    // own, in one round.
    self.main_attr.clear();
    if self.kind.is_read() {
      row_codec::read_attr_info_append(
        query.attr_record(),
        &mut self.main_attr,
      );
      if self.kind == RunKind::LockFirstRead && has_parts {
        self.lock_read(flags.abort_option);
      }
      // Through an index, the primary key after the columns.
      self.main_attr.extend_from_slice(&self.pk_info);
    } else if self.kind != RunKind::Delete {
      row_codec::write_attr_info_append(
        query.attr_record(),
        query.attr_row(),
        self.kind == RunKind::Update,
        query.blob_values(),
        &mut self.main_attr,
      )?;
    }
    // The heads of the values whose columns have parts.
    self.heads_due = self.kind.reads_heads() && has_parts;
    let mut ids: Vec<u32> = Vec::new();
    if self.heads_due {
      let mut index: usize = 0;
      while index < self.values.len() {
        if self.values[index].sizes.has_parts() {
          ids.push(self.columns[self.targets[index].column].column_id);
        }
        index += 1;
      }
    }
    blob::masked_read_info(&ids, &mut self.heads_info);
    // Through an index, the primary key after the heads.
    self.heads_info.extend_from_slice(&self.pk_info);
    // A delete's main row waits for its heads, read with its lock, and
    // so does an update's through an index, which goes by the primary
    // key they bring.
    let waits = self.kind == RunKind::Delete || !self.key_known;
    self.main_due = !(waits && self.heads_due);
    self.error = None;
    self.after = None;
    self.started = false;
    self.undecided = self.kind == RunKind::CommittedRead && has_parts;
    self.last = blob::plan_first_round(
      self.kind,
      &mut self.values,
      budget,
      &mut self.plan,
    );
    Ok(())
  }

  /// Have the main row's next read take the shared lock and its lock
  /// reference: a shared read with bit 20, the lock reference asked for
  /// after the columns (chapter 05 §6.10).
  fn lock_read(&mut self, abort_option: u32) {
    self.main_flags = KeyOpFlags {
      operation: key_ops::IC_OP_READ,
      committed_base: true,
      abort_option,
      ..KeyOpFlags::default()
    };
    self
      .main_attr
      .push(column_header::column_header(IC_COL_LOCK_REF, 0));
    self.main_locks = true;
  }

  /// A committed read that is not its transaction's only operation
  /// takes the lock first: two rounds, its commit held.
  fn lock_first(&mut self) {
    self.kind = RunKind::LockFirstRead;
    let abort_option = self.main_flags.abort_option;
    self.lock_read(abort_option);
    self.last = false;
  }

  /// Every operation the run has made.
  fn op_ids(&self) -> Vec<QueryId> {
    let mut ids: Vec<QueryId> = self.main_ops.clone();
    for column in &self.columns {
      ids.extend_from_slice(&column.ops);
    }
    ids
  }

  /// Record what failed the run, the first failure only.
  fn fail(&mut self, error: IcError, data: u32) {
    if self.error.is_none() {
      self.error = Some((error, data));
    }
  }

  /// The primary key a first round through a unique index read after
  /// `used` words, each key column behind its header, in attribute
  /// order: the key the run goes by from now on, and its copy in the
  /// parts tables' first columns, as `row_codec::key_info_into` and
  /// `parts_key_attr_info_into` build them from a key row. A key not as
  /// asked for is `IC_ERROR_INCONSISTENT_DATA`.
  fn take_primary_key(
    &mut self,
    row: &[u32],
    used: usize,
  ) -> Result<(), IcError> {
    let bad = IcError::new(err::IC_ERROR_INCONSISTENT_DATA);
    self.main_key.clear();
    self.key_attrs.clear();
    let table = Arc::clone(&self.main_table);
    let mut at = used;
    let mut place: u32 = 0;
    for attr in &table.info().attributes {
      if !attr.primary_key {
        continue;
      }
      let header = match row.get(at) {
        Some(header) => *header,
        None => return Err(bad),
      };
      let len = column_header::byte_size(header) as usize;
      let words = len.div_ceil(4);
      let fits = column_header::column_id(header) == attr.column_id
        && len > 0
        && at + 1 + words <= row.len();
      if !fits {
        return Err(bad);
      }
      let bytes = words_as_bytes(&row[at + 1..at + 1 + words]);
      push_padded(&mut self.main_key, &bytes[..len]);
      self
        .key_attrs
        .push(column_header::column_header(place, len as u32));
      push_padded(&mut self.key_attrs, &bytes[..len]);
      at += 1 + words;
      place += 1;
    }
    // A part's key is the main key and one word more.
    if self.main_key.len() >= key_ops::IC_MAX_KEY_WORDS {
      return Err(IcError::new(err::IC_ERROR_KEY_TOO_LONG));
    }
    self.key_known = true;
    Ok(())
  }

  /// What the read of the heads brought: each old value's length, its
  /// parts past the new value's to be deleted.
  fn take_heads(&mut self, row: &[u32]) {
    let used = match blob::parse_heads(&self.main_table, row, &mut self.heads) {
      Ok(used) => used,
      Err(e) => {
        self.fail(e, 0);
        return;
      }
    };
    if !self.key_known {
      if let Err(e) = self.take_primary_key(row, used) {
        self.fail(e, 0);
        return;
      }
    }
    let mut i: usize = 0;
    while i < self.heads.len() {
      let (column_id, old) = self.heads[i];
      i += 1;
      let mut index: usize = 0;
      while index < self.values.len() {
        let column = self.targets[index].column;
        if self.columns[column].column_id == column_id {
          self.values[index].old_value(old.unwrap_or(0));
        }
        index += 1;
      }
    }
  }
}

impl ApidConnection {
  /// Ready a BLOB query's run for the operation `flags` describes, and
  /// plan its first round, before the query is defined (chapter 06,
  /// phase 12).
  pub(crate) fn prepare_blob_run(
    &mut self,
    qid: QueryId,
    flags: &KeyOpFlags,
  ) -> Result<(), IcError> {
    let budget = self.shared.blob_round_bytes;
    let query = match self.queries.get_mut(qid.0) {
      Some(query) => query,
      None => return Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    };
    let mut run = match query.run.take() {
      Some(run) => run,
      None => Box::new(BlobRun::new(query.table())),
    };
    let prepared = run.prepare(query, flags, budget);
    query.run = Some(run);
    prepared
  }

  /// Start a BLOB query's run once the query has been defined on `tid`:
  /// its first round defined, or, behind an earlier BLOB query on the
  /// same row, left until that one ends; the transaction's commit held
  /// if a round follows. On a failure the round's operations are taken
  /// back; the query is the caller's to take back.
  pub(crate) fn start_blob_run(
    &mut self,
    qid: QueryId,
    tid: TransId,
  ) -> Result<(), IcError> {
    let (mut run, values) = match self.take_run(qid) {
      Some(taken) => taken,
      None => return Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    };
    let after = self.blob_predecessor(tid, &run);
    run.active = true;
    run.after = after;
    let mut defined: Result<(), IcError> = Ok(());
    if after.is_none() && !run.undecided {
      defined = self.define_round(qid, tid, &mut run, values.as_ref());
    }
    let mut hold = false;
    match defined {
      Ok(()) => {
        hold = after.is_some() || !run.last;
        run.holds_commit = hold;
      }
      Err(_) => {
        for (op, _) in &run.round_ops {
          self.undefine(*op, tid);
        }
        run.active = false;
        run.after = None;
        run.ops_out = 0;
      }
    }
    self.put_run(qid, run, values);
    if let Some(trans) = self.transactions.get_mut(tid.0) {
      if defined.is_ok() {
        trans.add_blob_run(qid);
      }
      if hold {
        trans.hold_commit();
      }
    }
    defined
  }

  /// Plan the first round of each committed BLOB read of `tid` that
  /// waits for its send: read committed first when it is the
  /// transaction's only operation, else the lock taken first, its
  /// commit held. A round that cannot be defined fails its query.
  pub(crate) fn decide_blob_reads(&mut self, tid: TransId) {
    let runs = match self.transactions.get(tid.0) {
      Some(trans) if !trans.blob_runs().is_empty() => {
        trans.blob_runs().to_vec()
      }
      _ => return,
    };
    for qid in runs {
      let waiting = match self.queries.get(qid.0) {
        Some(query) => match &query.run {
          Some(run) => run.active && run.undecided && run.after.is_none(),
          None => false,
        },
        None => false,
      };
      if !waiting {
        continue;
      }
      let alone = match self.transactions.get(tid.0) {
        Some(trans) => trans.only_query(qid),
        None => return,
      };
      let (mut run, values) = match self.take_run(qid) {
        Some(taken) => taken,
        None => continue,
      };
      run.undecided = false;
      if !alone {
        run.lock_first();
      }
      // Alone, the commit is held back from the first round all the
      // same: a value with parts is read again with the locking reads,
      // and an inline one then commits at the client, at no cost.
      let defined = self.define_round(qid, tid, &mut run, values.as_ref());
      let hold = defined.is_ok() && !run.holds_commit;
      if hold {
        run.holds_commit = true;
      }
      self.put_run(qid, run, values);
      match defined {
        Ok(()) => {
          if hold {
            if let Some(trans) = self.transactions.get_mut(tid.0) {
              trans.hold_commit();
            }
          }
        }
        Err(e) => {
          if let Some(query) = self.queries.get_mut(qid.0) {
            query.fail(e);
          }
          self.finish_query(qid);
        }
      }
    }
  }

  /// The latest BLOB query of `tid` still running on the row `run` is
  /// for: one table, the same key words.
  fn blob_predecessor(&self, tid: TransId, run: &BlobRun) -> Option<QueryId> {
    let trans = self.transactions.get(tid.0)?;
    let mut found: Option<QueryId> = None;
    for other in trans.blob_runs() {
      let earlier = match self.queries.get(other.0) {
        Some(query) => match &query.run {
          Some(earlier) => earlier,
          None => continue,
        },
        None => continue,
      };
      // Through a unique index the row is not known by its primary key
      // until the first round is back: such a query keeps its order
      // with every other on the table.
      let unique = earlier.index.is_some() || run.index.is_some();
      let same_row = earlier.active
        && earlier.main_table.table_id() == run.main_table.table_id()
        && (unique || earlier.main_key == run.main_key);
      if same_row {
        found = Some(*other);
      }
    }
    found
  }

  /// The query's run and blob values, taken out while a round is
  /// defined, so that the connection can be changed meanwhile.
  fn take_run(
    &mut self,
    qid: QueryId,
  ) -> Option<(Box<BlobRun>, Option<BlobValues>)> {
    let query = self.queries.get_mut(qid.0)?;
    let run = query.run.take()?;
    Some((run, query.take_blob_values()))
  }

  fn put_run(
    &mut self,
    qid: QueryId,
    run: Box<BlobRun>,
    values: Option<BlobValues>,
  ) {
    if let Some(query) = self.queries.get_mut(qid.0) {
      query.run = Some(run);
      if let Some(values) = values {
        query.put_blob_values(values);
      }
    }
  }

  /// Define a round of the run on `tid`: the heads' read and the main
  /// row's operation if due, then the part operations `run.plan` holds,
  /// each packed straight from `values`.
  fn define_round(
    &mut self,
    qid: QueryId,
    tid: TransId,
    run: &mut BlobRun,
    values: Option<&BlobValues>,
  ) -> Result<(), IcError> {
    run.round = run.round.wrapping_add(1);
    run.started = true;
    run.ops_out = 0;
    run.round_ops.clear();
    run.main_cursor = 0;
    for column in &mut run.columns {
      column.cursor = 0;
    }
    let to = match self.transactions.get(tid.0) {
      Some(trans) => RoundTo {
        tid,
        trans_id: trans.id(),
        coord: trans.coord,
      },
      None => return Err(IcError::new(err::IC_ERROR_TRANSACTION_ACTIVE)),
    };
    let mut scratch = Scratch {
      key: std::mem::take(&mut self.key_scratch),
      attr: std::mem::take(&mut self.attr_scratch),
      bytes: std::mem::take(&mut self.blob_bytes),
    };
    let result = self.define_round_ops(qid, &to, run, values, &mut scratch);
    self.key_scratch = scratch.key;
    self.attr_scratch = scratch.attr;
    self.blob_bytes = scratch.bytes;
    if result.is_ok() {
      self.queue_for_send(tid);
    }
    result
  }

  /// The operations of a round, built in `scratch`.
  fn define_round_ops(
    &mut self,
    qid: QueryId,
    to: &RoundTo,
    run: &mut BlobRun,
    values: Option<&BlobValues>,
    scratch: &mut Scratch,
  ) -> Result<(), IcError> {
    let main_table = Arc::clone(&run.main_table);
    // Until the primary key is known, the main row is reached through
    // the unique index.
    let mut through: Option<Arc<IndexDef>> = None;
    if !run.key_known {
      through = run.index.clone();
    }
    if run.heads_due {
      run.heads_due = false;
      // Exclusive, so that every operation locks the main row before
      // its parts; a write's ignores a row not there.
      let mut abort_option = run.main_flags.abort_option;
      if run.kind == RunKind::Write {
        abort_option = key_ops::IC_IGNORE_ERROR;
      }
      let flags = KeyOpFlags {
        operation: key_ops::IC_OP_READ_EXCLUSIVE,
        abort_option,
        ..KeyOpFlags::default()
      };
      let slot = self.main_slot(run)?;
      let req = OpReq {
        slot,
        of: part_of(qid, run),
        table: &main_table,
        index: through.as_deref(),
        flags,
        is_read: true,
        key: &run.main_key,
        attr: &run.heads_info,
        dist_key: None,
      };
      self.define_op(to, &req)?;
      run.round_ops.push((slot, OpRole::Heads));
      run.ops_out += 1;
    }
    if run.main_due {
      run.main_due = false;
      let slot = self.main_slot(run)?;
      let req = OpReq {
        slot,
        of: part_of(qid, run),
        table: &main_table,
        index: through.as_deref(),
        flags: run.main_flags,
        is_read: run.kind.is_read(),
        key: &run.main_key,
        attr: &run.main_attr,
        dist_key: None,
      };
      self.define_op(to, &req)?;
      run.round_ops.push((slot, OpRole::Main));
      run.ops_out += 1;
    }
    let mut i: usize = 0;
    while i < run.plan.len() {
      let op = run.plan[i];
      i += 1;
      let target = run.targets[op.value];
      let slot = self.part_slot(&mut run.columns[target.column])?;
      let column = &run.columns[target.column];
      let (parts, part_column) = match (&column.parts, column.part_column) {
        (Some(parts), Some(part_column)) => (Arc::clone(parts), part_column),
        _ => return Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED)),
      };
      let part = op.part as u32;
      blob::part_key(&run.main_key, part, &mut scratch.key);
      scratch.attr.clear();
      let mut flags = KeyOpFlags {
        operation: key_ops::IC_OP_DELETE,
        abort_option: key_ops::IC_ABORT_ON_ERROR,
        ..KeyOpFlags::default()
      };
      let mut is_read = false;
      match op.kind {
        // A simple read of the part's bytes, by bitmask; a part not
        // there is the value's damage, not the transaction's.
        PartOpKind::Read => {
          blob::masked_read_info(&[column.keys + 2], &mut scratch.attr);
          flags = KeyOpFlags {
            operation: key_ops::IC_OP_READ,
            simple: true,
            abort_option: key_ops::IC_IGNORE_ERROR,
            ..KeyOpFlags::default()
          };
          is_read = true;
        }
        PartOpKind::Write => {
          let value: &[u8] = match (values, target.slot) {
            (Some(values), Some(slot)) => values
              .nth(slot)
              .and_then(|field| field.value)
              .unwrap_or_default(),
            _ => &[],
          };
          let sizes = run.values[op.value].sizes;
          let start = sizes.part_offset(op.part) as usize;
          let len = sizes.part_len(value.len() as u64, op.part);
          if start + len > value.len() {
            return Err(IcError::new(err::IC_ERROR_INCONSISTENT_DATA));
          }
          scratch.attr.extend_from_slice(&run.key_attrs);
          scratch
            .attr
            .push(column_header::column_header(column.keys, 4));
          scratch.attr.push(part);
          scratch
            .attr
            .push(column_header::column_header(column.keys + 1, 4));
          scratch.attr.push(0);
          blob::push_part_value(
            part_column,
            column.keys + 2,
            &value[start..start + len],
            &mut scratch.bytes,
            &mut scratch.attr,
          );
          flags.operation = key_ops::IC_OP_WRITE;
        }
        PartOpKind::Delete => {}
        // Deleted if it is there; its absence is the answer looked for.
        PartOpKind::Probe => flags.abort_option = key_ops::IC_IGNORE_ERROR,
      }
      let req = OpReq {
        slot,
        of: part_of(qid, run),
        table: &parts,
        index: None,
        flags,
        is_read,
        key: &scratch.key,
        attr: &scratch.attr,
        dist_key: None,
      };
      self.define_op(to, &req)?;
      run.round_ops.push((slot, OpRole::Part(op)));
      run.ops_out += 1;
    }
    // The unlock, last, named by the lock reference: its first word the
    // distribution key, its other two the key. Not waited for.
    if run.unlock_due {
      run.unlock_due = false;
      if let Some(lock) = run.lock_ref {
        let slot = self.main_slot(run)?;
        let flags = KeyOpFlags {
          operation: key_ops::IC_OP_UNLOCK,
          dist_key: true,
          abort_option: key_ops::IC_ABORT_ON_ERROR,
          ..KeyOpFlags::default()
        };
        let key = [lock[1], lock[2]];
        let req = OpReq {
          slot,
          of: part_of(qid, run),
          table: &main_table,
          index: None,
          flags,
          is_read: false,
          key: &key,
          attr: &[],
          dist_key: Some(lock[0]),
        };
        self.define_op(to, &req)?;
        run.round_ops.push((slot, OpRole::Unlock));
      }
    }
    Ok(())
  }

  /// Pack one operation's request and define it on the transaction.
  fn define_op(
    &mut self,
    to: &RoundTo,
    req: &OpReq<'_>,
  ) -> Result<(), IcError> {
    // Through a unique index the request names the index's own table,
    // and the coordinator finds the row through it.
    let (signal, table_id, table_version) = match req.index {
      Some(index) => (
        numbers::IC_SIG_UNIQUE_OP_REQ,
        index.index_id(),
        index.index_version(),
      ),
      None => (
        numbers::IC_SIG_KEY_OP_REQ,
        req.table.table_id(),
        req.table.table_version(),
      ),
    };
    let request = KeyOpReq {
      coord_ptr: to.coord.coord_ptr,
      query_ref: req.slot.wire_word(),
      table_id,
      flags_word: req.flags.flags_word(),
      table_version,
      trans_id_lo: to.trans_id as u32,
      trans_id_hi: (to.trans_id >> 32) as u32,
    };
    let header =
      SignalHeader::new(signal, self.block_number(), to.coord.coord_block);
    let both: [&[u32]; 2] = [req.key, req.attr];
    let mut sections: &[&[u32]] = &both;
    if req.attr.is_empty() {
      sections = &both[..1];
    }
    let eight = request.encode();
    let nine;
    let mut words: &[u32] = &eight;
    if let Some(dist_key) = req.dist_key {
      nine = request.encode_dist_key(dist_key);
      words = &nine;
    }
    let staged = self.stage_signal(
      to.coord.node_id,
      to.coord.generation,
      &header,
      words,
      sections,
    )?;
    if let Some(op) = self.queries.get_mut(req.slot.0) {
      op.begin(
        Execution {
          trans: Some(to.tid),
          trans_id: to.trans_id,
          flags: req.flags,
          staged,
          is_read: req.is_read,
          part_of: Some(req.of),
          ..Execution::default()
        },
        0,
        None,
        0,
      );
    }
    if let Some(trans) = self.transactions.get_mut(to.tid.0) {
      trans.defined.push(req.slot);
    }
    Ok(())
  }

  /// An operation over the main table free for the round being
  /// defined: one kept from before that is idle, or a new one.
  fn main_slot(&mut self, run: &mut BlobRun) -> Result<QueryId, IcError> {
    while run.main_cursor < run.main_ops.len() {
      let id = run.main_ops[run.main_cursor];
      run.main_cursor += 1;
      if self.slot_free(id) {
        return Ok(id);
      }
    }
    let id = self.insert_query(ApidQuery::internal(&run.main_table)?)?;
    run.main_ops.push(id);
    run.main_cursor = run.main_ops.len();
    Ok(id)
  }

  /// A part operation of `column` free for the round being defined.
  fn part_slot(&mut self, column: &mut ColumnRun) -> Result<QueryId, IcError> {
    while column.cursor < column.ops.len() {
      let id = column.ops[column.cursor];
      column.cursor += 1;
      if self.slot_free(id) {
        return Ok(id);
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

  /// True if an operation kept is neither defined nor out.
  fn slot_free(&self, id: QueryId) -> bool {
    match self.queries.get(id.0) {
      Some(op) => {
        let state = op.state();
        state == QueryState::Idle || state == QueryState::Completed
      }
      None => false,
    }
  }

  /// An operation of a run has completed, `op` itself, with `error` if
  /// it failed and what its refusal said beyond the code.
  pub(crate) fn blob_op_done(
    &mut self,
    op: QueryId,
    of: PartOf,
    error: Option<IcError>,
    error_data: u32,
  ) {
    let (role, kind) = match self.queries.get(of.owner.0) {
      Some(query) => match &query.run {
        Some(run) if run.active && run.round == of.round => {
          match run.round_ops.get(of.index as usize) {
            Some((slot, role)) if *slot == op => (*role, run.kind),
            _ => return,
          }
        }
        _ => return,
      },
      None => return,
    };
    // The unlock is not waited for: the query completes with its parts.
    if role == OpRole::Unlock {
      return;
    }
    // A read's row stays with its operation; the run takes what it needs
    // from it, and gives the buffer back.
    let reads = match role {
      OpRole::Heads => true,
      OpRole::Main => kind.is_read(),
      OpRole::Part(part) => part.kind == PartOpKind::Read,
      OpRole::Unlock => false,
    };
    let mut row: Vec<u32> = Vec::new();
    if reads && error.is_none() {
      if let Some(query) = self.queries.get_mut(op.0) {
        row = std::mem::take(&mut query.execution.row);
      }
    }
    match role {
      OpRole::Main if kind.is_read() => {
        self.blob_read_done(of.owner, &row, error, error_data);
      }
      OpRole::Part(part) if part.kind == PartOpKind::Read => {
        self.blob_part_read(of.owner, part, &row, error, error_data);
      }
      _ => self.blob_write_done(of.owner, role, &row, error, error_data),
    }
    row.clear();
    if let Some(query) = self.queries.get_mut(op.0) {
      if query.execution.row.capacity() == 0 {
        query.execution.row = row;
      }
    }
    self.blob_round_check(of.owner);
  }

  /// A write's, an update's or a delete's operation has completed.
  fn blob_write_done(
    &mut self,
    owner: QueryId,
    role: OpRole,
    row: &[u32],
    error: Option<IcError>,
    error_data: u32,
  ) {
    let run = match self.queries.get_mut(owner.0) {
      Some(query) => match query.run.as_mut() {
        Some(run) => run,
        None => return,
      },
      None => return,
    };
    run.ops_out = run.ops_out.saturating_sub(1);
    let missing =
      matches!(error, Some(e) if e.code == IC_NDB_ERROR_NO_SUCH_ROW as i32);
    match (role, error) {
      (OpRole::Heads, None) => {
        // A delete's main row goes in the next round, with the parts
        // its heads have told of, and so does an update's through an
        // index, by the primary key they brought.
        if run.kind == RunKind::Delete || !run.key_known {
          run.main_due = true;
        }
        run.take_heads(row);
      }
      // A write finds no row: it inserts one, with no old parts to
      // delete; asked to, it probes for parts an insert racing it may
      // have left.
      (OpRole::Heads, Some(_)) if missing && run.kind == RunKind::Write => {
        if run.probe {
          for value in &mut run.values {
            value.no_row();
          }
        }
      }
      (OpRole::Part(part), None) if part.kind == PartOpKind::Probe => {
        run.values[part.value].probe_found(part.part);
      }
      // A probed part not there: the probing is over.
      (OpRole::Part(part), Some(_))
        if part.kind == PartOpKind::Probe && missing => {}
      (_, Some(e)) => run.fail(e, error_data),
      _ => {}
    }
  }

  /// A read of the main row has completed: its fields into the query's
  /// row, its blob values' heads and inline bytes into its values, and
  /// what comes next planned. A committed read that found parts reads
  /// the row again, locked, before them; a read that locked first lets
  /// the lock go once its parts are read.
  fn blob_read_done(
    &mut self,
    owner: QueryId,
    row: &[u32],
    error: Option<IcError>,
    error_data: u32,
  ) {
    let query = match self.queries.get_mut(owner.0) {
      Some(query) => query,
      None => return,
    };
    let mut run = match query.run.take() {
      Some(run) => run,
      None => return,
    };
    run.ops_out = run.ops_out.saturating_sub(1);
    let unpacked = match error {
      Some(e) => Err((e, error_data)),
      None => match query.unpack_blob_row(row) {
        Ok(used) => Ok(used),
        Err(e) => Err((e, 0)),
      },
    };
    match unpacked {
      Err((e, data)) => run.fail(e, data),
      Ok(used) => {
        if run.main_locks {
          match lock_ref_after(row, used) {
            Some(lock) => run.lock_ref = Some(lock),
            None => run.fail(IcError::new(err::IC_ERROR_INCONSISTENT_DATA), 0),
          }
        }
        // Through an index, the primary key the parts go by.
        if !run.key_known && !run.pk_info.is_empty() {
          if let Err(e) = run.take_primary_key(row, used) {
            run.fail(e, 0);
          }
        }
        // Each value's length, now known: its parts to be read.
        let mut parts = false;
        let mut index: usize = 0;
        while index < run.values.len() {
          let sizes = run.values[index].sizes;
          let mut length: u64 = 0;
          if let (Some(values), Some(slot)) =
            (query.blob_values(), run.targets[index].slot)
          {
            if let Some(value) = values.nth(slot).and_then(|field| field.value)
            {
              length = value.len() as u64;
            }
          }
          run.values[index] = ValueParts::read_value(sizes, length);
          parts |= run.values[index].parts() > 0;
          index += 1;
        }
        if run.kind == RunKind::CommittedRead && !run.main_locks && parts {
          // Read again, locked, so that the parts are the head's.
          let abort_option = run.main_flags.abort_option;
          run.main_attr.clear();
          row_codec::read_attr_info_append(
            query.attr_record(),
            &mut run.main_attr,
          );
          run.lock_read(abort_option);
          run.main_due = true;
          run.last = false;
        } else {
          run.unlock_due = run.lock_ref.is_some();
          run.last = !parts && !run.unlock_due;
        }
      }
    }
    query.run = Some(run);
  }

  /// A part read has completed: its bytes into the value, at its
  /// offset. A part not there is a damaged value (4267).
  fn blob_part_read(
    &mut self,
    owner: QueryId,
    part: PartOp,
    row: &[u32],
    error: Option<IcError>,
    error_data: u32,
  ) {
    let query = match self.queries.get_mut(owner.0) {
      Some(query) => query,
      None => return,
    };
    let mut run = match query.run.take() {
      Some(run) => run,
      None => return,
    };
    run.ops_out = run.ops_out.saturating_sub(1);
    let copied = match error {
      Some(e) if e.code == IC_NDB_ERROR_NO_SUCH_ROW as i32 => {
        Err((IcError::new(blob::IC_NDB_ERROR_BLOB_DAMAGED), 0))
      }
      Some(e) => Err((e, error_data)),
      None => copy_part(query, &run, part, row),
    };
    if let Err((e, data)) = copied {
      run.fail(e, data);
    }
    query.run = Some(run);
  }

  /// Once a round is done: complete the query if it was the last, or if
  /// an operation failed; otherwise leave the next round to the poll.
  fn blob_round_check(&mut self, qid: QueryId) {
    let (done, finish) = match self.queries.get(qid.0) {
      Some(query) => match &query.run {
        Some(run) if run.active && run.started && run.after.is_none() => {
          (run.ops_out == 0, run.last || run.error.is_some())
        }
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

  /// Complete a BLOB query whose rounds are over: with what failed an
  /// operation of it, or done.
  fn complete_blob_query(&mut self, qid: QueryId) {
    if let Some(query) = self.queries.get_mut(qid.0) {
      let mut error: Option<(IcError, u32)> = None;
      if let Some(run) = &query.run {
        error = run.error;
      }
      query.set_result_len(0);
      match error {
        Some((e, data)) => query.refuse(e, data),
        None => query.set_state(QueryState::Completed),
      }
    }
    self.finish_query(qid);
  }

  /// End a query's run, if it has one going: the query has completed,
  /// or was dropped, or will be failed by its transaction's end. A BLOB
  /// query on the same row waiting for it starts with the next poll,
  /// and the commit it held may go once no other holds it.
  pub(crate) fn end_blob_run(&mut self, qid: QueryId) {
    let (tid, held) = match self.queries.get_mut(qid.0) {
      Some(query) => {
        let tid = query.execution.trans;
        match query.run.as_mut() {
          Some(run) if run.active => {
            run.active = false;
            run.after = None;
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
    let tid = match tid {
      Some(tid) => tid,
      None => return,
    };
    let waiting = match self.transactions.get_mut(tid.0) {
      Some(trans) => {
        trans.remove_blob_run(qid);
        trans.blob_runs().to_vec()
      }
      None => Vec::new(),
    };
    for other in waiting {
      if let Some(query) = self.queries.get_mut(other.0) {
        if let Some(run) = query.run.as_mut() {
          if run.after == Some(qid) {
            run.after = None;
            self.blob_due.push(other);
          }
        }
      }
    }
    if held {
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
      self.send_due = true;
    }
  }

  /// What a poll does for BLOB queries, once it has taken its replies:
  /// plan and define the next round of each whose round is done, and
  /// the first of each that waited for another on its row, and send
  /// them, with any commit they let go.
  pub(crate) fn drive_blobs(&mut self) {
    // A run that ends here may let another on its row start, which the
    // same poll then starts too.
    while !self.blob_due.is_empty() {
      let mut due = std::mem::take(&mut self.blob_due);
      let mut i: usize = 0;
      while i < due.len() {
        self.next_blob_round(due[i]);
        i += 1;
      }
      due.clear();
      if self.blob_due.is_empty() {
        self.blob_due = due;
        break;
      }
    }
    if self.send_due {
      self.send_due = false;
      // A send that fails fails its transactions at the next poll, as
      // their link is gone.
      let _ = self.send_queries(false);
    }
  }

  /// The next round of a query whose round is done and was not its
  /// last, or the first of one that waited for another on its row.
  fn next_blob_round(&mut self, qid: QueryId) {
    let tid = match self.queries.get(qid.0) {
      Some(query) => {
        let state = query.state();
        let defined = state == QueryState::Defined || state == QueryState::Sent;
        match (&query.run, query.execution.trans) {
          (Some(run), Some(tid))
            if run.active
              && run.after.is_none()
              && run.ops_out == 0
              && defined =>
          {
            tid
          }
          _ => return,
        }
      }
      None => return,
    };
    let (goes_on, ended, trans_error, commit_asked) =
      match self.transactions.get(tid.0) {
        Some(trans) => (
          trans.takes_blob_rounds(),
          trans.ended(),
          trans.error(),
          trans.commit_asked(),
        ),
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
    let later = run.started;
    // A committed read that waited behind another on its row is not its
    // transaction's only operation: it takes the lock first.
    if run.undecided {
      run.undecided = false;
      run.lock_first();
    }
    let mut alone = false;
    if later {
      if run.kind.is_read() && run.main_due {
        // A committed read's row read again, locked: its parts wait for
        // the lengths that read brings, and its commit with them.
        run.plan.clear();
        run.last = false;
      } else {
        run.last = blob::plan_part_ops(&mut run.values, budget, &mut run.plan);
      }
      // No unlock once a commit is asked for: the commit lets the lock
      // go.
      if run.unlock_due && commit_asked {
        run.unlock_due = false;
      }
      if run.plan.is_empty() && !run.main_due {
        if !run.unlock_due {
          // Nothing left to do: the query is done, and a commit it held
          // goes alone.
          self.put_run(qid, run, values);
          self.complete_blob_query(qid);
          return;
        }
        // The unlock alone, not waited for: the query is done.
        alone = true;
      }
    }
    let defined = self.define_round(qid, tid, &mut run, values.as_ref());
    let mut release = false;
    if defined.is_ok() {
      if run.last || alone {
        if run.holds_commit {
          run.holds_commit = false;
          release = true;
        }
      } else if !run.holds_commit {
        // A run that plans a further round holds the commit.
        run.holds_commit = true;
        if let Some(trans) = self.transactions.get_mut(tid.0) {
          trans.hold_commit();
        }
      }
    }
    self.put_run(qid, run, values);
    match defined {
      Ok(()) => {
        if release {
          self.release_blob_commit(tid);
        }
        if later {
          self.blob_rounds += 1;
        }
        self.send_due = true;
        if alone {
          self.complete_blob_query(qid);
        }
      }
      Err(e) => {
        // A value's head may be in without its parts: the transaction
        // must not commit.
        self.force_rollback(tid);
        if let Some(query) = self.queries.get_mut(qid.0) {
          query.fail(e);
        }
        self.finish_query(qid);
        self.send_due = true;
      }
    }
  }

  /// Free a query's operations with it. Refused while one is defined
  /// or out (`IC_ERROR_TRANSACTION_ACTIVE`).
  pub(crate) fn free_blob_ops(&mut self, qid: QueryId) -> Result<(), IcError> {
    let ops = match self.queries.get(qid.0) {
      Some(query) => match &query.run {
        Some(run) => run.op_ids(),
        None => return Ok(()),
      },
      None => return Ok(()),
    };
    for op in &ops {
      if self.queries.get(op.0).is_some() && !self.slot_free(*op) {
        return Err(IcError::new(err::IC_ERROR_TRANSACTION_ACTIVE));
      }
    }
    for op in ops {
      self.queries.remove(op.0);
    }
    Ok(())
  }
}

/// The buffers a round's requests are built in, the connection's own,
/// kept for their allocation.
struct Scratch {
  key: Vec<u32>,
  attr: Vec<u32>,
  bytes: Vec<u8>,
}

/// What the next operation of a round carries to name its query and
/// its place in the round.
fn part_of(qid: QueryId, run: &BlobRun) -> PartOf {
  PartOf {
    owner: qid,
    round: run.round,
    index: run.round_ops.len() as u32,
  }
}

/// The lock reference a locked read brought after its row, which took
/// `used` words: the pseudo column's header and its three words.
fn lock_ref_after(
  row: &[u32],
  used: usize,
) -> Option<[u32; IC_LOCK_REF_WORDS]> {
  let header = *row.get(used)?;
  let size = (4 * IC_LOCK_REF_WORDS) as u32;
  if column_header::column_id(header) != IC_COL_LOCK_REF
    || column_header::byte_size(header) != size
  {
    return None;
  }
  let words = row.get(used + 1..used + 1 + IC_LOCK_REF_WORDS)?;
  Some([words[0], words[1], words[2]])
}

/// A part a read brought, in `row`, into `query`'s value at its offset.
fn copy_part(
  query: &mut ApidQuery,
  run: &BlobRun,
  part: PartOp,
  row: &[u32],
) -> Result<(), (IcError, u32)> {
  let failed = |e: IcError| (e, 0);
  let target = run.targets[part.value];
  let column = &run.columns[target.column];
  let (parts, part_column) = match (&column.parts, column.part_column) {
    (Some(parts), Some(part_column)) => (parts, part_column),
    _ => return Err(failed(IcError::new(err::IC_ERROR_NOT_SUPPORTED))),
  };
  let value = run.values[part.value];
  let offset = value.sizes.part_offset(part.part) as usize;
  let len = value.sizes.part_len(value.length, part.part);
  let read =
    blob::parse_part_row(parts, column.keys + 2, row).map_err(failed)?;
  let bytes = blob::decode_part(part_column, len, read).map_err(failed)?;
  let slot = match target.slot {
    Some(slot) => slot,
    None => return Err(failed(IcError::new(err::IC_ERROR_NO_SUCH_FIELD))),
  };
  let into = query
    .blob_values_mut()
    .and_then(|values| values.part_buf(slot, offset, len));
  match into {
    Some(into) => {
      into.copy_from_slice(bytes);
      Ok(())
    }
    None => Err(failed(IcError::new(blob::IC_NDB_ERROR_BLOB_DAMAGED))),
  }
}
