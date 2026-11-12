// Copyright (c) 2007-2015 Hopsworks AB.
// Copyright (c) 2026 Hopsworks and/or its affiliates.
// Licensed under the MIT License. See LICENSE in the repository root.

//! The query object (`main:api/ic_apid_op.ic`, `IC_INT_APID_QUERY`;
//! `IC_APID_QUERY` in `ic_apid.h`): one key operation, defined once and
//! used many times.
//!
//! A query ties a table to a key record and an attribute record, and
//! **owns the rows they lay out.** The application fills the key row,
//! and the attribute row for a write, defines the query on a
//! transaction, and reads the attribute row when the query has
//! completed. Nothing in the library holds a pointer into the
//! application's memory, which is what lets the rows be read during a
//! send and written during a poll without any unsafe code; the C header
//! passes the rows with each call, and its binding will copy or map
//! them onto these. Decided here, 2026-09-22, and noted in chapter 04.
//!
//! A query is idle, then defined on a transaction, then sent, then
//! completed, and is idle again once the application has taken it from
//! the connection's list of executed queries. The redesign of the C's
//! field-by-field binding into records is chapter 04's.
//!
//! **A query may run a program** (chapter 04, "Conditions and
//! assignments"): kept beside its records until changed, with its
//! inputs beside its rows, and for an update a record of the columns
//! read after the program, into a row of their own.
//!
//! **A query may hold blob values** (chapter 06, phase 12): when its
//! attribute record names blob fields, their values are kept beside its
//! rows, NULL until set, and its operation goes in rounds, as does a
//! delete from a table with blob columns: every request of it is an
//! operation of the driver's own (`blob_run`), the query itself going
//! on no wire. Those operations are queries too, in the same table, so
//! that their replies are routed as any query's are; they are the
//! connection's, and the program never sees them.

use std::sync::Arc;

use ic_port::err;
use ic_port::IcError;
use ic_signals::key_ops;
use ic_util::ptr_array::PtrId;

use crate::apid_conn::ApidConnection;
use crate::blob::BlobValues;
use crate::blob_run::BlobRun;
use crate::dict_cache::IndexDef;
use crate::dict_cache::TableDef;
use crate::program;
use crate::program::Program;
use crate::program::IC_PROGRAM_INPUTS;
use crate::record::Record;

/// A query's id in its connection. A data node names the query by a
/// word made from it ([`QueryId::wire_word`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryId(pub(crate) PtrId);

impl QueryId {
  /// The word a request carries and a reply echoes.
  pub(crate) fn wire_word(&self) -> u32 {
    self.0.wire_word()
  }

  /// The id as a 64-bit word, for a language binding.
  pub fn as_u64(&self) -> u64 {
    self.0.as_u64()
  }

  /// A query id from its word, as a language binding hands it back.
  /// Whether it still names a live query is for the connection to say;
  /// one freed never does again.
  pub fn from_u64(word: u64) -> QueryId {
    QueryId(PtrId::from_u64(word))
  }
}

/// A transaction's id in its connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TransId(pub(crate) PtrId);

impl TransId {
  /// The id as a 64-bit word, for a language binding.
  pub fn as_u64(&self) -> u64 {
    self.0.as_u64()
  }

  /// A transaction id from its word, as a language binding hands it
  /// back. Whether it still names a live transaction is for the
  /// connection to say; one closed never does again.
  pub fn from_u64(word: u64) -> TransId {
    TransId(PtrId::from_u64(word))
  }
}

/// What is called when a query completes (`IC_APID_CALLBACK`):
/// on the thread that polls, from inside the poll, with the connection,
/// the query's id, and the `user_ref` the query was defined with. The
/// query is by id since it lives in the connection, which the callback
/// has. Inside the callback the query is completed, with its error or
/// its row, and its transaction's state is settled as far as this
/// query decides it; the callback may define queries, start, commit
/// and send, but a poll inside it does nothing. When it returns the
/// query is idle, unless the callback defined it anew. A query with no
/// callback goes on the executed list instead.
pub type QueryCallback = fn(&mut ApidConnection, QueryId, usize);

/// How a read locks (`IC_READ_KEY_QUERY_TYPE`), in the header's order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadKind {
  /// A shared lock taken and let go of in the read itself. Through a
  /// unique index the lock is held until the transaction ends, as for
  /// `Locked`: the coordinator takes no simple read through an index.
  /// Verify: chapter 05 §6.8.
  #[default]
  Simple = 0,
  /// A shared lock held until the transaction ends.
  Locked = 1,
  /// An exclusive lock held until the transaction ends.
  Exclusive = 2,
  /// The last committed value, with no lock at all. Through a unique
  /// index a shared lock is taken and held until the transaction ends,
  /// and a backup replica may serve the read. Verify: chapter 05 §6.8.
  Committed = 3,
  /// As exclusive, but failing rather than waiting for another's lock.
  ExclusiveNoWait = 4,
}

/// What a write does (`IC_WRITE_KEY_QUERY_TYPE`), in the header's order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteKind {
  /// The row must be there.
  #[default]
  Update = 0,
  /// Insert, or overwrite what is there.
  Write = 1,
  /// The row must not be there.
  Insert = 2,
  /// Delete the row.
  Delete = 3,
}

/// What a failure does to the transaction (`IC_ABORT_OPTION`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AbortOption {
  /// Abort on error for a write, ignore it for a read.
  #[default]
  Default = 0,
  /// The transaction is rolled back.
  AbortOnError = 1,
  /// The query fails alone, and the transaction goes on.
  IgnoreError = 2,
}

/// RonDB's batching hint on a write (`IC_BATCH_HINT`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BatchHint {
  /// Leave it to the data node.
  #[default]
  None = 0,
  /// The write does not depend on earlier ones in its batch.
  Safe = 1,
  /// It does.
  Unsafe = 2,
}

/// What a read asks for (`IC_READ_KEY_ARGS`, less the rows, which the
/// query owns).
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadKeyArgs {
  /// How to lock.
  pub kind: ReadKind,
  /// What a failure does to the transaction. A committed read can only
  /// ignore errors.
  pub abort_option: AbortOption,
  /// Anything, given back with the completed query.
  pub user_ref: usize,
  /// Called when the query completes, in place of the executed list.
  pub callback: Option<QueryCallback>,
  /// A word for a language binding's own use, read back with
  /// [`ApidQuery::binding`]: the C API keeps the C callback there for
  /// its trampoline. Rust callers leave it 0.
  pub binding: usize,
}

/// What a write asks for (`IC_WRITE_KEY_ARGS`, less the rows).
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteKeyArgs {
  /// What to do.
  pub kind: WriteKind,
  /// What a failure does to the transaction.
  pub abort_option: AbortOption,
  /// RonDB's batching hint.
  pub batch_hint: BatchHint,
  /// Anything, given back with the completed query.
  pub user_ref: usize,
  /// Called when the query completes, in place of the executed list.
  pub callback: Option<QueryCallback>,
  /// A word for a language binding's own use; see
  /// [`ReadKeyArgs::binding`].
  pub binding: usize,
}

/// Where a query is in its life.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QueryState {
  /// Ready to be defined.
  #[default]
  Idle,
  /// Defined on a transaction and not yet sent.
  Defined,
  /// Sent, its replies not all in.
  Sent,
  /// Done, and waiting to be taken from the executed list.
  Completed,
}

/// One execution's state, between define and completion.
#[derive(Clone, Debug, Default)]
pub(crate) struct Execution {
  /// The transaction it was defined on.
  pub trans: Option<TransId>,
  /// The transaction's id, which every reply must carry.
  pub trans_id: u64,
  /// The flags apart from start, execute and commit, which are set when
  /// it is sent.
  pub flags: key_ops::KeyOpFlags,
  /// Where its request lies, packed when it was defined, until sent.
  pub staged: crate::apid_conn::Staged,
  /// True for a read, whose row is to be put into the attribute row.
  pub is_read: bool,
  /// True for an update that reads columns after its program, whose
  /// row is to be put into the row for them.
  pub reads_after: bool,
  /// When a read was sent, for detecting a replaced reading-node link.
  pub sent_at: ic_port::time::IcTimer,
  /// What the coordinator said the operation would bring, once it has.
  pub confirmed: Option<key_ops::KeyOpResult>,
  /// The row, as it has come so far.
  pub row: Vec<u32>,
  /// True once any of the row has come.
  pub has_row: bool,
  /// Words of row that have come, whether kept in `row` or unpacked.
  pub row_words: u32,
  /// True once the row has been unpacked into the attribute row
  /// straight from the signal it came in.
  pub unpacked: bool,
  /// For a takeover of a scanned row, the receiver whose batch it is,
  /// which asks for more only once every takeover of the batch is done.
  pub takeover: Option<crate::scan::TakeoverOf>,
  /// For a part operation, the BLOB query it is for and that query's
  /// round.
  pub part_of: Option<PartOf>,
}

/// The BLOB query an operation of the driver's is for, the round it
/// went in, and its place in the round: a reply to an operation of an
/// earlier round is no longer the query's concern.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartOf {
  /// The query.
  pub owner: QueryId,
  /// Its round, as its run counts them.
  pub round: u32,
  /// Its place among the round's operations.
  pub index: u32,
}

/// True if the record is over this version of the table.
pub(crate) fn over_table(rec: &Record, table: &TableDef) -> bool {
  rec.table().table_id() == table.table_id()
    && rec.table().table_version() == table.table_version()
}

/// Refuse records that do not fit a key operation over `table`: both
/// must be over this version of it, and the key record must hold every
/// primary key column. A key record may name no blob field; which
/// operations take an attribute record naming one is for each to say
/// (chapter 06, phase 12).
pub(crate) fn check_records(
  table: &TableDef,
  key_rec: &Record,
  attr_rec: &Record,
) -> Result<(), IcError> {
  let fits = over_table(key_rec, table)
    && over_table(attr_rec, table)
    && key_rec.covers_primary_key();
  if !fits {
    return Err(IcError::new(err::IC_ERROR_KEY_RECORD));
  }
  key_rec.refuse_blob_fields()
}

/// As [`check_records`], for an operation through a unique index: the
/// index must be unique and the records over its table. That the key
/// record holds every indexed column is checked when the operation is
/// defined. Neither record may name a blob field until phase 12 goes
/// through unique indexes (chapter 06, 12.7).
pub(crate) fn check_unique_records(
  index: &IndexDef,
  key_rec: &Record,
  attr_rec: &Record,
) -> Result<(), IcError> {
  if !index.is_unique() {
    return Err(IcError::new(err::IC_ERROR_NOT_SUPPORTED));
  }
  let table = index.table();
  if !over_table(key_rec, table) || !over_table(attr_rec, table) {
    return Err(IcError::new(err::IC_ERROR_KEY_RECORD));
  }
  key_rec.refuse_blob_fields()?;
  attr_rec.refuse_blob_fields()
}

/// One key operation, defined once and used many times
/// (`IC_APID_QUERY`).
pub struct ApidQuery {
  table: Arc<TableDef>,
  /// The unique index the key goes through, for a unique query.
  index: Option<Arc<IndexDef>>,
  key_rec: Record,
  attr_rec: Record,
  key_row: Vec<u8>,
  attr_row: Vec<u8>,
  state: QueryState,
  error: Option<IcError>,
  /// What the refusal said beyond its code, or zero.
  error_data: u32,
  user_ref: usize,
  callback: Option<QueryCallback>,
  /// The binding's word the query was defined with.
  binding: usize,
  /// Bytes of row data the last read brought.
  result_len: u32,
  /// The program each execution runs, if any.
  program: Option<Arc<Program>>,
  /// The inputs it is given.
  inputs: [i64; IC_PROGRAM_INPUTS as usize],
  /// For an update, the columns read after its program, and the row
  /// they come into.
  after_rec: Option<Record>,
  after_row: Vec<u8>,
  /// The values of the attribute record's blob fields, if it names any.
  blobs: Option<BlobValues>,
  /// The run of its rounds, made at its first execution with blob
  /// values and kept, with its part operations, for the next.
  pub(crate) run: Option<Box<BlobRun>>,
  /// True for a part operation of the driver's own, which the program
  /// never sees.
  internal: bool,
  /// A write of a BLOB query that finds no row probes for parts a
  /// racing insert may have left ([`set_blob_probe`](Self::set_blob_probe)).
  blob_probe: bool,
  pub(crate) execution: Execution,
}

impl std::fmt::Debug for ApidQuery {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "ApidQuery({}, {:?}{})",
      self.table.name(),
      self.state,
      match self.error {
        Some(e) => format!(", error {}", e.code),
        None => String::new(),
      }
    )
  }
}

impl ApidQuery {
  /// A query over a table, with a record for its key and one for the
  /// attributes it reads or writes. Both must be over the same version
  /// of the table, and the key record must hold every primary key
  /// column.
  ///
  /// The attribute record may name blob fields, whose values the query
  /// holds ([`blob`](Self::blob)); inserts, writes and updates take
  /// them (chapter 06, 12.3 and 12.4), reads come in 12.5.
  pub fn new(
    table: &Arc<TableDef>,
    key_rec: &Record,
    attr_rec: &Record,
  ) -> Result<ApidQuery, IcError> {
    check_records(table, key_rec, attr_rec)?;
    let mut blobs: Option<BlobValues> = None;
    if attr_rec.has_blob_fields() {
      blobs = Some(BlobValues::new(attr_rec));
    }
    Ok(ApidQuery {
      table: Arc::clone(table),
      index: None,
      key_rec: key_rec.clone(),
      attr_rec: attr_rec.clone(),
      key_row: vec![0; key_rec.row_size() as usize],
      attr_row: vec![0; attr_rec.row_size() as usize],
      state: QueryState::Idle,
      error: None,
      error_data: 0,
      user_ref: 0,
      callback: None,
      binding: 0,
      result_len: 0,
      program: None,
      inputs: [0; IC_PROGRAM_INPUTS as usize],
      after_rec: None,
      after_row: Vec::new(),
      blobs,
      run: None,
      internal: false,
      blob_probe: false,
      execution: Execution::default(),
    })
  }

  /// A query through a unique index (`ic_apid_query_create_unique`).
  /// The records are over the index's table; the key record must hold
  /// every column the index is over, which is checked when the query is
  /// defined. Only reads, updates and deletes go through an index.
  pub fn new_unique(
    index: &Arc<IndexDef>,
    key_rec: &Record,
    attr_rec: &Record,
  ) -> Result<ApidQuery, IcError> {
    check_unique_records(index, key_rec, attr_rec)?;
    let table = index.table();
    Ok(ApidQuery {
      table: Arc::clone(table),
      index: Some(Arc::clone(index)),
      key_rec: key_rec.clone(),
      attr_rec: attr_rec.clone(),
      key_row: vec![0; key_rec.row_size() as usize],
      attr_row: vec![0; attr_rec.row_size() as usize],
      state: QueryState::Idle,
      error: None,
      error_data: 0,
      user_ref: 0,
      callback: None,
      binding: 0,
      result_len: 0,
      program: None,
      inputs: [0; IC_PROGRAM_INPUTS as usize],
      after_rec: None,
      after_row: Vec::new(),
      blobs: None,
      run: None,
      internal: false,
      blob_probe: false,
      execution: Execution::default(),
    })
  }

  /// A part operation of the driver's own, over a blob column's parts
  /// table: its sections are built by the driver, so its rows are left
  /// empty, and its records are the parts table's every column.
  pub(crate) fn internal(parts: &Arc<TableDef>) -> Result<ApidQuery, IcError> {
    let rec = Record::default_for(parts, None)?;
    Ok(ApidQuery {
      table: Arc::clone(parts),
      index: None,
      key_rec: rec.clone(),
      attr_rec: rec,
      key_row: Vec::new(),
      attr_row: Vec::new(),
      state: QueryState::Idle,
      error: None,
      error_data: 0,
      user_ref: 0,
      callback: None,
      binding: 0,
      result_len: 0,
      program: None,
      inputs: [0; IC_PROGRAM_INPUTS as usize],
      after_rec: None,
      after_row: Vec::new(),
      blobs: None,
      run: None,
      internal: true,
      blob_probe: false,
      execution: Execution::default(),
    })
  }

  /// True for a part operation of the driver's own.
  pub(crate) fn is_internal(&self) -> bool {
    self.internal
  }

  /// The table the query is over.
  pub fn table(&self) -> &Arc<TableDef> {
    &self.table
  }

  /// The unique index the key goes through, for a unique query.
  pub fn index(&self) -> Option<&Arc<IndexDef>> {
    self.index.as_ref()
  }

  /// The record the key row is laid out by.
  pub fn key_record(&self) -> &Record {
    &self.key_rec
  }

  /// The record the attribute row is laid out by.
  pub fn attr_record(&self) -> &Record {
    &self.attr_rec
  }

  /// The key row, to fill before defining the query.
  pub fn key_row(&self) -> &[u8] {
    &self.key_row
  }

  /// The key row, to fill before defining the query. Not to be changed
  /// while the query is defined or sent.
  pub fn key_row_mut(&mut self) -> &mut [u8] {
    &mut self.key_row
  }

  /// The attribute row: what a read brought, or what a write is to send.
  pub fn attr_row(&self) -> &[u8] {
    &self.attr_row
  }

  /// The attribute row, to fill before defining a write.
  pub fn attr_row_mut(&mut self) -> &mut [u8] {
    &mut self.attr_row
  }

  /// Where the query is in its life.
  pub fn state(&self) -> QueryState {
    self.state
  }

  /// True if the last execution failed (`ic_apid_query_is_failed`).
  pub fn is_failed(&self) -> bool {
    self.error.is_some()
  }

  /// Why the last execution failed. An NDB error comes as its code: 626
  /// for a row that is not there, 630 for one that already is.
  pub fn error(&self) -> Option<IcError> {
    self.error
  }

  /// What the data node said about the last execution's refusal beyond
  /// its code, or zero: for 893, a duplicate key, the id of the unique
  /// index whose value was taken. Verify: chapter 05 §6.8.
  pub fn error_data(&self) -> u32 {
    self.error_data
  }

  /// What was given with the arguments of the last execution.
  pub fn user_ref(&self) -> usize {
    self.user_ref
  }

  /// The callback of the last execution, if it had one.
  pub fn callback(&self) -> Option<QueryCallback> {
    self.callback
  }

  /// The binding's word the query was defined with
  /// ([`ReadKeyArgs::binding`]).
  pub fn binding(&self) -> usize {
    self.binding
  }

  /// Bytes of row data the last read brought; zero for a row that was
  /// not there.
  pub fn result_len(&self) -> u32 {
    self.result_len
  }

  /// The transaction the query was last defined on.
  pub fn transaction(&self) -> Option<TransId> {
    self.execution.trans
  }

  /// Run `program` with every execution defined from now on, or none
  /// (chapter 04, "Conditions and assignments"). It must be for the
  /// query's table (`IC_ERROR_BAD_PROGRAM`). Which operations take it
  /// is checked when one is defined. Refused while the query is defined
  /// or sent (`IC_ERROR_TRANSACTION_ACTIVE`).
  pub fn set_program(
    &mut self,
    program: Option<&Arc<Program>>,
  ) -> Result<(), IcError> {
    self.check_idle()?;
    match program {
      Some(program) => {
        program::check_program(program, &self.table, true)?;
        self.program = Some(Arc::clone(program));
      }
      None => self.program = None,
    }
    Ok(())
  }

  /// The program each execution runs.
  pub fn program(&self) -> Option<&Arc<Program>> {
    self.program.as_ref()
  }

  /// Set input `input`, 0 to 15, to `value`, for the executions defined
  /// from now on; each holds what it was last set to, 0 at first
  /// (`IC_ERROR_BAD_PROGRAM` beyond 15).
  pub fn set_input(&mut self, input: u32, value: i64) -> Result<(), IcError> {
    match self.inputs.get_mut(input as usize) {
      Some(slot) => {
        *slot = value;
        Ok(())
      }
      None => Err(IcError::new(err::IC_ERROR_BAD_PROGRAM)),
    }
  }

  /// The inputs the program is given.
  pub fn inputs(&self) -> &[i64; IC_PROGRAM_INPUTS as usize] {
    &self.inputs
  }

  /// Every input at once, as an executor's operation gives them.
  pub(crate) fn set_inputs(
    &mut self,
    inputs: &[i64; IC_PROGRAM_INPUTS as usize],
  ) {
    self.inputs = *inputs;
  }

  /// For an update, read the columns of `rec` after its program, into
  /// [`after_row`](Self::after_row), or none. The record must be over
  /// the query's table (`IC_ERROR_KEY_RECORD`); only an update takes
  /// it, which is checked when it is defined. Refused while the query is
  /// defined or sent (`IC_ERROR_TRANSACTION_ACTIVE`).
  pub fn set_reads_after(
    &mut self,
    rec: Option<&Record>,
  ) -> Result<(), IcError> {
    self.check_idle()?;
    match rec {
      Some(rec) => {
        if !over_table(rec, &self.table) {
          return Err(IcError::new(err::IC_ERROR_KEY_RECORD));
        }
        rec.refuse_blob_fields()?;
        self.after_row = vec![0; rec.row_size() as usize];
        self.after_rec = Some(rec.clone());
      }
      None => {
        self.after_rec = None;
        self.after_row = Vec::new();
      }
    }
    Ok(())
  }

  /// The record of the columns an update reads after its program.
  pub fn after_record(&self) -> Option<&Record> {
    self.after_rec.as_ref()
  }

  /// What an update read after its program, laid out by
  /// [`after_record`](Self::after_record); empty without one.
  pub fn after_row(&self) -> &[u8] {
    &self.after_row
  }

  /// The value of the blob field at `position` of the attribute record
  /// (`ic_apid_query_blob`): `None` for NULL. What a write is to send,
  /// or once a read has completed, what it read. Every value is NULL
  /// until set. `IC_ERROR_NO_SUCH_FIELD` when the position names no
  /// blob field.
  pub fn blob(&self, position: u32) -> Result<Option<&[u8]>, IcError> {
    match &self.blobs {
      Some(values) => values.blob(position),
      None => Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    }
  }

  /// Set the blob field at `position` to a copy of `value`, for the
  /// executions defined from now on (`ic_apid_query_set_blob`). Refused
  /// with 4266 when the value is longer than its column's MySQL type
  /// holds, or than a column without parts holds inline
  /// ([`BlobSizes::holds`](crate::blob::BlobSizes::holds)), and while
  /// the query is defined or sent (`IC_ERROR_TRANSACTION_ACTIVE`), since
  /// its rounds read the value as they go.
  pub fn set_blob(
    &mut self,
    position: u32,
    value: &[u8],
  ) -> Result<(), IcError> {
    self.check_idle()?;
    self.values_mut()?.set_blob(position, value)
  }

  /// Set the blob field at `position` NULL
  /// (`ic_apid_query_set_blob_null`).
  pub fn set_blob_null(&mut self, position: u32) -> Result<(), IcError> {
    self.check_idle()?;
    self.values_mut()?.set_blob_null(position)
  }

  /// The buffer of the blob field at `position`, to fill in place
  /// (`ic_apid_query_blob_buffer`): the field is no longer NULL, and
  /// holds what the buffer holds when the query is defined. It holds
  /// the last value set or read, with its capacity. A value longer than
  /// a write takes is refused with 4266 when the query is defined.
  pub fn blob_buf_mut(
    &mut self,
    position: u32,
  ) -> Result<&mut Vec<u8>, IcError> {
    self.check_idle()?;
    self.values_mut()?.blob_buf_mut(position)
  }

  /// Have a write that finds no row also delete, if there, the part
  /// past its new value's last, and the next while one is, for the
  /// executions defined from now on (chapter 06, phase 12): an insert of
  /// the row racing the write, between its head read and its write, may
  /// otherwise leave parts past the value's length, never read. Off by
  /// default (the author, 2026-11-12): such a write's commit then waits
  /// for the answer, a round later. Refused while the query is defined
  /// or sent (`IC_ERROR_TRANSACTION_ACTIVE`).
  pub fn set_blob_probe(&mut self, probe: bool) -> Result<(), IcError> {
    self.check_idle()?;
    self.blob_probe = probe;
    Ok(())
  }

  /// True if a write that finds no row probes for parts left past its
  /// value ([`set_blob_probe`](Self::set_blob_probe)).
  pub fn blob_probe(&self) -> bool {
    self.blob_probe
  }

  /// The values of the attribute record's blob fields, if it names any.
  pub fn blob_values(&self) -> Option<&BlobValues> {
    self.blobs.as_ref()
  }

  fn values_mut(&mut self) -> Result<&mut BlobValues, IcError> {
    match &mut self.blobs {
      Some(values) => Ok(values),
      None => Err(IcError::new(err::IC_ERROR_NO_SUCH_FIELD)),
    }
  }

  /// The blob values taken out while the driver plans a round, and put
  /// back after.
  pub(crate) fn take_blob_values(&mut self) -> Option<BlobValues> {
    self.blobs.take()
  }

  pub(crate) fn put_blob_values(&mut self, values: BlobValues) {
    self.blobs = Some(values);
  }

  fn check_idle(&self) -> Result<(), IcError> {
    if self.state == QueryState::Defined || self.state == QueryState::Sent {
      return Err(IcError::new(err::IC_ERROR_TRANSACTION_ACTIVE));
    }
    Ok(())
  }

  pub(crate) fn set_state(&mut self, state: QueryState) {
    self.state = state;
  }

  pub(crate) fn begin(
    &mut self,
    mut execution: Execution,
    user_ref: usize,
    callback: Option<QueryCallback>,
    binding: usize,
  ) {
    // The last execution's row buffer is kept for its allocation.
    let mut row = std::mem::take(&mut self.execution.row);
    row.clear();
    execution.row = row;
    self.execution = execution;
    self.user_ref = user_ref;
    self.callback = callback;
    self.binding = binding;
    self.error = None;
    self.error_data = 0;
    self.result_len = 0;
    self.state = QueryState::Defined;
  }

  pub(crate) fn fail(&mut self, error: IcError) {
    self.error = Some(error);
    self.state = QueryState::Completed;
  }

  /// As [`fail`](Self::fail), for a refusal that says more than its code.
  pub(crate) fn refuse(&mut self, error: IcError, error_data: u32) {
    self.fail(error);
    self.error_data = error_data;
  }

  pub(crate) fn set_result_len(&mut self, len: u32) {
    self.result_len = len;
  }

  /// Unpack a whole row straight from the signal it came in into the
  /// attribute row, or for an update the row of what it read after its
  /// program. False if it would not unpack, for the caller to keep it
  /// and let the completion report why.
  pub(crate) fn unpack_direct(&mut self, words: &[u32]) -> bool {
    // An operation of the driver's keeps its row for its query's run.
    if self.internal {
      return false;
    }
    if self.unpack(words).is_err() {
      return false;
    }
    self.result_len = 4 * words.len() as u32;
    self.execution.unpacked = true;
    true
  }

  pub(crate) fn take_row(&mut self) -> Result<(), IcError> {
    if self.execution.unpacked {
      return Ok(());
    }
    let words = std::mem::take(&mut self.execution.row);
    let unpacked = self.unpack(&words);
    if unpacked.is_ok() {
      self.result_len = 4 * words.len() as u32;
    }
    // The buffer is kept for its allocation.
    self.execution.row = words;
    self.execution.row.clear();
    unpacked
  }

  /// A packed row into the row it is for.
  fn unpack(&mut self, words: &[u32]) -> Result<(), IcError> {
    if self.execution.reads_after {
      if let Some(rec) = &self.after_rec {
        crate::row_codec::unpack_row(rec, words, &mut self.after_row)?;
        return Ok(());
      }
    }
    crate::row_codec::unpack_row(&self.attr_rec, words, &mut self.attr_row)?;
    Ok(())
  }
}
