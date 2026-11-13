# Changes

What each release of iClaustron brought. How a release is made:
[`doc/releasing.md`](doc/releasing.md).

## 1.1.0 (2026-11-13)

Blob columns, read and written whole by key. Nothing of 1.0.0 is
removed or changed: a C program built against it runs unchanged, and a
Rust program builds unchanged but in one case below.

### What it needs

As 1.0.0: a RonDB cluster whose data nodes are of version 26.10.0 or
later.

### New since 1.0.0

- BLOB, TEXT and JSON columns, and GEOMETRY, which the dictionary
  describes as blobs, read and written whole by primary key and through
  a unique index: inserted, written, updated and deleted, read
  committed and read locked, on a connection, through an executor, and
  with the synchronous helpers `read_committed_blobs` and
  `write_key_blobs`. Deletes from a table with blob columns now take
  the parts with the row, where 1.0 refused them with 7153 (user guide,
  section 8, "Blob columns").
- A value's first bytes are kept in its row and the rest in a parts
  table, which the client keeps in step: an insert goes in one round
  trip, a write, an update or a delete in two, and a committed read in
  one when the value is all inline and in three when it has parts, or
  in two with `blob_lock_first`. At most `blob_round_bytes`, 1 MiB by
  default, of part data goes in one round for one query.
- Writes are held to the MySQL type's limit, 4266 past it; a value
  whose parts are not all there is 4267 when read.
- The C API: 21 functions, all additions. On a query and on a request's
  operation, a blob value is read, set, set NULL or filled in place,
  and a read made to take its lock first or a write to probe for parts
  left past its value; `IC_BLOB_VALUES` holds the values for
  `ic_apid_conn_read_committed_blobs` and `ic_apid_conn_write_blobs`.
  C names a blob field by its field id. `IC_GLOBAL_OPTIONS` gains
  `blob_round_bytes` and `IC_EXECUTOR_STATS` `blob_rounds`, at their
  ends; 4266 and 4267 are `IC_NDB_ERROR_BLOB_TOO_LONG` and
  `IC_NDB_ERROR_BLOB_DAMAGED`.
- A data node's word that a transaction or a scan it holds is still
  running restarts that one's wait for its replies, so that a scan
  running long without a batch no longer ends with 4012 after six
  minutes while its coordinator says it runs.
- A twelfth example, `blobs`, in Rust and in C, and the user guide's
  section 8.
- What blob columns cost, measured (`doc/rust/12-performance-model.md`,
  "Blob columns"); `ic_bench`, `ic_trans` and `ic_read` take tables
  with blob columns.

### Changed: Rust programs built against 1.0.0

- `GlobalOptions` gains `blob_round_bytes` and `ExecutorStats`
  `blob_rounds`. A program that makes either with every field named,
  without `..Default::default()`, names the new one too.

### Not done yet

Blob columns in scans and in rows a scan hands over, parts of a value
read or written alone, TLS, location domains, events and schema
changes, among others: user guide, "What is not done yet".

## 1.0.0 (2026-11-05)

The first version. It talks to RonDB 26.10 data nodes and is the same
on the wire as the reference API, and as 0.1. Releases 0.2 and 0.3 were
planned but never tagged; what they were to hold is here.

### What it needs

A RonDB cluster whose data nodes are of version 26.10.0 or later. Do
not point it at an older cluster: a data node of 26.02 stops on a
request this release sends when it first binds a table with a
character column, and gets some conditions wrong
(`doc/rust/06-phases.md`, "Later").

### New since 0.1

- Executors: a thread without a connection of its own runs its
  transactions through an executor, which sends many threads' calls
  together; blocking calls work through them too.
- Scans of tables and of ordered indexes, over ranges ascending or
  descending, on a connection or through an executor, taken row by row,
  a batch at a time, or through a scan reader. Rows a
  locking scan read can be updated or deleted through it, on a
  connection.
- Character sets as the data nodes describe them: text in and out of
  rows, and keys of every collation placed by their hash, but three
  that weigh a character by its neighbours (user guide, "What is not
  done yet").
- Programs: conditions and assignments that run on the data nodes, on
  key operations and on scans.
- AUTO_INCREMENT values from the counter MySQL uses, and tables
  without a primary key.
- Committed-read scans with four receivers to a fragment, fanout tables
  placed and their scans pruned, and a scan closed at once.
- Eleven example programs, the same in Rust and in C (`examples/`), and
  a user guide covering all of the above (`doc/user-guide.md`).

### Hardened

- Two reviews by the author, and thirteen fixes from them.
- Ids are 64 bits, a slot and a generation, so a stale id is never
  followed.
- A transaction waits six minutes at most for its replies, and a scan
  whose coordinator never answers ends with 4012.
- A connection's free waits for the records it asked for, and a
  coordinator record lost with its link frees its place once nothing
  holds it.
- What arrives from the network is decoded by code that is fuzzed
  (`cargo xtask fuzz`); five inputs that could panic or overflow are
  refused.
- Errors classed as the data nodes class them: a refused fanout scan
  (2204) and a schema object that exists are permanent, 270 is
  temporary.

### Changed: C programs built against 0.1

- `IC_QUERY_ID` and `IC_TRANS_ID` are `uint64_t`, where they were
  `uint32_t`. Build such programs again; their source needs no change
  unless it stored ids in 32 bits.
- The shared library is `libiclaustron.so.1` on Linux and
  `libiclaustron.1.dylib` on macOS, the name a program records. On
  Linux 0.1's `libiclaustron.so.0` stays for the programs built against
  it; on macOS 0.1's programs recorded `libiclaustron.dylib`, which
  `cargo xtask install` now links to 1.0's library, so they have to be
  built again.
- No function of 0.1 was removed or changed; 114 were added.
- The original iClaustron C code, under `legacy-c/` in 0.1, is no longer
  in the tree; it is on branch `main`.

### Not done yet

Blob columns, TLS, location domains, events and schema changes, among
others: user guide, "What is not done yet".

## 0.1.0 (2026-10-15)

The first release of the Rust NDB API. It is built for the RonDB 26.10
data nodes and is the same on the wire as the reference API.

- Connects through the management server, takes a node id, heartbeats,
  and survives a data node restart and a management server restart.
- Describes MySQL-created tables as the reference's tool does.
- Primary and unique key reads and writes, with commit and rollback,
  pipelined in batches. Rows of every size up to the 2026-10 limits,
  sent as fragment trains when needed.
- Temporary errors are classified, and a transaction whose coordinator
  failed can be retried elsewhere.
- Up to 4 711 user threads.
- A C library, libiclaustron, with a documented header, a user guide,
  and examples in Rust and C.
- Committed reads at 8 threads run at about twice the reference
  benchmark's rate, with no allocations per operation.

Criteria and evidence: doc/rust/06-phases.md, phase 7.
