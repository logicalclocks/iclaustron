# Changes

What each release of iClaustron brought. How a release is made:
[`doc/releasing.md`](doc/releasing.md).

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
