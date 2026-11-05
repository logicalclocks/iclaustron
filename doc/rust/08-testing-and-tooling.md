# 08 — Testing, tooling and verification

The developer runs all builds and tests from the command line; the
framework is designed for that and nothing assumes a CI service. Each
milestone in [06-phases.md](06-phases.md) names the command that proves
it. A CI job can run the same commands later if wanted.

## Toolchain

- Rust stable via `rustup` (not installed on this machine yet:
  `curl https://sh.rustup.rs -sSf | sh`, then `rustup component add
  clippy rustfmt`). Pin with `rust-toolchain.toml` to the current stable at
  project start and bump quarterly.
- `cargo install cbindgen cargo-deny` (header generation, license and
  advisory checks). Plain `cargo test` is the test runner.
- macOS (this machine) and Linux are both first-class; aarch64 Linux
  because RonDB ships on ARM.
- A C compiler and CMake only for the C/C++ examples and the compatibility
  benchmark against the reference API.

## Test pyramid

### 1. Unit tests (per crate, `cargo test -p <crate>`)

- `ic_util`: direct ports of `test/test_unit.c` types 1–8 (memory
  container, simple/ordered dynamic array, ptr array, bitmap, hashtable,
  connectstring, socket buffer). These are the first tests to exist.
- `ic_comm`: loopback socket tests for `Connection` read/write/writev/
  sessions and for each `PollSet` backend; `test/test_comm.c` scenarios.
- `ic_signals`: encode/decode round-trips for every signal struct;
  golden-vector tests from captured traffic (below); fuzz tests of
  every decoder (below, "Fuzzing"), as these take bytes from the
  network.
- `ic_apic`: parameter-table invariants (unique ids, unique names), config
  blob v2 decoding from captured blobs, typed config extraction.
- `ic_apid`: state machines tested without a network: transaction
  bookkeeping (pending counts, KEY_OP_CONF/ROW_DATA_REP ordering, node-down
  notices), adaptive send statistics, the WHERE-condition compiler
  (source tree → expected interpreter words), record packing/unpacking
  for every column type.

### 2. Captured-traffic tests

Traffic is captured with `tcpdump` on the development cluster's
loopback device and read with our Wireshark dissector (chapter 06 §6.7).
A small tool `tools/ic_capture` reads such capture files, follows each
connection past its handshake, stores the signal streams as `.bin` +
`.json` (decoded by our own codec), and the unit tests replay them. This gives regression
vectors for exactly the signals RonDB emits, independent of RonDB
source code. Captures come from running the reference API's own example
programs (simple key operations, the same through a unique index, the
asynchronous interface, later a scan) against a local 26.10 cluster.

### 3. Integration tests (`cargo xtask test-integration` = `IC_TEST_CONNECTSTRING=... cargo test -p ic_apid --features integration`)

Need a live cluster. Two ways to get one:

- **Local build**: a RonDB tree built with `-DWITH_NDB=1`; start with the MTR-provided cluster
  (`cd mysql-test && ./mtr --start ndb.ndb_basic` leaves a cluster running
  on well-known ports) or with a hand-written `config.ini`, the
  management server, the data nodes and `mysqld`. This is the primary
  development setup.
- **Docker**: RonDB's published images (`rondb-docker`), tag pinned to
  26.10.

Tests are ordinary `#[test]` functions gated by an env var
`IC_TEST_CONNECTSTRING`; when unset they are skipped, not failed. Tables
are created via MySQL (`mysql` client over TCP) so the tests exercise
exactly what `mysqld`-created tables look like, which is what users have.

As built (2026-09-22, `crates/ic_apid/tests/integration.rs`): three
variables, and a test that lacks what it needs says so on stderr and
returns. `IC_TEST_CONNECTSTRING` names the management server;
`IC_TEST_MYSQL` is a `mysql` client command with its connection
options, run through `sh -c`, through which the tests make their
tables in the database `ic_it` and check what they wrote;
`IC_TEST_NDB_MGM` is a management client command for the `failure` group,
which stops a data node and starts it again. Each test is its own
program against the cluster, taking an API node id and letting it go,
with a retry on the id since the one the last test released may not be
free at once. The groups built are the phase-5 ones: `connect`,
`dict` (columns and key, index columns in key order, the cache letting
go after `ALTER TABLE`), `pk` (all five operations checked through
MySQL, 626 and 630, commit and rollback visibility), `uk` (read, update
and delete through `uk_code$unique`, the name a MySQL server gives a
unique hash index), `types` (the integer family at its edges, char,
varchar, binary, varbinary, and the long forms, written through the API
and read back both ways; float and double read from what MySQL wrote;
a row of NULLs; the date, time, decimal and bit types wait for the text
codec to format them) and `failure` (an update confirmed at a
coordinator that is then restarted: the commit fails with a temporary
error, the retry at the survivor commits, and the node is waited for
until it is started again, so that the next test finds the cluster
whole). Seen live 2026-09-22: the pending transaction ended rolled
back with 4010, the branch built for a coordinator that fails before
the commit is asked for. The management server restart is not yet covered.

Test groups, mirroring the phases:

| Group | Proves |
|---|---|
| `connect` | node id allocation, handshake with every data node, HEARTBEAT_CONF received, `wait_first_node_connect` returns; survives a data node restart ordered from the management client |
| `dict` | every column type in one table decodes to the right `FieldType`/length; indexes and their key order; table version bump after `ALTER TABLE` invalidates the cached def |
| `pk` | read/insert/update/delete/write; error 626 (no such row) and 630 (duplicate) mapping; commit and rollback visibility checked through MySQL |
| `uk` | unique index read/update/delete |
| `scan` | (0.2) full table scan count equals `SELECT COUNT(*)`; range scans with each bound type; multi-range; takeover update/delete |
| `where` | pushed conditions match MySQL results on a randomly generated table |
| `assign` | conditional assignments `a = a * (b + 1)` match MySQL arithmetic |
| `types` | round-trip of every type incl. Decimal, Datetime2/Timestamp2 fractional seconds, Bit, Varbinary, Char with charsets |
| `blob`, `event`, `ddl` | 0.3 |
| `failure` | kill a data node mid-transaction: correct temporary error class, transaction retried by the test on another node; restart the management server while connected |

### 4. Conformance against the reference API

`tools/ic_bench` and a matching C++ program (built in the RonDB tree
against the reference API) run the same workload (PK reads of 8
columns, 1 000 rows, N threads, async batches of 100). We record throughput and p99 latency
for both and keep the numbers in `doc/bench/`. The 0.1 criterion is
within 20 % of the reference API.

### 5. Multi-language smoke tests

Under `tests/examples/`: `c/simple.c`, `cpp/simple.cpp`, plus the
languages chosen in Q6 (Python `ctypes`, Java Panama suggested). Each does
connect → bind table → PK insert/read → commit and is run by
`cargo xtask test-examples` against the development cluster. Their purpose is to catch ABI mistakes
(struct layout, ownership, string lifetime), not to be real bindings.

## Fuzzing (`cargo xtask fuzz`)

The decoders of what arrives from the network are fuzzed, those
listed below, each by a test the developer runs when it changes. The
fuzzer is our own, `ic_port::fuzz`: it needs nothing beyond std and
runs on the stable toolchain, like everything else here.

**The runner.** A fuzz test is a plain `#[test]` named `fuzz_<target>`
and marked `#[ignore]`, so that `cargo test` leaves it out. It hands
`ic_port::fuzz::run` the target's name, a few seeds made by our own
encoders, and the target: a plain `fn(&[u8])` that gives one input to
the decoders. A target of a decoder of words makes the bytes into
words with `fuzz::words_of`, and its seeds are made from words with
`fuzz::bytes_of`. The first cases are the seeds as they are; each of
the rest is a seed changed by one to four mutations, those that find
what a decoder trusts: bits flipped, bytes changed, a word set to a
value at an edge (0, 1, all ones, the largest signed) or just past
one, the input cut short or made longer, a piece repeated, the tail
of another seed spliced on. Now and then a case is bytes of no seed
at all.

**The pass rule.** A target throws away what a decoder returns: an
error is the right answer to bytes that make no sense. A case fails
when it panics, or when it takes longer than 1 s
(`IC_FUZZ_CASE_LIMIT_MS`); a case still running after 10 s
(`IC_FUZZ_HANG_MS`) is caught by a watching thread, which stops the
process. The `fuzz` build profile of the workspace's `Cargo.toml` is
the release profile with overflow checks and debug assertions on, so
that an integer overflow panics instead of wrapping unseen.

**The targets**, by crate:

- `ic_signals` (`src/fuzz_tests.rs`, using only the crate's public
  interface):
  - `fuzz_header_decode`: `header::peek_message_len` and
    `header::decode` on each message of the input in turn, then each
    section. Seeds from `header::encode`, with and without signal id
    and checksum, with 0 to 3 sections and up to 25 words of data,
    several messages back to back, and trains of fragments from
    `header::encode_train`.
  - `fuzz_bundle_unpack`: `bundle::unpack` and `unpack_into`, then the
    words of each part.
  - `fuzz_property_stream`: a `PropertyReader` read to the end or to
    the first error. Seeds from `PropertyWriter`: numbers of one and
    two words, text and bytes of every length the padding treats
    apart.
  - `fuzz_table_info`: `parse_table_info` and `parse_hash_map_info`,
    then what a reader asks of the result, such as each column's
    `max_byte_size`. Seeds: tables of every sized type, with a key,
    replicas and a default, and hash maps.
  - `fuzz_signal_data`: every signal decoder on the same words, which
    the first word splits into data, section 0 and section 1: the
    coordinator's records, key operations and their replies, scans
    and their batches in each form, the dictionary's answers,
    collations and their weights, heartbeats and node failure. Seeds
    of the lengths these signals have.
- `ic_apic` (`conf_blob.rs`): `fuzz_config_blob`, `ConfigBlob::decode`
  and, from what decodes, `ClusterConfig::from_blob` for the API node
  of the test blobs. The blob ends with a checksum of its words; in
  about half the cases the target puts it right first, so that a
  mutation reaches the parser.
- `ic_protocol` (`base64.rs`): `fuzz_base64_decode`, `base64::decode`
  of what `encode` and `encode_no_breaks` make, with blanks and the
  trailing NUL.
- `ic_apid`:
  - `fuzz_signal_stream` (`signal_reader.rs`): the receive path as the
    receive thread runs it. The stream goes into a `SignalReader` in
    pieces of sizes the input chooses; each whole signal is decoded, a
    bundled one taken apart, and consumed, and a signal that does not
    decode ends the case, as it drops the link. Pages are held as a
    large signal handed on where it lies holds them, so that the
    reader goes on in other pages.
  - `fuzz_unpack_row` (`row_codec.rs`): `unpack_row` of a read's
    packed row into a row of the module's test table.

**Running.**

```
cargo xtask fuzz                     every target, 100 000 cases each
cargo xtask fuzz 1000                every target, 1 000 cases each
cargo xtask fuzz header_decode       one target
cargo xtask fuzz signal_data 5000000 one target, a long run
```

It runs `cargo test --workspace --lib --profile fuzz -- --ignored
--test-threads=1 --nocapture fuzz_<target>`, the target named with or
without its `fuzz_`, or every `fuzz_` test. Three variables steer a
run:

- `IC_FUZZ_ITERS`: cases for each target, 100 000 if not set; a number
  given to `cargo xtask fuzz` sets it.
- `IC_FUZZ_SEED`: the run's seed, from the clock if not set, and
  printed at the start either way.
- `IC_FUZZ_CASE`: run only this case of the run.

A case is the same on every machine for the same seed and number, so
a failure names both, and the case runs again alone, under a debugger
or with a backtrace:

```
IC_FUZZ_SEED=1234 IC_FUZZ_CASE=5678 RUST_BACKTRACE=1 \
  cargo xtask fuzz header_decode
```

A case that kills the process rather than panicking, as an allocation
larger than memory does, is not named. The cases run in order, so
halving `IC_FUZZ_ITERS` again and again with the same seed finds it.

**Why not cargo-fuzz.** It needs the nightly toolchain, where
`rust-toolchain.toml` pins stable, and the crate `libfuzzer-sys`,
where principle 8 of [chapter 01](01-goals-and-principles.md) allows
std and `libc` alone. What libFuzzer adds is guidance by coverage.
Our decoders read inputs of a few hundred words with their fields at
fixed places, from seeds that are valid already, and blind mutation
reaches them. A target needs nothing but its input, so a cargo-fuzz
crate kept outside the workspace could call the same functions later,
once they are made public for it; today they are private to their
test modules.

## Static checks (`cargo xtask check`, run by the developer before a commit)

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check licenses advisories
cargo test --workspace
cargo doc --no-deps -D rustdoc::broken_intra_doc_links
cargo xtask header --check       # cbindgen output == checked-in header
```

Plus a custom check `cargo xtask style`, which does two things clippy
and rustfmt cannot:

- **Line width.** Every line of every `.rs` file must fit in 80 columns.
  rustfmt keeps code within the limit but never reflows a comment or a
  string literal, so those are only caught here.
- **Forbidden constructs.** The items from
  [07-style-guide.md](07-style-guide.md) that clippy cannot express
  (`impl Trait`, `where `, `async `, `Rc<`, `RefCell<`, `.iter().map(`,
  `macro_rules!`, `.unwrap()`), skipped inside `#[cfg(test)]` modules,
  with `xtask/style-allow.txt` listing the few sanctioned places and the
  reason for each. Only code is searched: comments and what string and
  character literals hold are left out, a string followed over the
  lines it spans. A pattern that starts with a name, such as `Cell<`,
  is not matched as the end of a longer one, such as `RefCell<`; one
  that starts with a dot, such as `.unwrap()`, is matched wherever it
  is (until 2026-10-17 it was missed after a name, as in
  `x.unwrap()`).

## Editor navigation: `cargo xtask tags`

The successor of the C code's `git_tags.sh`. It lists every source
file git knows about (tracked, or new and not ignored), Rust and C, and
feeds them to Universal Ctags with the Rust, C and C++ parsers,
producing a sorted `./tags` file for vim/gvim (`tags` is git-ignored).
Needs Universal
Ctags (`brew install universal-ctags` or `sudo port install
universal-ctags`); the BSD `ctags` that ships with macOS has no Rust
parser and the command says so. Suggested `~/.vimrc` lines:

```
set tags=./tags;,tags;
" :tag name, Ctrl-] jump, Ctrl-T back, :tselect for several matches
```

Re-run after adding files; a `BufWritePost` autocommand or a git
`post-commit` hook calling `cargo xtask tags` keeps it current. For tags
into the Rust standard library and dependencies, `rusty-tags`
(`cargo install rusty-tags`, `rusty-tags vi`) can be layered on later.

## Debugging aids to build early

- `IC_DEBUG=level_bits` env var driving the ported `DEBUG_*` machinery,
  same 17 level bits as the C.
- `IC_NDB_MESSAGE_LEVEL` prints every signal in and out as
  `sig_no name, sender/receiver block refs, words, sections` using the
  `ic_signals` decoders. This is the single most useful tool during
  phases 3–6.
- `tools/ic_config_dump`: fetch and print a cluster config from the
  management server (phase 2 deliverable), also used to diff parameter tables
  between RonDB versions.
- `tools/ic_desc db.table`: print a table definition as the API sees it
  (phase 4 deliverable).
- `tools/ic_sigdump file.bin`: decode a captured signal stream.

## Documentation

- `cargo doc` for the Rust API; the doc comments are ported from the
  `ic_apid.h` prose (they are the specification).
- The C header carries the same prose (cbindgen copies doc comments).
- `doc/` in the new repo: this plan, a user guide with the C, C++ and Rust
  examples, and `MODULE.md` per crate.
