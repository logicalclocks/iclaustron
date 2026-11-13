#!/bin/sh
# Phase 12.9 (doc/rust/06-phases.md): what a blob column costs, measured
# with ic_bench against one cluster into one log, for chapter 12.
# Measure a cluster built in release (RONDB_BIN below), as
# tools/perf_pass.sh does, not the debug development cluster, and give
# it a redo log that takes ten seconds of 100 000-byte updates (MTR's
# default of four files of 64 MB fills in seconds, and the updates
# fail with 410).
#
#   tools/blob_bench.sh                # from the repository's root
#   CS=host:1186 SECONDS_RUN=20 THREADS=16 tools/blob_bench.sh
#
# Each operation is a transaction of its own, by primary key, in
# ic_bench's batches of 200 with two in flight, over a table of its own
# for each value:
#   tvar     200 bytes in a VARCHAR, the baseline;
#   tinline  200 bytes in a TEXT, all inline: read in one round, as the
#            baseline is, or in two with the lock first; an update's
#            commit goes alone once the old head is back;
#   ttext    4 000 bytes in a TEXT, 256 inline and two parts of 2 000:
#            read committed first (three rounds) and with the lock first
#            (two); updated in two;
#   tmedium  100 000 bytes in a MEDIUMTEXT, the smallest type that
#            holds them (a TEXT holds 65 535), 256 inline and 25 parts
#            of 4 000: the same, in batches of BATCH_LARGE, about 1 MB a
#            batch at the default; read on THREADS also with two
#            receive threads, the bytes a single one reads being the
#            likely limit.
# Every run is made on one user thread, then on THREADS, each with a
# connection and a share of the keys of its own. tvar's and ttext's are
# also made through executors, which serve many application threads on
# few threads of their own: for each EXECUTORS entry, executors:callers,
# each caller making one blocking call at a time. A caller's rate is
# then one over a call's time until the executors are full, so 64
# callers measure the call and 1 024 the executor. Every table's rows
# are written and read first, then ic_bench built with count-allocs
# reads ttext; the updates come last, PAUSE seconds apart, so that the
# redo log one run fills is freed before the next. A table whose rows
# were not written is not read. The tables are dropped at the end.
#
# KEYS gives each of the default 8 threads 1 000 keys, more than its 400
# in flight, so that no thread has two updates of one row out at once,
# and is more than the most callers; KEYS_LARGE gives each thread 50
# against its 20. Mind the data memory: ttext
# keeps 34 MB on each node holding it, tmedium 40 MB at the defaults.
set -u

# RONDB names the RonDB tree the cluster runs from (doc/dev-cluster.md);
# RONDB_BIN, its programs' directory, defaults to its release build's.
RONDB=${RONDB:?set RONDB to the RonDB tree of the cluster}
RONDB_BIN=${RONDB_BIN:-$RONDB/prod_build/bin}
CS=${CS:-localhost:1186}
# The first MySQL server's port is read from MTR's var/my.cnf, as
# tools/blob_capture.sh does; MYSQL_PORT, or MYSQL as a whole, may be
# given instead. MTR leaves var/my.cnf behind when its cluster stops,
# so the port is checked before anything is built.
if [ -z "${MYSQL_PORT:-}" ]; then
  MYSQL_PORT=13000
  cnf=$RONDB/prod_build/mysql-test/var/my.cnf
  if [ -f "$cnf" ]; then
    port=$(sed -n '/^\[mysqld\.1\.1\]/,/^\[/s/^port=//p' "$cnf" | head -1)
    MYSQL_PORT=${port:-13000}
  fi
fi
MYSQL=${MYSQL:-"$RONDB_BIN/mysql -uroot --protocol=tcp --host=127.0.0.1 --port $MYSQL_PORT --connect-timeout=5"}
if ! $MYSQL -e "SELECT 1" >/dev/null; then
  echo "No MySQL server answered on port $MYSQL_PORT. Start the release"
  echo "cluster (prod_build/mysql-test: ./mtr --suite=ndb"
  echo "ndb_setup_fixed_ports --start), or give MYSQL_PORT or MYSQL."
  exit 1
fi
SECONDS_RUN=${SECONDS_RUN:-10}
THREADS=${THREADS:-8}
KEYS=${KEYS:-8000}
KEYS_LARGE=${KEYS_LARGE:-400}
BATCH_LARGE=${BATCH_LARGE:-10}
PAUSE=${PAUSE:-30}
EXECUTORS=${EXECUTORS:-1:64 1:256 1:1024 2:1024}
OUT=${OUT:-target/blob-bench-$(date +%Y%m%d-%H%M%S).log}

mkdir -p target
# A run into the log, its status ic_bench's: its output is shown once
# it is done.
run() {
  echo "== $*" | tee -a "$OUT"
  "$@" > "$OUT.run" 2>&1
  status=$?
  tee -a "$OUT" < "$OUT.run"
  rm -f "$OUT.run"
  return $status
}
skipped() {
  echo "== skipped: $*" | tee -a "$OUT"
}
# The runs through executors, one for each EXECUTORS entry; with WAIT
# set to 1, PAUSE seconds before each.
through_executors() {
  for spec in $EXECUTORS; do
    if [ "$WAIT" = 1 ]; then
      sleep "$PAUSE"
    fi
    run $B "$@" --no-prepare --executors "${spec%%:*}" --clients "${spec#*:}"
  done
}

cargo build --release -p ic_bench || exit 1
cargo build --release -p ic_bench --features count-allocs \
  --target-dir target/count-allocs || exit 1

$MYSQL -e "CREATE DATABASE IF NOT EXISTS ictest;
  DROP TABLE IF EXISTS ictest.tvar, ictest.tinline, ictest.ttext,
    ictest.tmedium;
  CREATE TABLE ictest.tvar (id INT NOT NULL PRIMARY KEY, v INT,
    t VARCHAR(200)) ENGINE=NDB;
  CREATE TABLE ictest.tinline (id INT NOT NULL PRIMARY KEY, v INT,
    t TEXT) ENGINE=NDB;
  CREATE TABLE ictest.ttext (id INT NOT NULL PRIMARY KEY, v INT,
    t TEXT) ENGINE=NDB;
  CREATE TABLE ictest.tmedium (id INT NOT NULL PRIMARY KEY, v INT,
    t MEDIUMTEXT) ENGINE=NDB" || exit 1

echo "== $(git log -1 --format='%h %s'), $(uname -sm), $(date)" | tee -a "$OUT"

B="target/release/ic_bench -c $CS -d ictest --seconds $SECONDS_RUN"
A="target/count-allocs/release/ic_bench -c $CS -d ictest --seconds $SECONDS_RUN --no-prepare"
T="--threads $THREADS"
VAR="tvar --keys $KEYS --bytes 200"
INLINE="tinline --keys $KEYS --bytes 200"
TEXT="ttext --keys $KEYS --bytes 4000"
LARGE="tmedium --keys $KEYS_LARGE --bytes 100000 --batch $BATCH_LARGE"

# 1. Written, then read: on one thread, then on THREADS.
var_ok=0
text_ok=0
if run $B $VAR; then
  var_ok=1
  run $B $VAR --no-prepare $T
else
  skipped "tvar's reads"
fi
if run $B $INLINE; then
  run $B $INLINE --no-prepare --lock-first
  run $B $INLINE --no-prepare $T
  run $B $INLINE --no-prepare $T --lock-first
else
  skipped "tinline's reads"
fi
if run $B $TEXT; then
  text_ok=1
  run $B $TEXT --no-prepare --lock-first
  run $B $TEXT --no-prepare $T
  run $B $TEXT --no-prepare $T --lock-first
  run $A $TEXT
else
  skipped "ttext's reads"
fi
if run $B $LARGE; then
  run $B $LARGE --no-prepare --lock-first
  run $B $LARGE --no-prepare $T
  run $B $LARGE --no-prepare $T --lock-first
  run $B $LARGE --no-prepare $T --receive-threads 2
else
  skipped "tmedium's reads"
fi
# Through executors: the baseline, then the 4 000 bytes.
WAIT=0
if [ "$var_ok" = 1 ]; then
  through_executors $VAR
fi
if [ "$text_ok" = 1 ]; then
  through_executors $TEXT
fi

# 2. Updated, PAUSE seconds apart: on one thread, then on THREADS, then
# through executors.
for table in "$VAR" "$INLINE" "$TEXT" "$LARGE"; do
  sleep "$PAUSE"
  run $B $table --no-prepare --mode write
  sleep "$PAUSE"
  run $B $table --no-prepare --mode write $T
done
WAIT=1
through_executors $VAR --mode write
through_executors $TEXT --mode write
sleep "$PAUSE"
run $A $TEXT --mode write

$MYSQL -e "DROP TABLE ictest.tvar, ictest.tinline, ictest.ttext,
  ictest.tmedium"
echo "The log is $OUT"
