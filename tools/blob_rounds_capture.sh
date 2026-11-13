#!/bin/sh
# Phases 12.3 and 12.5 (doc/rust/06-phases.md): our client's rounds for
# blob values, for a capture read with the dissector against chapter 05
# §6.10. Start the capture, then this, then stop the capture. Every
# step is a run of one of our tools, whose links to the data nodes are
# new, so that the dissector sees their handshakes:
#
#   sudo tcpdump -i lo0 -w /tmp/blob-rounds.pcap \
#     'tcp port 1186 or tcp port 11861 or tcp port 11862'
#   tools/blob_rounds_capture.sh        # from the repository's root
#
# Each step runs alone, two seconds after the last, and the time printed
# before it says which frames are its; the tools print the rounds they
# counted after each query's first. The table is ictest.tcap, a key and
# a TEXT, made here and dropped at the end. The steps:
#   1. A hundred rows inserted in one transaction, the TEXT all inline
#      (200 bytes): one round (12.3); read back in one batch, each read
#      taking its lock first since there are many, and kept.
#   2. A hundred rows more, the TEXT with two parts (4 000 bytes): one
#      round too, the parts with their rows; read back the same way.
#   3. Row 1, inline, read alone: committed, one round; with the lock
#      first, two, the commit alone the second; locked, two (12.5).
#   4. Row 101, with parts, read alone: committed, three rounds, the
#      row committed, then locked with its lock reference, then the
#      parts with the unlock; with the lock first, two; locked.
set -u

# RONDB names the RonDB tree the cluster runs from (doc/dev-cluster.md);
# RONDB_BIN, its programs' directory, defaults to its release build's.
RONDB=${RONDB:?set RONDB to the RonDB tree of the cluster}
RONDB_BIN=${RONDB_BIN:-$RONDB/prod_build/bin}
CS=${CS:-localhost:1186}
# The first MySQL server's port is read from MTR's var/my.cnf of the
# release build, as tools/blob_bench.sh does; MYSQL_PORT, or MYSQL as a
# whole, may be given instead. MTR leaves var/my.cnf behind when its
# cluster stops, so the port is checked before anything else.
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
  echo "No MySQL server answered on port $MYSQL_PORT; give MYSQL_PORT or MYSQL."
  exit 1
fi

cargo build --release -p ic_trans -p ic_read || exit 1
TRANS="target/release/ic_trans -c $CS -d ictest tcap"
READ="target/release/ic_read -c $CS -d ictest tcap"

$MYSQL -e "CREATE DATABASE IF NOT EXISTS ictest;
  DROP TABLE IF EXISTS ictest.tcap;
  CREATE TABLE ictest.tcap (id INT NOT NULL PRIMARY KEY, t TEXT)
    ENGINE=NDB" || exit 1

step() {
  sleep 2
  echo "== $(date +%H:%M:%S.%N | cut -c1-12) $*"
  "$@"
}

# 1 and 2. A hundred rows in one transaction, inline, then with parts.
step $TRANS --from 1 --count 100 --bytes 200 --keep
step $TRANS --from 101 --count 100 --bytes 4000 --keep
# 3. Row 1, inline, read alone in each way.
step $READ 1
step $READ 1 --lock-first
step $READ 1 --locked
# 4. Row 101, with parts.
step $READ 101
step $READ 101 --lock-first
step $READ 101 --locked

sleep 2
$MYSQL -e "DROP TABLE ictest.tcap"
echo "== done; stop the capture"
