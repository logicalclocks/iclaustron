#!/bin/sh
# Phase 12.1 (doc/rust/06-phases.md): blob columns read and written by
# a MySQL server, one statement after another, for a capture read with
# the dissector against chapter 05 §6.10. Start the capture before the
# cluster, then this, then stop the capture. The dissector knows a
# signal link by its handshake, and the MySQL server's links come up
# when the cluster starts:
#
#   sudo tcpdump -i lo0 -w /tmp/blobs.pcap \
#     'tcp port 1186 or tcp port 11861 or tcp port 11862'
#   (start the release cluster, doc/dev-cluster.md A)
#   tools/blob_capture.sh            # from the repository's root
#
# Each statement runs alone, two seconds after the last, and the time
# printed before it says which frames are its. A statement that fails
# is reported and the next one runs. The tables' ids, and their parts
# tables' names, are printed after they are made: a part is a row of
# NDB$BLOB_<table id>_<column id>, read and written by its own key.
#
# The values say where they lie: a TEXT's inline bytes are one letter
# and each part of 2 000 bytes the next, so that a part's request shows
# which part it is (i, then a, b, c for 5 000 bytes; I, then A to E
# for 9 000).
#
# What each step is for (chapter 05 §6.10 lists what is to be seen):
#   - inserts: a value inline only, one with parts of 2 000 bytes, a
#     JSON value (4 000 inline bytes, parts of 8 100), a LONGBLOB of
#     100 000 bytes (parts of 13 948), a TINYBLOB (no parts table), and
#     ten rows in one statement: the head's layout, the parts' key and
#     data, and how many go in one batch;
#   - inserts that ignore errors, of a key there and of one not: are
#     the parts held back until the main insert is known to succeed?
#   - committed reads, of an inline value and of values with parts:
#     the shared lock and the lock reference that replace the committed
#     read, the parts' simple reads, and the unlock; in an open
#     transaction too, where the unlock cannot ride with a commit;
#   - reads with a shared and an exclusive lock;
#   - updates that grow a value, shrink it to inline, set it NULL: the
#     head read before the update and its lock, and the parts written
#     and deleted after;
#   - REPLACE, a write, of a row there and of one not;
#   - deletes: the head read, and the parts deleted;
#   - through a unique index: a read, an update, a delete;
#   - with DISK=1, a TEXT stored on disk, whose parts are fixed-size and
#     padded (it makes the log file group ic_it_lg and the tablespace
#     ic_it_ts if the cluster has none, as the integration tests do).
#
# Before anything it checks that the management server at CS has every
# data node started, that the MySQL server is one of that cluster's,
# and that it makes NDB tables: a management server left over from an
# earlier run on the same port answers, but has no data nodes, and
# another MTR run may hold port 13000. A step that fails prints its error,
# the rows are listed at the end, and the script ends with an error if
# any step failed.
set -u

# RONDB names the RonDB tree the cluster runs from (doc/dev-cluster.md);
# RONDB_BIN, its programs' directory, may be given instead.
# The debug build's programs are taken when there are any, as the
# cluster runs from that build.
if [ -z "${RONDB_BIN:-}" ]; then
  RONDB_BIN=${RONDB:?set RONDB to the RonDB tree of the cluster}/debug_build/bin
  if [ ! -x "$RONDB_BIN/mysql" ]; then
    RONDB_BIN=$RONDB/prod_build/bin
  fi
fi
CS=${CS:-localhost:1186}
# MTR fixes only the cluster's ports: its MySQL servers take 13000 when
# no other MTR run on the machine holds that range, and the next free
# range when one does. The first MySQL server's port is read from MTR's
# var/my.cnf; MYSQL_PORT, or MYSQL as a whole, may be given instead.
if [ -z "${MYSQL_PORT:-}" ]; then
  MYSQL_PORT=13000
  cnf=${RONDB:-}/debug_build/mysql-test/var/my.cnf
  if [ -n "${RONDB:-}" ] && [ -f "$cnf" ]; then
    port=$(sed -n '/^\[mysqld\.1\.1\]/,/^\[/s/^port=//p' "$cnf" | head -1)
    MYSQL_PORT=${port:-13000}
  fi
fi
MYSQL=${MYSQL:-"$RONDB_BIN/mysql -uroot --protocol=tcp --host=127.0.0.1 --port $MYSQL_PORT --connect-timeout=5"}
DISK=${DISK:-0}

FAILED=0

die() {
  echo "blob_capture: $*" >&2
  exit 1
}

step() {
  what=$1
  shift
  echo "== $(date +%H:%M:%S) $what"
  if ! out=$("$@" 2>&1); then
    FAILED=$((FAILED + 1))
    echo "   FAILED:"
    echo "$out" | head -3 | sed 's/^/   /'
  fi
  sleep 2
}

sql() {
  $MYSQL -e "$1"
}

# Every data node of the cluster at CS started, as ndb_mgm shows them.
show=$("$RONDB_BIN/ndb_mgm" -c "$CS" -e show 2>&1) ||
  die "ndb_mgm -c $CS failed: $show"
nodes=$(echo "$show" | sed -n '/^\[ndbd(NDB)\]/,/^$/p' | grep '^id=')
total=$(echo "$nodes" | grep -c '^id=')
started=$(echo "$nodes" | grep '@' | grep -v -c 'starting')
if [ "$total" -lt 1 ] || [ "$started" -ne "$total" ]; then
  echo "$show" >&2
  die "not every data node at $CS is started; a management server left from an earlier run on the same port?"
fi
echo "== $(date +%H:%M:%S) $started data nodes started at $CS"
# The MySQL server answers, and took its configuration from the
# management server at CS, not from another cluster's on this machine.
from=$(sql "SHOW GLOBAL STATUS LIKE 'Ndb_config_from_port'" 2>&1) ||
  die "the MySQL server does not answer: $MYSQL: $from"
from=$(echo "$from" | awk 'NR == 2 { print $2 }')
want=${CS##*:}
if [ "$want" = "$CS" ]; then
  want=1186
fi
if [ "$from" != "$want" ]; then
  die "the MySQL server at port $MYSQL_PORT has its configuration from port '$from', not from $CS"
fi
echo "== $(date +%H:%M:%S) MySQL server at port $MYSQL_PORT, of the cluster at $CS"

# Values whose bytes say where they lie (TEXT: 256 inline, parts of 2 000).
T100="REPEAT('s', 100)"
T5000="CONCAT(REPEAT('i', 256), REPEAT('a', 2000), REPEAT('b', 2000),
               REPEAT('c', 744))"
T9000="CONCAT(REPEAT('I', 256), REPEAT('A', 2000), REPEAT('B', 2000),
               REPEAT('C', 2000), REPEAT('D', 2000), REPEAT('E', 744))"
J10000="JSON_OBJECT('k', REPEAT('j', 10000))"
B100000="CONCAT(REPEAT('i', 256), REPEAT('L', 99744))"
TINY200="REPEAT('t', 200)"

sql "CREATE DATABASE IF NOT EXISTS ic_it;
  DROP TABLE IF EXISTS ic_it.blob_t;
  DROP TABLE IF EXISTS ic_it.blob_u;
  CREATE TABLE ic_it.blob_t (
    id INT NOT NULL PRIMARY KEY,
    t TEXT,
    j JSON,
    b LONGBLOB,
    tiny TINYBLOB
  ) ENGINE=NDB;
  CREATE TABLE ic_it.blob_u (
    id INT NOT NULL PRIMARY KEY,
    u INT NOT NULL,
    t TEXT,
    UNIQUE KEY blob_u_u (u)
  ) ENGINE=NDB;" || die "the MySQL server cannot make NDB tables"
if [ "$DISK" = 1 ]; then
  # The cluster's files only: InnoDB's undo tablespaces are UNDO LOG
  # files too, with no log file group.
  lg=$($MYSQL -N -e "SELECT DISTINCT logfile_group_name
                     FROM information_schema.files
                     WHERE engine = 'ndbcluster' AND file_type = 'UNDO LOG'
                     LIMIT 1")
  if [ -z "$lg" ]; then
    lg=ic_it_lg
    sql "CREATE LOGFILE GROUP $lg ADD UNDOFILE 'ic_it_undo.log'
           INITIAL_SIZE 32M UNDO_BUFFER_SIZE 4M ENGINE=NDB;" ||
      die "no log file group"
  fi
  ts=$($MYSQL -N -e "SELECT 1 FROM information_schema.files
                     WHERE engine = 'ndbcluster'
                       AND tablespace_name = 'ic_it_ts' LIMIT 1")
  if [ -z "$ts" ]; then
    sql "CREATE TABLESPACE ic_it_ts ADD DATAFILE 'ic_it_data.dat'
           USE LOGFILE GROUP $lg INITIAL_SIZE 64M ENGINE=NDB;" ||
      die "no tablespace"
  fi
  sql "DROP TABLE IF EXISTS ic_it.blob_d;
    CREATE TABLE ic_it.blob_d (
      id INT NOT NULL PRIMARY KEY,
      t TEXT STORAGE DISK
    ) TABLESPACE ic_it_ts ENGINE=NDB;" || die "no table on disk"
fi
echo "== $(date +%H:%M:%S) tables made; their ids and parts tables:"
"$RONDB_BIN/ndb_show_tables" -c "$CS" 2>/dev/null |
  grep -E 'blob_[tud]|NDB\$BLOB' || echo "   (ndb_show_tables failed)"
for table in blob_t blob_u blob_d; do
  "$RONDB_BIN/ndb_desc" -c "$CS" -d ic_it -b "$table" 2>/dev/null |
    grep -E '^-- |TableId|Blob|Text' |
    sed 's/^/   /'
done
sleep 2

step "insert, TEXT inline only (100 bytes)" \
  sql "INSERT INTO ic_it.blob_t (id, t) VALUES (1, $T100)"
step "insert, TEXT of 5 000 bytes: 256 inline, 3 parts" \
  sql "INSERT INTO ic_it.blob_t (id, t) VALUES (2, $T5000)"
step "insert, JSON of about 10 000 bytes: 4 000 inline, parts of 8 100" \
  sql "INSERT INTO ic_it.blob_t (id, j) VALUES (3, $J10000)"
step "insert, LONGBLOB of 100 000 bytes: 8 parts of 13 948" \
  sql "INSERT INTO ic_it.blob_t (id, b) VALUES (4, $B100000)"
step "insert, TINYBLOB of 200 bytes: no parts table" \
  sql "INSERT INTO ic_it.blob_t (id, tiny) VALUES (5, $TINY200)"
step "insert, ten rows of 5 000-byte TEXT in one statement" \
  sql "INSERT INTO ic_it.blob_t (id, t)
       WITH RECURSIVE seq (n) AS
         (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 10)
       SELECT 100 + n, $T5000 FROM seq"
step "insert ignoring errors, a key there (2), 9 000 bytes" \
  sql "INSERT IGNORE INTO ic_it.blob_t (id, t) VALUES (2, $T9000)"
step "insert ignoring errors, a key not there (6), 5 000 bytes" \
  sql "INSERT IGNORE INTO ic_it.blob_t (id, t) VALUES (6, $T5000)"

step "committed read, inline value (1)" \
  sql "SELECT t FROM ic_it.blob_t WHERE id = 1"
step "committed read, 3 parts (2)" \
  sql "SELECT t FROM ic_it.blob_t WHERE id = 2"
step "committed read, JSON with parts (3)" \
  sql "SELECT j FROM ic_it.blob_t WHERE id = 3"
step "committed read, LONGBLOB of 8 parts (4)" \
  sql "SELECT b FROM ic_it.blob_t WHERE id = 4"
step "committed read, TINYBLOB (5)" \
  sql "SELECT tiny FROM ic_it.blob_t WHERE id = 5"
step "committed read in an open transaction, then another read" \
  sql "BEGIN;
       SELECT t FROM ic_it.blob_t WHERE id = 2;
       SELECT id FROM ic_it.blob_t WHERE id = 1;
       COMMIT;"
step "read with a shared lock (2)" \
  sql "BEGIN;
       SELECT t FROM ic_it.blob_t WHERE id = 2 LOCK IN SHARE MODE;
       COMMIT;"
step "read with an exclusive lock (2)" \
  sql "BEGIN;
       SELECT t FROM ic_it.blob_t WHERE id = 2 FOR UPDATE;
       COMMIT;"

step "update, 5 000 to 9 000 bytes: 3 parts to 5 (2)" \
  sql "UPDATE ic_it.blob_t SET t = $T9000 WHERE id = 2"
step "update, 9 000 to 100 bytes: 5 parts to none (2)" \
  sql "UPDATE ic_it.blob_t SET t = $T100 WHERE id = 2"
step "update, 100 to 5 000 bytes: none to 3 parts (2)" \
  sql "UPDATE ic_it.blob_t SET t = $T5000 WHERE id = 2"
step "update, to NULL: 3 parts to none (2)" \
  sql "UPDATE ic_it.blob_t SET t = NULL WHERE id = 2"
step "update of a key not there (99)" \
  sql "UPDATE ic_it.blob_t SET t = $T5000 WHERE id = 99"
step "REPLACE, a row there (6, 3 parts), with 9 000 bytes" \
  sql "REPLACE INTO ic_it.blob_t (id, t) VALUES (6, $T9000)"
step "REPLACE, a row not there (7), with 5 000 bytes" \
  sql "REPLACE INTO ic_it.blob_t (id, t) VALUES (7, $T5000)"

step "delete, 5 parts (6)" \
  sql "DELETE FROM ic_it.blob_t WHERE id = 6"
step "delete, LONGBLOB of 8 parts (4)" \
  sql "DELETE FROM ic_it.blob_t WHERE id = 4"
step "delete, inline only (1)" \
  sql "DELETE FROM ic_it.blob_t WHERE id = 1"

step "unique index: insert (1, u 11), 5 000 bytes" \
  sql "INSERT INTO ic_it.blob_u VALUES (1, 11, $T5000)"
step "unique index: committed read" \
  sql "SELECT t FROM ic_it.blob_u WHERE u = 11"
step "unique index: update to 9 000 bytes" \
  sql "UPDATE ic_it.blob_u SET t = $T9000 WHERE u = 11"
step "unique index: delete" \
  sql "DELETE FROM ic_it.blob_u WHERE u = 11"

if [ "$DISK" = 1 ]; then
  step "disk: insert, 5 000 bytes, the last part short" \
    sql "INSERT INTO ic_it.blob_d VALUES (1, $T5000)"
  step "disk: committed read" \
    sql "SELECT t FROM ic_it.blob_d WHERE id = 1"
  step "disk: update to 100 bytes" \
    sql "UPDATE ic_it.blob_d SET t = $T100 WHERE id = 1"
fi
echo "== $(date +%H:%M:%S) what the tables hold now (lengths in bytes)"
sql "SELECT id, LENGTH(t), LENGTH(j), LENGTH(b), LENGTH(tiny)
     FROM ic_it.blob_t ORDER BY id;
     SELECT id, u, LENGTH(t) FROM ic_it.blob_u ORDER BY id" | sed 's/^/   /'
if [ "$FAILED" -gt 0 ]; then
  die "$FAILED steps failed"
fi
echo "== $(date +%H:%M:%S) done, every step ran"
