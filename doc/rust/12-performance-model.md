# 12 Performance model

How latency and throughput follow from what each thread costs, so that a
change can be judged before it is built. Two tools: a handful of
operational laws that give bounds on the back of an envelope, and
`ic_model`, a discrete-event simulation of the client and the data nodes
that gives the whole curve. Both take their costs from the measurements
in chapter 06; neither replaces measuring.

## The path of a batch

```
user thread     define b ops (define_ns each, batch_ns once)
      │         one write per data node (write_ns + bytes)
      ▼
data node       receive thread: a packet, then a signal per operation
      │         execution threads: chunks of chunk_ops
      │         send thread: one write per chunk
      ▼
client          receive thread round: kevent if it slept, one read per
receive         node, route each signal, post once per user thread
      │
      ▼
user thread     woken if asleep (wake_latency_ns, wake_cpu_ns);
                complete b ops (complete_ns each), define the next
```

Every box is a server with a first-come queue. A data node thread that
has been idle longer than it spins sleeps, and work that finds it
asleep waits `node_wake_ns` for it: a lightly loaded cluster pays this
at every hand-over, a busy one not at all. What a batch pays once
(a write, a read, a round, a wake-up) is shared by its operations; what
it pays per operation is not. That split decides almost everything.

## Operational laws

**Little's law.** A user thread with `depth` batches of `b` operations
in flight, each taking `R` from send to completion, carries

    X = depth · b / R

operations a second. Deeper pipelines raise X until R starts to grow
with them, which happens when some server fills up.

**The bottleneck law.** A server with `m` threads, each operation
costing it `D` of busy time, can carry no more than `m / D`:

    X ≤ min over servers of  m_i / D_i

With the Mac calibration and batches of 300 (150 to each data node),
the servers' ceilings before cores are shared are:

| Server | m | D per operation | m / D |
|---|---|---|---|
| Data node receive threads, both nodes | 2 | `node_packet_ns / 150 + node_signal_ns` ≈ 163 ns | 12.3 M/s |
| Data node execution threads, both nodes | 8 | `node_exec_ns` 600 ns | 13.3 M/s |
| Client receive thread | 1 | `recv_ns / 150 + reply_signals · route_ns + recv_byte_ns · reply_bytes + …` ≈ 110 ns | 9 M/s at one read per node and round, more as rounds gather |
| One user thread | 1 | define + complete + writes ≈ 360 ns | 2.8 M/s |

Divide each by the stretch `f` below to get what the shared machine
allows. On the Mac `f` reaches 1.8 at eight user threads, which is what
brings 12 M/s down to the 7 to 9 measured. On Linux, chapter 06 found
the data node receive thread first: a different machine, different
ceilings.

**Cost per operation.** A client operation costs

    d = d_op + d_batch / b

with `d_op` = define + complete + routing ≈ 300 ns and `d_batch` = the
write per node, the read per node, the round, the post and the wake-up,
several microseconds. Doubling the batch halves the second term only;
past b ≈ 200 it is small, which is why the measured CPU per read stays
near 545 ns from depth 1 to depth 4.

**Wake-ups and spinning.** If replies come a mean gap `g` apart and a
thread spins `s` before it sleeps, a spin catches the next reply with
probability `1 − e^(−s/g)` (gaps taken as exponential). Each catch saves
one wake-up (`wake_cpu_ns` plus `wake_latency_ns` of delay) and each
spin costs up to `s` of CPU. Spinning pays only when `s` is short
against `g` yet `g` is short against the wake-up: with batches of 200
reads a gap is tens of microseconds, the spin misses, and it is pure
cost. That is what chapter 06 measured twice.

**Shared cores.** Threads that ask for more CPU than there are cores
run slower by the factor

    f = max(1, CPU asked for / cores)

which stretches every cost, and so lowers every server's `m / D`. A
spinning thread asks for CPU that someone else needed; the model sees
this, the laws above do not. The CPU asked for includes CPU the model
does not follow, `other_cores`: data node threads that spin while they
wait, the data node's other threads, the operating system. It is why
throughput bends gradually towards its limit instead of reaching it at
once: each thread added asks for more CPU and stretches everyone.

## The simulation: `ic_model`

`tools/ic_model` runs the path above event by event: every thread a
server with a queue, costs jittered by ±`jitter`, a warm-up discarded,
then `seconds` measured. With `cores` set it runs again with every cost
stretched by `f` until `f` settles (a mean-field view: it catches the
direction of oversubscription and understates its spikes).

```
cargo run --release -p ic_model -- show
cargo run --release -p ic_model -- sweep=depth:1,2,4,8
cargo run --release -p ic_model -- threads=8 sweep=node_recv_threads:1,2,4
cargo run --release -p ic_model -- depth=4 sweep=spin_ns:0,2000,5000,20000
```

Each line gives the rate, the median and 99th percentile batch latency,
client CPU per operation (user/system), wake-ups, reads and writes per
thousand operations, how busy the data node receive and execution
threads and the client receive threads were, how often a spin caught a
reply, the cores asked for and the stretch `f`.

### Parameters

| Group | Parameters |
|---|---|
| Load | `threads`, `depth`, `batch`, `nodes`, `receive_threads` |
| User thread | `define_ns`, `complete_ns`, `batch_ns`, `write_ns`, `write_byte_ns`, `request_bytes`, `spin_ns` |
| Client receive | `kevent_ns`, `kevent_latency_ns`, `recv_ns`, `recv_byte_ns`, `round_ns`, `route_ns`, `reply_signals`, `reply_bytes`, `post_ns` |
| Wake-up | `wake_latency_ns`, `wake_cpu_ns` |
| Network | `net_ns` (one way) |
| Data node | `node_recv_threads`, `node_packet_ns`, `node_signal_ns`, `node_exec_threads`, `node_exec_ns`, `chunk_ops`, `node_send_ns`, `node_send_op_ns`, `node_wake_ns`, `node_spin_ns` |
| Machine and run | `cores` (0: unlimited), `other_cores`, `seconds`, `warmup`, `jitter`, `seed` |
| Executors | `executors` (0: user threads as above), `clients`, `in_flight`, `submit_ns`, `deliver_ns`, `think_ns` |

### Calibration

The client costs come from profiles (sample and perf) of `ic_bench`; the
data node costs and the cores from fitting the measured points below
with the client costs held near the profile. Fitted on 2026-09-24 on
the Mac (M5 Pro: 6 fast and 12 slower cores; two release data nodes on
the same machine; `t9` reads by primary key; model values from the
Python prototype the tool was ported from):

| Run | Measured M reads/s | Model M reads/s | Measured p50 | Model p50 |
|---|---|---|---|---|
| 1 thread, depth 1, batch 200 | 1.17 – 1.35 | 1.06 | | 161 µs |
| 1 thread, depth 2, batch 200 | 1.79 | 1.94 | | 179 µs |
| 1 thread, depth 4, batch 200 | 2.55 – 2.61 | 2.40 | | 262 µs |
| 4 threads, depth 2, batch 300 | 5.80 | 6.35 | 330 µs | 319 µs |
| 8 threads, depth 2, batch 300 | 7.36 – 8.88 | 7.19 | 553 µs | 617 µs |
| 16 threads, depth 2, batch 300 | 7.93 | 7.37 | 1077 µs | 1318 µs |

With `cores=10` and `other_cores=8`, within 14 % in rate (8 % on
average) and 22 % in latency. The fit came in three steps, and each
step's failure says something:

1. Fitted to the one-thread points alone, with `cores=4` and 400 ns a
   signal in the data node receive thread, the model matched them
   within 15 % and capped eight threads at 5 M/s. It had put into a
   per-signal cost the latency that comes from data node threads
   waking. Light load cannot tell a latency from a cost; a calibration
   needs a saturated point.
2. With `node_wake_ns`, 18 cores and the eight-thread point, it
   matched those four points, and predicted 9 M/s at four threads
   already, with a sharp knee. Four threads measured 5.8: throughput
   bends gradually, which a single saturated server does not do.
3. What bends it is CPU the model did not count. At four threads the
   bench saw 392 000 preemptions in five seconds, and a data node's
   threads spin while they wait. With that CPU as `other_cores`, and
   the fast and slow cores counted as 10, every point is within 14 %.

What the model gets right without being fitted to it:

- spinning in the user thread halves its wake-ups, adds 50 to 100 ns of
  CPU per read and gains at most a few percent in rate;
- batches of 50 → 400 cut client CPU per read steeply and raise the rate;
- reads per thousand operations fall as threads are added, the receive
  thread finding more in each read (measured 8.6 at four threads, 4.9
  at eight; the model 9.9 and 6.9).

What it gets wrong: depth 1 to depth 2 gains 1.8 times in the model and
1.4 times measured; client CPU per read rises with threads when
measured (495, 512, 538 ns at 4, 8, 16) and not in the model, which
counts CPU unstretched; and the harm of spinning is understated. Each
is a thing the model has no part for: caches (a woken thread on a cold
core, a spinning thread evicting a neighbour's lines), fast and slow
cores as such, the data node's own thread structure (a read passes a
receive thread, a coordinator thread, a thread that runs the operation
on its fragment and a send thread, here merged into three stages), and
core contention as an average rather than a queue.

Predictions to check it by, at eight threads and batch 300: the data
node receive thread at close to 100 %; a second client receive thread
gains nothing; and data nodes that spin less (`other_cores` 8 → 4)
would give about 15 % more, 8.3 M/s, while ones that do not spin at all
would give up to 9.8.

### Executors

With `executors` set, the user threads are executors serving `clients`
application threads that each make one blocking call at a time
(chapter 06 §8.1). A client spends `think_ns` and `submit_ns`, and
wakes its executor if it sleeps willing to take a request; the
executor admits everything waiting, up to `in_flight` out, as one batch
of whatever size has gathered, and as replies complete requests it
spends `complete_ns` and `deliver_ns` on each and wakes the client
sleeping on its slot (`wake_cpu_ns` of the executor's time,
`wake_latency_ns` before the client runs). Each line then counts
requests, and its latency is a call's, from submit to return.

```
cargo run --release -p ic_model -- executors=1 sweep=clients:1,8,64,256
cargo run --release -p ic_model -- clients=256 sweep=executors:1,2,4
```

Predicted before the first measurement, and measured (2026-10-15, the
Mac; reads a second, median call, client CPU a call):

| Run | Predicted | Measured |
|---|---|---|
| 64 threads, a connection each, one call at a time | 238 000, 259 µs, 16.4 µs | 198 000, 319 µs, 18.3 µs |
| 64 callers, one executor | 193 000, 329 µs, 4.5 µs | 268 000, 231 µs, 9.8 µs |
| 256 callers, two executors | 451 000, 562 µs, 4.3 µs | 307 000, 821 µs, 18.1 µs |

The model had the limit right, the executor's thread waking its
callers one by one, and the batching nearly so (63 writes per thousand
calls against 86). It counted half the CPU: a caller's own cost of
going to sleep on its slot and waking, which the kernel does and the
model had no part for. That cost is now in `submit_ns`, fitted at
5 µs a call, which brings the CPU to 9.2 µs against 9.8 and the two
executors to 391 000. Two gaps remain. One executor is 28 % low: a
caller's wake-up may cost the executor less than the 3 µs of
`wake_cpu_ns`, which at 2 µs would give 238 000, but the other points
fix `wake_cpu_ns` and it stays. Two executors are 27 % high: 256
runnable threads doubled each call's kernel time in the measurement
(8.3 to 16.2 µs) and preempted 3.45 million times in ten seconds,
which a cost stretched on average across the cores does not show.

To calibrate for another machine: measure depth 1, 2 and 4 with one
user thread, and 4, 8 and 16 threads at batch 300; take the client
costs from a profile; then adjust `node_signal_ns`, `node_exec_ns`,
`node_wake_ns`, `cores` and `other_cores` until the points are within
the run-to-run spread, and change the defaults' comment in
`tools/ic_model/src/main.rs` if it is the new reference.

## Blob columns

A blob value with parts is read or written in rounds (chapter 06,
phase 12, "How each operation goes"), and every round is a batch of
its own to both sides: defined and completed in the user thread,
written and read once per data node, executed in a round at the data
nodes. An operation of `r` rounds holds its batch for `r` round trips,
and costs the client what its key operations cost:

| Operation, `p` parts | Rounds | Key operations |
|---|---|---|
| Read, all inline | 1 | 1 |
| Read with parts, committed first | 3 | 3 + p: the row committed, the row locked, the parts and the unlock |
| Read with parts, lock first | 2 | 2 + p: the row locked, the parts and the unlock |
| Update, all inline | 1, then the commit alone | 2: the head read and the row |
| Update with parts | 2 | 2 + p: the head read and the row, then the parts with the commit |

A TEXT of 4 000 bytes has 2 parts, a MEDIUMTEXT of 100 000 has 25.
`ic_model` has no rounds; to the laws above a blob operation is its
key operations, `n · d`.

### Measured

`tools/blob_bench.sh`, 2026-11-13, on the Mac (M5 Pro) against two
release data nodes built that day, with a redo log of four 1 GB files.
Every operation a transaction of its own, batches of 200 with two in
flight a thread (10 at 100 000 bytes); operations a second, and the
client's CPU an operation:

| Reads | 1 thread | CPU | 8 threads | CPU |
|---|---|---|---|---|
| VARCHAR, 200 B | 1 734 000 | 607 ns | 5 610 000 | 705 ns |
| TEXT, 200 B inline | 1 751 000 | 712 ns | 4 636 000 | 1 313 ns |
| TEXT, 200 B inline, lock first | 721 000 | 1 537 ns | 2 279 000 | 2 060 ns |
| TEXT, 4 000 B, committed first | 261 000 | 5.2 µs | 547 000 | 7.2 µs |
| TEXT, 4 000 B, lock first | 324 000 | 3.9 µs | 619 000 | 5.7 µs |
| MEDIUMTEXT, 100 000 B, committed first | 24 700 | 40.5 µs | 33 600 | 54.1 µs |
| MEDIUMTEXT, 100 000 B, lock first | 27 000 | 34.9 µs | 34 600 | 47.5 µs |

| Updates | 1 thread | CPU | 8 threads | CPU |
|---|---|---|---|---|
| VARCHAR, 200 B | 307 000 | 1.2 µs | 649 000 | 1.7 µs |
| TEXT, 200 B inline | 181 000 | 3.8 µs | 216 000 | 5.6 µs |
| TEXT, 4 000 B | 101 000 | 8.3 µs | 131 000 | 10.8 µs |
| MEDIUMTEXT, 100 000 B | 8 260 | 109 µs | 12 500 | 131 µs |

Through executors, each application thread making one blocking call
at a time, calls a second (a third run, 13:46 that day, the baseline
beside each):

| Through executors | 64 callers, 1 executor | 256, 1 | 1 024, 1 | 1 024, 2 |
|---|---|---|---|---|
| VARCHAR, 200 B, reads | 244 000 | 284 000 | 207 000 | 223 000 |
| TEXT, 4 000 B, reads | 57 200 | 104 000 | 97 000 | 111 000 |
| VARCHAR, 200 B, updates | 45 700 | 109 000 | 106 000 | 118 000 |
| TEXT, 4 000 B, updates | 21 600 | 28 100 | 53 400 | 47 900 |

Built with `count-allocs`, one thread made 4.1 allocations a
4 000-byte read and 4.2 an update, 535 and 390 bytes, where a plain
operation makes none.
Every run's rounds were the table's: 0, 2 and 1 after the first for
reads, 1 for updates with parts, and 0 for an inline update, whose
commit alone is not a round of the value's, nor is the commit alone
of an inline read that took its lock first. The inline reads with the
lock first come from a second run 20 minutes later, whose other runs
were within 5 % of these.

### What it shows

- **An inline value costs a read little on one thread**: 105 ns of
  client CPU, at the same rate. On eight threads it costs 600 ns and
  17 % of the rate, so something the blob path shares between
  threads scales badly; it is not found yet.
- **A value with parts is bound by the client.** By the laws above a
  4 000-byte read costs its key operations, 5 · 607 ns = 3.0 µs
  committed first and 2.4 µs lock first; it measured 5.2 and 3.9 µs.
  The blob path costs 1.6 to 1.7 times what its key operations cost
  as plain ones, more of it in system time than in user time. Rate
  times CPU is 1.35 cores on one thread against 1.05 for plain reads:
  the user thread is full.
- **Taking the lock first is faster whenever the value has parts**:
  24 % more reads at 4 000 bytes on one thread, 13 % on eight, 9 % and
  3 % at 100 000. **Reading the row committed first is faster for a
  value all inline, and by more**: 2.2 times the reads on one thread
  and 2.0 on eight, since the read is done in one round where the lock
  first takes a second for its commit. Inline values being the common
  case, the measurement bears out committed first as the default
  (chapter 06, phase 12, item 4).
- **An inline update pays a round trip for its commit**, which cannot
  ride the row's write, since the old head may name parts to delete:
  41 % of the VARCHAR's rate on one thread, 3.2 times the client CPU.
  It hardly scales, 216 000 on eight threads against 649 000.
- **An update with parts is not bound by the client on eight
  threads** (1.4 cores) but by the data nodes, which write three rows
  an update, two of them of 2 000 bytes.
- **At 100 000 bytes the bytes are what counts**: reads move 2.5 GB/s
  on one thread and 3.4 on eight, committed first and lock first alike
  there; updates 0.8 and 1.25 GB/s. Eight threads gain a third. A
  second client receive thread gains nothing (32 800 reads a second
  against 33 400), so the one thread reading 3.4 GB/s is not what
  holds them.
- **Through an executor, 64 callers measure a call's time, not the
  executor.** Each makes one call at a time, so by Little's law the
  rate is 64 over a call's time: 64 / 1 104 µs = 58 000 for a read of
  three rounds, 64 / 3 012 µs = 21 200 for an update, 64 / 257 µs =
  249 000 for a plain read; each within 3 % of what was measured.
  Their CPU a call, 40 and 54 µs, follows from it: the CPU a second is
  about what plain reads take, spread over fewer calls.
- **With more callers an executor gives blob reads what it gives
  plain ones, less their rounds.** 4 000-byte reads go from 57 000 at
  64 callers to 104 000 at 256, 37 % of the plain reads' 284 000 there,
  at 24 µs of client CPU a call against 12.5: twice a plain call,
  where on a pipelining thread a blob read costs eight times a plain
  one (5.2 µs against 607 ns). A caller's sleep and wake, which a call
  pays once whatever its rounds, is most of either.
- **Past 256 callers nothing gains.** Plain reads fall from 284 000 to
  207 000 at 1 024 callers, blob reads stay near 100 000, and a second
  executor adds 8 to 14 % while system time a call rises by half or
  more. With 1 024 runnable threads the kernel's waking and preempting
  them, 14 to 35 µs of system time a call, is the limit, not the
  executor's thread: on this machine about 280 000 plain calls a
  second, reached at 256 callers.
- **Blob updates batch badly through an executor.** At 256 callers
  they go at 28 000 a second, a quarter of the plain updates' 109 000:
  2.4 requests a send against 27.9, nearly a socket write a call, and
  25 000 passes of the executor's loop a second for 28 000 calls. The
  executor's thread spends itself on passes that each carry about one
  request; 1 024 callers bring the batching back, 67 a send, and the
  rate to 53 000. To look at: a pass sending a request's later round
  and its admissions in one write, and a pass with only a later round
  to send waiting a moment for more.
- **About four allocations an operation**. A first run against the
  slower debug cluster counted 47 a read and 15 KB: how many there are
  follows how the rounds fall in time, not only the operations.

## Using it

Ask the model before building: set the parameter that the change would
move to its new value, run the sweep that exposes it, and read which
server's utilisation reaches 1. A change to a server that is not the
bottleneck will not move the rate; a change that adds a per-batch cost
matters at small batches only; a change that adds CPU on a machine
already short of cores slows everything. Then build it and measure; if
the measurement and the model disagree by more than the calibration
error, the model is missing something, and that is worth knowing too.
