# Design notes

## Threads and ownership

* **One event-loop thread** owns every `Connection`, both pools, the epoll set and all connection state.
  Nothing connection-related is shared, so there are no per-connection locks.
* **N worker threads** only ever touch a `Job` (`payload` in, `response` out). A `Job` is handed to a
  worker through the bounded queue and handed back through a completion list guarded by one short
  mutex; the loop swaps that list out in O(1).
* Wakeups are coalesced: a worker writes the `eventfd` only if the loop has not already been told
  (`wake_pending`), so a burst of completions costs one syscall.

## Why the loop never blocks

The only cross-thread operations the loop performs are `ThreadPool::try_post` (non-blocking, fails if
full) and swapping the completion list (a mutex held for a pointer swap). Handlers run on workers.
`test_event_loop_never_blocks` parks a worker in `SLEEP 600` and checks another socket is still served
within 300 ms.

## Connection state machine (`Server::Impl::service`)

Every readiness event, completion or resumed read funnels into one routine:

1. flush output if the socket is writable
2. dispatch buffered complete lines (if no batch is in flight)
3. read (`readv`) while allowed
4. dispatch again, flush again
5. close if finished (EOF/QUIT/shutdown and nothing in flight, nothing left to send)
6. level-triggered only: refresh the epoll interest mask

Flags `can_read` / `can_write` record "the kernel may have more" and are cleared only on `EAGAIN`.
That makes both trigger modes use the same code.

### Edge-triggered rules the code relies on

* Register `IN|OUT|RDHUP|ET` once. Never re-arm.
* Read until `EAGAIN` (a short read is *not* proof of an empty socket in ET mode). In level-triggered
  mode a short read is enough and saves a syscall.
* A paused connection keeps its `can_read` flag; there will be no new edge, so resuming is an explicit
  call into `service()` from whatever event freed capacity (completion, `EPOLLOUT`).
* Fairness: each connection reads at most 256 KiB per turn, then is queued on `ready` for the next
  loop iteration (`epoll_wait` timeout 0), so one firehose cannot starve the rest.
* In level-triggered mode, pausing removes `EPOLLIN|EPOLLRDHUP` from the interest mask, otherwise the
  loop would spin on a socket it refuses to read.

## Backpressure

Reading from a socket is paused when either:

* its input buffer is full (`max_inbuf_bytes`): requests are not being consumed, or
* its output queue is above `out_high_watermark`: the client is not consuming responses.

It resumes when input is under half the limit **and** output is under half the watermark (hysteresis, to
avoid flapping). Pausing means the kernel receive buffer fills, TCP's window closes, and the sender
is slowed at the source. Nothing is dropped.

If no job slot is available (`inflight >= queue_capacity + workers`, or the queue is full), the
connection waits on a retry list that is walked after each batch of completions.

With backpressure **off**, the same pressure becomes failure instead of waiting: a full input buffer or
oversize output closes the connection (`overflow_closes`), and a missing job slot answers every
request in the batch with `-BUSY` (`shed_busy`). This is the baseline the benchmark compares against.

## Buffers and allocation

* `Buffer` (input): contiguous, compacts instead of growing, handed back to a pool as soon as it is
  empty, so an idle connection holds **no** input buffer at all.
* `OutQueue`: a FIFO of 16 KiB `Chunk`s from a pool, drained with `writev` over up to 16 chunks.
* `Job`: strings keep their capacity between uses; oversized ones (> 256 KiB) are shrunk.
* Tasks are `{function pointer, void*}`: queueing one never allocates.
* Per-connection setup does allocate (the `shared_ptr<Connection>` and the chunk-slot vector); the
  request path does not. `tests/test_alloc.cpp` verifies it by counting `operator new` on server
  threads over 80,000 requests on persistent connections.

## Graceful shutdown

`stop()` is async-signal-safe (atomic store + `write` to the eventfd). The loop then:

1. closes the listener (new connects are refused),
2. stops reading and dispatching on every connection,
3. lets in-flight batches finish, flushes their output, closes each connection when idle,
4. exits when no connections or jobs remain, or after `drain_timeout_ms` (remaining connections are
   force-closed),
5. closes the task queue; workers finish what is already queued and are joined.

Requests received but not yet dispatched when shutdown begins are discarded; everything handed to a
worker is answered.

## Closed connections and stale events

An `epoll_wait` batch can contain an event for a connection closed earlier in the same batch. Closed
connections release their fd and buffers immediately but their object stays alive (a graveyard list)
until the end of the iteration, so the stale `data.ptr` is always valid and ignored via `closed`.
