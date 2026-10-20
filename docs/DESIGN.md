# Synchronization model

**Design: lock-free triple buffer.** Three fixed `ForceCommand` slots plus one
`std::atomic<uint8_t>` holding the index of the "middle" slot and a `FRESH` bit.
The producer owns one slot, the consumer owns another, and the middle slot holds
the latest complete command. Ownership moves only through an atomic `exchange`.

## Summary: the six questions from the brief

1. **Shared data:** the 1-byte atomic `middle_` (slot index + `FRESH` bit). The three
   slots are shared over time but never at the same time: whoever owns a slot is the only
   thread touching it.
2. **Publishing operation:** `middle_.exchange(back_ | FRESH, acq_rel)` in `publish()`.
   Before it, the command is in a slot only the producer can reach; after it, that slot is
   the middle.
3. **Memory ordering:** `acq_rel` on both exchanges. Release publishes the producer's writes
   and the consumer's finished reads; acquire receives them. Producer-release/consumer-acquire
   makes the command visible; consumer-release/producer-acquire stops the producer from
   overwriting a slot the consumer was still reading.
4. **No partial command:** a slot is never written and read at the same time, because it has
   exactly one owner and changes owner only through the atomic exchange. Tearing is
   impossible by construction; there is no detect-and-retry step.
5. **Three publishes, one read:** each publish parks the new command in the middle and
   recycles the previous middle. The consumer's one read gets the third; the first two are
   overwritten, and `publish()` reports each as `kSkippedUnread`. A second read returns
   "nothing new".
6. **Lost intermediate commands:** yes, by design. A force command is the state to render
   *now*; a stale force is worse than a skipped one, and queuing would add latency and memory
   that grow without bound (6 kHz in, 4 kHz out = +2,000 commands/s).

Neither side ever waits or retries: each call is one atomic exchange, with no loops in our
code. That is **wait-free** on x86 and ARMv8.1+, where the exchange is a single instruction;
on ARMv8.0 the exchange itself compiles to a load-linked/store-conditional retry loop, so
there it is lock-free. Memory is fixed (`sizeof(channel)` = 384 bytes), and
`ForceCommandReader` enforces requirement 6 by only returning commands with a higher
`sequence` than the last one processed.

---

## Details

### The protocol

| Role     | Owner     | Contents                                         |
|----------|-----------|--------------------------------------------------|
| `back`   | producer  | the command currently being written              |
| `middle` | nobody    | the latest **complete** publication              |
| `front`  | consumer  | the command the consumer is copying / last took  |

```
publish(cmd):  slots[back] = cmd                                  // private slot
               old  = middle.exchange(back | FRESH, acq_rel)      // <- publication point
               back = old & INDEX                                 // recycle previous middle
               return (old & FRESH) ? kSkippedUnread : kNothingSkipped

try_read(out): if (middle.load(relaxed) & FRESH) == 0: return false
               old   = middle.exchange(front, acq_rel)            // take middle, clear FRESH
               front = old & INDEX
               out   = slots[front]                               // private slot
               return true
```

`back_` and `front_` are plain bytes private to one thread each. Slots, `middle_`, `back_`
and `front_` sit on separate cache lines to avoid false sharing.

### The relaxed early-out in `try_read()`

It is required for correctness, not just speed: exchanging when nothing is fresh would hand
back an already-read slot as the middle and take an older command as if it were new. It is
safe because only the consumer clears `FRESH`: if the load sees it set, the exchange sees it
set too.

What the relaxed load can miss is precise. If a publication *happens-before* the call (the
consumer has learned of it through any synchronization), coherence guarantees the load sees
that publication or a newer one, so `try_read()` delivers it. Only a publication still in
flight, not yet synchronized with the consumer, can show up one call later. No data is read
based on that load. The stress test asserts exactly this guarantee (the freshness check).

### Trace: three publishes while the consumer processes one

Start: back=0, middle=1, front=2 (consumer holding cmd #10).

| Step        | back | middle    | front | `publish()` returns | Note                                        |
|-------------|------|-----------|-------|---------------------|---------------------------------------------|
| publish #11 | 1    | 0 + FRESH | 2     | `kNothingSkipped`   | #11 parked in slot 0                        |
| publish #12 | 0    | 1 + FRESH | 2     | `kSkippedUnread`    | #12 parked in slot 1; slot 0 (#11) recycled |
| publish #13 | 1    | 0 + FRESH | 2     | `kSkippedUnread`    | #13 parked in slot 0; slot 1 (#12) recycled |
| try_read    | 1    | 2         | 0     |                     | consumer gets **#13**, hands back slot 2    |
| try_read    | 1    | 2         | 0     |                     | `FRESH` clear -> returns false              |

### Requirement 6: newer than the last processed command

- `try_read()` returns `true` only for a publication not delivered before (the `FRESH` bit).
- `ForceCommandReader::try_read_newer()` additionally compares `sequence` with the last
  processed one and rejects anything not strictly newer (counted, for tests). The channel
  stays generic; the `ForceCommand`-specific rule lives in one place.

### Debug-only contract check

In Debug builds each role (producer, consumer) records the first thread that uses it and
asserts if another thread calls in. Release builds compile the check away; the guard's
member exists in every build, so the class layout never depends on `NDEBUG`.

---

## Design choice and alternatives considered

The brief names *latest-value slot, double buffering, immutable publication, or another
atomic design*.

| Option | Verdict | Why |
|---|---|---|
| **`std::atomic<ForceCommand>`** (naive latest-value slot) | Rejected | 40 bytes is not lock-free on mainstream targets; the standard library falls back to an internal lock, which violates "must not block". |
| **Seqlock** (latest-value slot + version counter) | Viable, not chosen | 1 slot, very common. But the consumer must *retry* when it races a write, and a fast producer can starve it. Strictly in C++ the speculative copy is a data race unless every field is accessed atomically, with careful fences. TSan flags naive versions. |
| **Double buffer** (2 slots + atomic index) | Rejected | When the consumer holds one slot and the producer has filled the other, the producer's *next* write must overwrite the only complete newer command in place. A consumer arriving mid-write must then read a torn value, wait, or report nothing although a complete command had been published. The third slot removes that window. |
| **Immutable publication** (`atomic<ForceCommand*>`, new object per publish) | Rejected | A single pointer exchange publishes, which is elegant. But it allocates on the real-time path, and freeing old objects safely needs reclamation (hazard pointers / epochs / RCU). `std::atomic<std::shared_ptr>` is not lock-free in libstdc++ or MSVC. A fixed pool to avoid allocation turns it back into an N-buffer. |
| **Bounded SPSC ring, consumer skips to newest** | Rejected | Bounded, but more state than latest-value semantics need, and overwrite-when-full means the producer touching slots the consumer may be reading. |
| **Triple buffer** | **Chosen** | No waiting or retries on either side (one exchange per call), torn reads impossible by construction, always delivers the newest complete command, fixed size. The cost is two extra slots and one copy per side. |

## Verification

`tests/concurrency_test.cpp`:

- **Deterministic semantics:** three publishes then one read returns only #3, and
  `publish()` reports the two overwrites; a second read returns false and leaves `out`
  untouched; the reader rejects an older sequence.
- **Concurrent stress:** 100,000 commands, producer ~20x faster than the consumer, both
  started behind a barrier so they overlap (typically 3,000-30,000 concurrent reads). For
  every read:
  - all fields match `sequence` (a torn read is detected);
  - `sequence` strictly increases;
  - **freshness:** the delivered command is at least as new as the newest publication known
    to have completed before the read began (the producer publishes `published_upto` with
    release after each `publish()`);
  - and at the end: the final command is delivered, **`consumed + dropped == produced`**
    exactly, and the producer and consumer loops performed **zero** heap allocations
    (per-thread counter in a replaced `operator new`).
- **Data races:** `cmake --preset tsan` (Linux/WSL) runs the same test under
  ThreadSanitizer. It is clean.

**Does the test catch real bugs?** Each mutation below was applied to the channel and run:

| Mutation | Plain build (x86) | ThreadSanitizer |
|---|---|---|
| Naive single slot + flag | fails: 50-94 torn reads per run | data race, every run |
| Remove the relaxed early-out | fails: deterministic checks | fails |
| Producer exchange `release` only | passes | data race, every run |
| Consumer exchange `acquire` only | passes | data race in 17 of 20 runs (two batches of 10) |
| All exchanges `relaxed` | passes | data race, every run |

The ordering mutations pass on x86 because its hardware memory model is stronger than
C++'s. This is why the TSan run is part of the verification and not optional. A TSan report
makes the process exit with code 66, so `ctest` fails even though the test prints PASSED.
Because detection can be intermittent, run the TSan test several times
(`ctest --preset tsan --repeat until-fail:10`).
