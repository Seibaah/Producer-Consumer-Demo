// Focused concurrency test for LatestValueChannel<ForceCommand>.
//
// What it checks (section 04 of the brief, plus the latest-value guarantee):
//   1. sequence never moves backward (strictly increasing across reads).
//   2. every consumed command is internally consistent (no torn reads):
//      all fields are a deterministic function of `sequence`, so a mix of
//      two publications is detected immediately.
//   3. freshness: each read returns a command at least as new as the newest
//      one known to be published before the read started.
//   4. exact accounting: every publication is either consumed once or
//      reported as overwritten by publish(); consumed + dropped == produced.
//   5. bounded memory: the channel has a fixed size, and the producer and
//      consumer loops perform exactly zero heap allocations (counted per
//      thread via a replaced global operator new).
//   6. no data race: run this binary under ThreadSanitizer
//      (cmake --preset tsan, Linux/WSL).
//
// No test framework, so it builds anywhere with just a C++20 compiler.

#include <haptics/force_command.hpp>
#include <haptics/force_command_reader.hpp>
#include <haptics/latest_value_channel.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <thread>

using haptics::ForceCommand;
using haptics::PublishResult;

// ---- per-thread allocation counter (bounded-memory check) -------------------
// thread_local: each thread counts only its own allocations, so the producer
// and consumer loops can each be required to allocate exactly zero times,
// independently of std::thread's own setup allocations.
static thread_local std::size_t t_allocations = 0;

void* operator new(std::size_t n) {
    ++t_allocations;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc{};
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

// ---- tiny check helper (main thread only) -----------------------------------
static int g_failures = 0;
#define CHECK(cond, ...)                                                    \
    do {                                                                    \
        if (!(cond)) {                                                      \
            ++g_failures;                                                   \
            std::fprintf(stderr, "CHECK FAILED %s:%d: %s | ", __FILE__,     \
                         __LINE__, #cond);                                  \
            std::fprintf(stderr, __VA_ARGS__);                              \
            std::fputc('\n', stderr);                                       \
        }                                                                   \
    } while (0)

static unsigned long long ull(std::uint64_t v) { return static_cast<unsigned long long>(v); }

// Every field derived from the sequence: a torn read cannot satisfy this.
static ForceCommand make_command(std::uint64_t seq) {
    return ForceCommand{seq, seq * 1'000ULL, static_cast<double>(seq),
                        -static_cast<double>(seq), 0.5 * static_cast<double>(seq)};
}

static bool is_consistent(const ForceCommand& c) {
    const ForceCommand e = make_command(c.sequence);
    return c.timestamp_ns == e.timestamp_ns && c.fx == e.fx && c.fy == e.fy && c.fz == e.fz;
}

// Deterministic, single-threaded check of the latest-value semantics described
// in the write-up: three publishes before one read -> the read returns only the
// newest, the two skipped ones are reported, and a second read reports "nothing new".
static void test_channel_semantics() {
    haptics::LatestValueChannel<ForceCommand> channel;
    const ForceCommand sentinel = make_command(999);
    ForceCommand out = sentinel;

    CHECK(!channel.try_read(out), "empty channel must report nothing new");
    CHECK(out.sequence == sentinel.sequence, "out must be untouched when nothing is read");

    CHECK(channel.publish(make_command(1)) == PublishResult::kNothingSkipped,
          "first publish skipped nothing");
    CHECK(channel.publish(make_command(2)) == PublishResult::kSkippedUnread,
          "publish #2 must report skipping unread #1");
    CHECK(channel.publish(make_command(3)) == PublishResult::kSkippedUnread,
          "publish #3 must report skipping unread #2");

    CHECK(channel.try_read(out), "expected a fresh command");
    CHECK(out.sequence == 3 && is_consistent(out), "expected seq 3, got %llu", ull(out.sequence));

    out = sentinel;
    CHECK(!channel.try_read(out), "same publication must not be delivered twice");
    CHECK(out.sequence == sentinel.sequence, "out must be untouched when nothing is read");

    CHECK(channel.publish(make_command(4)) == PublishResult::kNothingSkipped,
          "#3 was consumed, so #4 skipped nothing");
    CHECK(channel.try_read(out) && out.sequence == 4, "expected seq 4, got %llu", ull(out.sequence));
}

// Requirement 6: the reader only returns commands newer than the last processed one.
static void test_reader_rejects_older() {
    haptics::ForceCommandChannel channel;
    haptics::ForceCommandReader reader(channel);
    ForceCommand out{};

    CHECK(!reader.try_read_newer(out), "nothing published yet");

    channel.publish(make_command(5));
    CHECK(reader.try_read_newer(out) && out.sequence == 5, "expected seq 5, got %llu",
          ull(out.sequence));

    channel.publish(make_command(3));  // a regression a buggy producer could cause
    CHECK(!reader.try_read_newer(out), "older command must be rejected");
    CHECK(reader.rejected() == 1, "rejected=%llu", ull(reader.rejected()));
    CHECK(reader.last_processed() == 5, "last_processed=%llu", ull(reader.last_processed()));

    channel.publish(make_command(6));
    CHECK(reader.try_read_newer(out) && out.sequence == 6, "expected seq 6, got %llu",
          ull(out.sequence));
}

int main() {
    test_channel_semantics();
    test_reader_rejects_older();

    // ---- concurrent stress test --------------------------------------------
    constexpr std::uint64_t kCommands = 100'000;

    haptics::ForceCommandChannel channel;
    std::atomic<bool> producer_done{false};
    // Highest sequence whose publish() has returned. Stored with release after
    // each publish, so a consumer that acquires value p knows publication p
    // happens-before its read and must therefore see p or something newer.
    std::atomic<std::uint64_t> published_upto{0};

    // Start barrier: both threads spin until both are running, so the producer
    // cannot finish all its work before the consumer has even started (which
    // happened in fast Release builds and left the test with a single read).
    std::atomic<int> ready{0};
    auto wait_for_both = [&ready] {
        ready.fetch_add(1, std::memory_order_acq_rel);
        while (ready.load(std::memory_order_acquire) < 2) {
        }
    };

    // Producer results (read by main after join).
    std::uint64_t dropped = 0;
    std::size_t producer_allocs = 0;

    // Consumer results (read by main after join).
    haptics::ForceCommandReader reader(channel);
    std::uint64_t consumed = 0;
    std::uint64_t torn = 0;
    std::uint64_t backwards = 0;
    std::uint64_t stale = 0;
    std::uint64_t last_seq = 0;
    std::size_t consumer_allocs = 0;

    std::thread consumer([&] {
        wait_for_both();
        const std::size_t allocs_before = t_allocations;
        auto drain_once = [&] {
            const std::uint64_t newest_known = published_upto.load(std::memory_order_acquire);
            ForceCommand cmd{};
            if (reader.try_read_newer(cmd)) {
                if (!is_consistent(cmd)) ++torn;
                if (cmd.sequence <= last_seq) ++backwards;
                if (cmd.sequence < newest_known) ++stale;
                last_seq = cmd.sequence;
                ++consumed;
            } else if (reader.last_processed() < newest_known) {
                ++stale;  // something newer was published, yet we got nothing
            }
        };
        // Deliberately slower than the producer: simulate ~2 us of processing per
        // read with a busy-wait. (sleep_for is too coarse on Windows, ~1 ms+, and
        // would leave the consumer reading only a handful of times.)
        while (!producer_done.load(std::memory_order_acquire)) {
            drain_once();
            const auto until = std::chrono::steady_clock::now() + std::chrono::microseconds(2);
            while (std::chrono::steady_clock::now() < until) {
            }
        }
        drain_once();  // pick up the final publication
        consumer_allocs = t_allocations - allocs_before;
    });

    std::thread producer([&] {
        wait_for_both();
        const std::size_t allocs_before = t_allocations;
        for (std::uint64_t seq = 1; seq <= kCommands; ++seq) {
            if (channel.publish(make_command(seq)) == PublishResult::kSkippedUnread) ++dropped;
            published_upto.store(seq, std::memory_order_release);
            // ~100 ns between publishes: still ~20x faster than the consumer, but
            // stretches the run to ~10 ms so a scheduling hiccup on a busy or
            // 2-core machine cannot let the producer finish before the consumer
            // has done meaningful work (an unpaced Release loop takes ~0.3 ms).
            const auto until = std::chrono::steady_clock::now() + std::chrono::nanoseconds(100);
            while (std::chrono::steady_clock::now() < until) {
            }
        }
        producer_allocs = t_allocations - allocs_before;
        producer_done.store(true, std::memory_order_release);
    });

    producer.join();
    consumer.join();

    CHECK(consumed > 0, "consumer never received a command");
    CHECK(torn == 0, "torn reads=%llu", ull(torn));
    CHECK(backwards == 0, "non-increasing sequences=%llu", ull(backwards));
    CHECK(reader.rejected() == 0, "reader rejected %llu older commands", ull(reader.rejected()));
    CHECK(stale == 0, "reads that missed a newer published command=%llu", ull(stale));
    CHECK(last_seq == kCommands, "final read seq=%llu, expected %llu", ull(last_seq),
          ull(kCommands));
    CHECK(consumed < kCommands, "consumer was not slower; test is not exercising overwrite");
    CHECK(consumed + dropped == kCommands, "consumed=%llu + dropped=%llu != produced=%llu",
          ull(consumed), ull(dropped), ull(kCommands));
    CHECK(producer_allocs == 0, "producer loop allocated %zu times", producer_allocs);
    CHECK(consumer_allocs == 0, "consumer loop allocated %zu times", consumer_allocs);

    std::printf("produced=%llu consumed=%llu dropped=%llu sizeof(channel)=%zu "
                "loop allocs: producer=%zu consumer=%zu\n",
                ull(kCommands), ull(consumed), ull(dropped), sizeof(channel), producer_allocs,
                consumer_allocs);
    // Not a failure (scheduling dependent), but a very short overlap means the
    // run exercised little concurrency; worth re-running.
    if (consumed < 50) {
        std::printf("WARNING: only %llu concurrent reads; re-run for stronger coverage\n",
                    ull(consumed));
    }

    if (g_failures) {
        std::printf("FAILED (%d checks)\n", g_failures);
        return EXIT_FAILURE;
    }
    std::printf("PASSED\n");
    return EXIT_SUCCESS;
}
