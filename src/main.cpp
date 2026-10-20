// Demo of the scenario in the brief: Application produces at ~6 kHz, the
// Service SDK consumes at ~4 kHz. Prints how many commands were produced,
// consumed and skipped, and how old each command was when the consumer got it.
//
// Latest-value semantics in numbers: about one command in three is skipped, and
// a consumed command is on average well under one producer period (~167 us) old;
// the worst case adds OS scheduling jitter but does not grow over time.
// A FIFO would instead deliver every command, with a backlog and latency that
// grow without bound (+2000 commands per second at these rates).
// (consumed + dropped is produced, minus at most one command still unread at stop.)

#include <haptics/force_command.hpp>
#include <haptics/force_command_reader.hpp>
#include <haptics/latest_value_channel.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

using namespace std::chrono;
using haptics::ForceCommand;

namespace {

std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(
        duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
}

// Fixed-rate loop helper. Waits for each tick by spinning with yield() instead
// of sleeping: OS sleep granularity (often 1-15.6 ms on Windows) is far coarser
// than the 167-250 us periods here, so sleep_until would run the loops in
// bursts at the wrong rates. Spinning costs one CPU core per loop, which is
// what real-time haptic loops typically pay for accurate timing.
template <typename Fn>
void run_at(double hz, const std::atomic<bool>& stop, Fn&& fn) {
    const auto period = duration_cast<steady_clock::duration>(duration<double>(1.0 / hz));
    auto next = steady_clock::now();
    while (!stop.load(std::memory_order_relaxed)) {
        fn();
        next += period;
        auto now = steady_clock::now();
        if (now - next > period) {
            next = now;  // fell behind (e.g. preempted): resync instead of bursting
        }
        while (now < next) {
            std::this_thread::yield();
            now = steady_clock::now();
        }
    }
}

}  // namespace

int main() {
    haptics::ForceCommandChannel channel;
    std::atomic<bool> stop{false};

    // Producer-side statistics (read after join).
    std::uint64_t produced = 0;
    std::uint64_t dropped = 0;  // overwritten before the consumer read them

    // Consumer-side statistics (read after join).
    haptics::ForceCommandReader reader(channel);
    std::uint64_t consumed = 0;
    std::uint64_t age_sum_ns = 0;
    std::uint64_t age_max_ns = 0;

    std::thread application([&] {
        run_at(6000.0, stop, [&] {
            const std::uint64_t seq = ++produced;
            const double s = static_cast<double>(seq);  // made-up forces for the demo
            const ForceCommand cmd{seq, now_ns(), 0.1 * s, 0.2 * s, 0.3 * s};
            if (channel.publish(cmd) == haptics::PublishResult::kSkippedUnread) {
                ++dropped;
            }
        });
    });

    std::thread service_sdk([&] {
        run_at(4000.0, stop, [&] {
            ForceCommand cmd{};
            if (reader.try_read_newer(cmd)) {
                const std::uint64_t age = now_ns() - cmd.timestamp_ns;
                age_sum_ns += age;
                age_max_ns = std::max(age_max_ns, age);
                ++consumed;
                // ... send cmd to the device ...
            }
        });
    });

    std::this_thread::sleep_for(seconds(2));
    stop.store(true, std::memory_order_relaxed);
    application.join();
    service_sdk.join();

    const double avg_age_us =
        consumed ? static_cast<double>(age_sum_ns) / static_cast<double>(consumed) / 1000.0 : 0.0;
    std::printf("produced=%llu consumed=%llu dropped=%llu last_seq=%llu\n",
                static_cast<unsigned long long>(produced),
                static_cast<unsigned long long>(consumed),
                static_cast<unsigned long long>(dropped),
                static_cast<unsigned long long>(reader.last_processed()));
    std::printf("command age at consumption: avg=%.1f us max=%.1f us\n", avg_age_us,
                static_cast<double>(age_max_ns) / 1000.0);
    return 0;
}
