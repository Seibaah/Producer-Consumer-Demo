#pragma once

// Single-producer / single-consumer, bounded, non-blocking latest-value handoff,
// implemented as a lock-free triple buffer. Each of the three slots has exactly
// one owner at any instant:
//
//   back   - the producer writes the next command here.
//   middle - nobody; holds the latest complete publication (+ a FRESH bit).
//   front  - the consumer copies the command out of here.
//
// Ownership changes only through one atomic exchange on the middle index, so a
// slot is never written and read at the same time. See docs/DESIGN.md for the
// memory-ordering argument, the alternatives considered, and how it is tested.

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <type_traits>

namespace haptics {

// Separate producer-, consumer- and shared state onto different cache lines to
// avoid false sharing. 64 bytes is correct for x86-64 and most ARM cores; we use
// a constant instead of std::hardware_destructive_interference_size because that
// value is ABI-unstable (GCC warns when it is used in a header).
inline constexpr std::size_t kCacheLine = 64;

// What publish() did to the previous command.
enum class PublishResult {
    kNothingSkipped,  // the consumer had already taken the previous command (or there was none)
    kSkippedUnread,   // the previous command was never read and has now been overwritten
};

template <typename T>
    requires std::is_trivially_copyable_v<T>
class LatestValueChannel {
public:
    LatestValueChannel() = default;
    LatestValueChannel(const LatestValueChannel&) = delete;
    LatestValueChannel& operator=(const LatestValueChannel&) = delete;

    // Producer thread only. Never blocks: one copy + one atomic exchange.
    // The result reports whether an unread command was skipped; it is for
    // telemetry/tests, and ignoring it is fine.
    PublishResult publish(const T& value) noexcept {
        producer_guard_.check();

        slots_[back_].value = value;  // private slot: no other thread touches it

        // release: makes the write above visible to the consumer that acquires
        //          this index.
        // acquire: the slot we receive may be the one the consumer just handed
        //          back; its reads of that slot must happen-before our next write.
        const std::uint8_t previous =
            middle_.exchange(static_cast<std::uint8_t>(back_ | kFresh), std::memory_order_acq_rel);
        back_ = static_cast<std::uint8_t>(previous & kIndexMask);
        return (previous & kFresh) != 0 ? PublishResult::kSkippedUnread
                                        : PublishResult::kNothingSkipped;
    }

    // Consumer thread only. Never blocks.
    // Returns true and copies the most recent complete publication into `out` if
    // one has arrived since the last successful call; returns false otherwise
    // (`out` is left untouched).
    [[nodiscard]] bool try_read(T& out) noexcept {
        consumer_guard_.check();

        // Early-out, required for correctness: exchanging when nothing is fresh
        // would hand back an already-read slot and take an older command as if it
        // were new. Only the consumer ever clears FRESH, so if we see it set here
        // it is still set when we exchange below.
        // What this relaxed load can miss is precise: if a publication
        // happens-before this call, coherence guarantees the load sees it (or
        // something newer). Only a publication still in flight, not yet
        // synchronized with this thread, can show up one call later.
        if ((middle_.load(std::memory_order_relaxed) & kFresh) == 0) {
            return false;
        }

        // acquire: pairs with the producer's release; the slot's contents are
        //          now visible.
        // release: our earlier reads of the slot we hand back happen-before the
        //          producer reuses it.
        const std::uint8_t previous = middle_.exchange(front_, std::memory_order_acq_rel);
        front_ = static_cast<std::uint8_t>(previous & kIndexMask);

        out = slots_[front_].value;  // private slot: producer cannot be writing it
        return true;
    }

private:
    // Debug builds assert if a second live thread uses the same role (producer or
    // consumer). The check compiles away with NDEBUG; the member exists in every
    // build so the class layout never depends on it.
    class RoleGuard {
    public:
        void check() noexcept {
#ifndef NDEBUG
            const std::thread::id me = std::this_thread::get_id();
            std::thread::id expected{};  // "no owner yet"
            if (!owner_.compare_exchange_strong(expected, me, std::memory_order_relaxed)) {
                assert(expected == me && "LatestValueChannel: role used from a second thread");
            }
#endif
        }

    private:
        std::atomic<std::thread::id> owner_{};
    };

    static constexpr std::uint8_t kIndexMask = 0b011;
    static constexpr std::uint8_t kFresh = 0b100;

    struct alignas(kCacheLine) Slot {
        T value{};
    };

    Slot slots_[3]{};

    // Shared: index of the middle slot | FRESH bit. Starts at slot 1, not fresh.
    alignas(kCacheLine) std::atomic<std::uint8_t> middle_{1};

    // Producer-private: index of the back slot (+ debug role guard).
    alignas(kCacheLine) std::uint8_t back_{0};
    RoleGuard producer_guard_;

    // Consumer-private: index of the front slot (+ debug role guard).
    alignas(kCacheLine) std::uint8_t front_{2};
    RoleGuard consumer_guard_;

    static_assert(std::atomic<std::uint8_t>::is_always_lock_free);
};

}  // namespace haptics
