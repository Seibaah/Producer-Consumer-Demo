#pragma once

// Consumer-side wrapper for the Service SDK thread.
//
// LatestValueChannel answers "has anything new been published since my last
// read?" (the FRESH bit). The brief's requirement 6 asks for more: the consumer
// must determine whether a command is newer than the last *processed* command.
// That rule lives here, in one place, instead of in every caller.

#include <haptics/force_command.hpp>
#include <haptics/latest_value_channel.hpp>

#include <cstdint>

namespace haptics {

using ForceCommandChannel = LatestValueChannel<ForceCommand>;

class ForceCommandReader {
public:
    explicit ForceCommandReader(ForceCommandChannel& channel) noexcept : channel_(channel) {}

    // Consumer thread only. Never blocks.
    // Returns true and fills `out` only with a command whose sequence is strictly
    // greater than the last one this reader returned. Sequences start at 1;
    // 0 means "nothing processed yet".
    [[nodiscard]] bool try_read_newer(ForceCommand& out) noexcept {
        ForceCommand cmd{};
        if (!channel_.try_read(cmd)) {
            return false;  // nothing published since the last read
        }
        if (cmd.sequence <= last_processed_) {
            // Unreachable with a monotonic producer and a correct channel; counted
            // (not asserted) so tests can report it instead of aborting.
            ++rejected_;
            return false;
        }
        last_processed_ = cmd.sequence;
        out = cmd;
        return true;
    }

    [[nodiscard]] std::uint64_t last_processed() const noexcept { return last_processed_; }
    [[nodiscard]] std::uint64_t rejected() const noexcept { return rejected_; }

private:
    ForceCommandChannel& channel_;
    std::uint64_t last_processed_ = 0;
    std::uint64_t rejected_ = 0;
};

}  // namespace haptics
