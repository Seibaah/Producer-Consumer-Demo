#pragma once

#include <cstdint>
#include <type_traits>

namespace haptics {

// Exactly as specified in the exercise brief.
struct ForceCommand {
    std::uint64_t sequence;
    std::uint64_t timestamp_ns;
    double fx;
    double fy;
    double fz;
};

// Whatever design we pick copies the command by value, so it must stay a plain,
// trivially copyable aggregate (40 bytes, no pointers, no ownership).
static_assert(std::is_trivially_copyable_v<ForceCommand>);
static_assert(sizeof(ForceCommand) == 40);

}  // namespace haptics
