# Latest-value force command handoff

Bounded, non-blocking, single-producer / single-consumer handoff of `ForceCommand`
from the Application thread to the Service SDK thread, synchronized with `std::atomic`
(a lock-free triple buffer: one atomic exchange per call, no loops or retries).
The short answers to the brief's questions are at the top of [docs/DESIGN.md](docs/DESIGN.md).

```
include/haptics/force_command.hpp         ForceCommand (as specified)
include/haptics/latest_value_channel.hpp  LatestValueChannel<T>  <- the synchronization
include/haptics/force_command_reader.hpp  consumer side: only returns commands newer than the last processed
src/main.cpp                            demo: 6 kHz producer -> 4 kHz consumer, drop and latency stats
tests/concurrency_test.cpp              deterministic + concurrent test (100k commands)
docs/DESIGN.md                          synchronization model, alternatives, verification
```

## Build, test, run

Requires CMake 3.21+ and a C++20 compiler.

Windows (Visual Studio 2022 / MSVC), from a Developer PowerShell:

```
cmake --preset default
cmake --build --preset default
ctest --preset default
build\Release\demo.exe
```

Debug build (also enables the assertion that each role is used from a single thread):

```
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
build-debug\Debug\demo.exe
```

Data-race check with ThreadSanitizer (Linux / WSL only; MSVC has no ThreadSanitizer).
`--repeat until-fail:10` runs the test up to 10 times, because a race may not show up on every run.

```
sudo apt install -y build-essential cmake   # first time on a fresh Ubuntu / WSL: compiler, make, CMake
cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan --repeat until-fail:10
```

If ThreadSanitizer finds a data race, it makes the test exit with code 66 instead of 0.
`ctest` then reports the test as failed, even if the test's own output says PASSED.

If the test aborts immediately with `FATAL: ThreadSanitizer: unexpected memory mapping`, that is a
known clash between some ThreadSanitizer versions and the kernel's address randomization, not a
problem in the code. Either run `sudo sysctl vm.mmap_rnd_bits=28` and try again, or, without root,
prefix the test command: `setarch $(uname -m) -R ctest --preset tsan --repeat until-fail:10`.
