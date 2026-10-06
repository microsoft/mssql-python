/*
 * Performance Profiling for mssql-python
 * Thread-safe performance counter with Python API
 */

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace mssql_profiling {

// Platform detection
#if defined(_WIN32) || defined(_WIN64)
    #define PROFILING_PLATFORM "windows"
#elif defined(__linux__)
    #define PROFILING_PLATFORM "linux"
#elif defined(__APPLE__) || defined(__MACH__)
    #define PROFILING_PLATFORM "macos"
#else
    #define PROFILING_PLATFORM "unknown"
#endif

struct PerfStats {
    // Accumulate in NANOSECONDS. Converting each sample to whole microseconds
    // before summing (as an earlier version did) truncated every sub-microsecond
    // call to 0, so high-frequency timers under-reported. int64 nanoseconds holds
    // ~292 years, so overflow is not a concern. get_stats() converts to us.
    int64_t total_time_ns = 0;
    int64_t call_count = 0;
    int64_t min_time_ns = INT64_MAX;
    int64_t max_time_ns = 0;
};

struct TimelineEvent {
    std::string name;
    int64_t start_us;   // offset from epoch_
    int64_t duration_us;
};

class PerformanceCounter {
private:
    std::unordered_map<std::string, PerfStats> counters_;
    std::vector<TimelineEvent> timeline_;
    // A single mutex still guards recording. Opt-in sparse sampling avoids
    // clocks, allocation and this lock for rejected samples under contention.
    std::mutex mutex_;
    // Config flags are atomic so enable()/disable()/enable_timeline() can be
    // called from a different thread than the one running timers (timers execute
    // with the GIL released). epoch_ is written under mutex_ in enable_timeline()
    // and only read under mutex_ in record(), so it needs no separate atomic.
    std::atomic<bool> enabled_{false};
    std::atomic<bool> timeline_enabled_{false};
    std::chrono::time_point<std::chrono::steady_clock> epoch_;
    // Reject samples crossing an aggregate-window boundary. Timeline restarts
    // keep the aggregate window and are handled separately in record().
    std::atomic<uint64_t> generation_{0};
    std::atomic<uint32_t> sample_every_{1};

public:
    static PerformanceCounter& instance() {
        static PerformanceCounter counter;
        return counter;
    }

    void enable() {
        std::lock_guard<std::mutex> lock(mutex_);
        // New window: move the generation so any timer still in flight from a
        // previous window is rejected by record() instead of landing here.
        generation_.fetch_add(1, std::memory_order_relaxed);
        enabled_ = true;
    }
    void disable() {
        std::lock_guard<std::mutex> lock(mutex_);
        enabled_ = false;
    }
    bool is_enabled() const { return enabled_; }
    uint64_t current_generation() const { return generation_.load(std::memory_order_relaxed); }

    void set_sample_every(uint32_t every) {
        if (every == 0)
            throw std::invalid_argument("sample_every must be at least 1");
        std::lock_guard<std::mutex> lock(mutex_);
        if (enabled_)
            throw std::runtime_error("Disable profiling before changing sample_every");
        // Never mix sampling probabilities in one aggregate or timeline.
        generation_.fetch_add(1, std::memory_order_relaxed);
        counters_.clear();
        timeline_.clear();
        sample_every_.store(every, std::memory_order_relaxed);
    }

    py::dict get_config() const {
        const auto every = sample_every_.load(std::memory_order_relaxed);
        py::dict config;
        config["sample_every"] = every;
        config["sampling"] = every == 1 ? "all" : "thread_local_prng";
        config["counts_are_samples"] = every != 1;
        config["totals_are_scaled"] = false;
        config["timers_are_inclusive"] = true;
        config["events_have_zero_duration"] = true;
        return config;
    }

    bool should_sample() const {
        if (!enabled_) return false;
        const auto every = sample_every_.load(std::memory_order_relaxed);
        if (every == 1) return true;
        // SplitMix64: independent decisions per span/event, not every Nth
        // operation (which aliases repeated operation sequences). Only thread
        // initialization uses a shared atomic; the hot PRNG state is local.
        static std::atomic<uint64_t> seeds{0};
        thread_local uint64_t state =
            seeds.fetch_add(1, std::memory_order_relaxed) * UINT64_C(0xd1342543de82ef95);
        uint64_t bits = (state += UINT64_C(0x9e3779b97f4a7c15));
        bits = (bits ^ (bits >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        bits = (bits ^ (bits >> 27)) * UINT64_C(0x94d049bb133111eb);
        bits ^= bits >> 31;
        return bits % every == 0;
    }

    void event(const char* name) noexcept {
        const auto generation = current_generation();
        if (!should_sample()) return;
        try {
            record(name, 0, std::chrono::steady_clock::now(), generation);
        } catch (...) {
            // Profiling must never affect driver error semantics.
        }
    }

    void enable_timeline() {
        std::lock_guard<std::mutex> lock(mutex_);
        // Clear stale events when (re)setting the epoch so every event in
        // timeline_ shares the current epoch; a second enable_timeline() without
        // an intervening reset() would otherwise mix offsets from two epochs.
        timeline_.clear();
        epoch_ = std::chrono::steady_clock::now();
        timeline_enabled_ = true;
    }
    void disable_timeline() {
        std::lock_guard<std::mutex> lock(mutex_);
        timeline_enabled_ = false;
    }
    bool is_timeline_enabled() const { return timeline_enabled_; }

    void record(const std::string& name, int64_t duration_ns,
                std::chrono::time_point<std::chrono::steady_clock> start, uint64_t generation) {
        if (!enabled_) return;

        std::lock_guard<std::mutex> lock(mutex_);
        // Check under the lock so disable(), enable() and resets cannot race the write.
        if (!enabled_ || generation != generation_.load(std::memory_order_relaxed))
            return;
        auto& stats = counters_[name];
        stats.total_time_ns += duration_ns;
        stats.call_count++;
        stats.min_time_ns = std::min(stats.min_time_ns, duration_ns);
        stats.max_time_ns = std::max(stats.max_time_ns, duration_ns);

        // Keep aggregate samples even if their timeline epoch has been replaced.
        if (timeline_enabled_ && start >= epoch_) {
            auto offset = std::chrono::duration_cast<std::chrono::microseconds>(start - epoch_).count();
            timeline_.push_back({name, offset, duration_ns / 1000});
        }
    }

    py::dict get_stats() {
        std::unordered_map<std::string, PerfStats> snapshot;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            snapshot = counters_;
        }
        // Python allocation may run a finalizer that re-enters native recording.
        // Only native data is copied under the mutex; build the report after unlocking.
        py::dict result;

        for (const auto& [name, stats] : snapshot) {
            py::dict d;
            // Convert accumulated nanoseconds to microseconds only here (never
            // per-sample), keeping sub-microsecond precision as fractional us so
            // high-frequency timers do not truncate to zero.
            d["total_us"] = stats.total_time_ns / 1000.0;
            d["calls"] = stats.call_count;
            d["avg_us"] = stats.call_count > 0
                              ? static_cast<double>(stats.total_time_ns) / stats.call_count / 1000.0
                              : 0.0;
            d["min_us"] = stats.min_time_ns == INT64_MAX ? 0.0 : stats.min_time_ns / 1000.0;
            d["max_us"] = stats.max_time_ns / 1000.0;
            d["platform"] = PROFILING_PLATFORM;
            result[py::str(name)] = d;
        }

        return result;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        // Counters are cleared, so any timer that started before now belongs to a
        // window that no longer exists; move the generation to reject it.
        generation_.fetch_add(1, std::memory_order_relaxed);
        counters_.clear();
        timeline_.clear();
    }

    void reset_stats_only() {
        std::lock_guard<std::mutex> lock(mutex_);
        generation_.fetch_add(1, std::memory_order_relaxed);
        counters_.clear();
    }

    py::list get_timeline() {
        std::vector<TimelineEvent> snapshot;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            snapshot = timeline_;
        }
        py::list result;
        for (const auto& ev : snapshot) {
            py::dict d;
            d["name"] = ev.name;
            d["start_us"] = ev.start_us;
            d["duration_us"] = ev.duration_us;
            result.append(d);
        }
        return result;
    }
};

// RAII timer - automatically records on destruction
class ScopedTimer {
private:
    const char* name_;
    std::chrono::time_point<std::chrono::steady_clock> start_;
    // Capture the enabled state ONCE at construction. Using this captured flag
    // (instead of re-checking is_enabled() in the destructor) means a concurrent
    // enable()/disable() between construction and destruction can never make us
    // read an uninitialized start_ or record a half-open interval.
    bool active_;
    // Window generation captured at construction, handed back to record() so a
    // sample that outlived its window is dropped rather than mis-attributed.
    uint64_t startGeneration_;

public:
    explicit ScopedTimer(const char* name)
        : name_(name), active_(false),
          startGeneration_(PerformanceCounter::instance().current_generation()) {
        active_ = PerformanceCounter::instance().should_sample();
        if (active_) {
            start_ = std::chrono::steady_clock::now();
        }
    }

    ~ScopedTimer() {
        if (active_) {
            // A destructor is implicitly noexcept: if record() threw (its
            // unordered_map insert / vector push_back can throw bad_alloc), the
            // exception would call std::terminate and crash the driver — but only
            // while profiling. Swallow any failure so profiling can never take the
            // process down; a dropped sample is an acceptable cost under OOM.
            try {
                auto end = std::chrono::steady_clock::now();
                auto duration_ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_).count();
                PerformanceCounter::instance().record(name_, duration_ns, start_, startGeneration_);
            } catch (...) {
                // ignore: never let a profiling timer abort the process
            }
        }
    }
};

} // namespace mssql_profiling

// Convenience macro - use __COUNTER__ for unique variable names even with nested timers
// __COUNTER__ is supported by MSVC, GCC, and Clang
#define PERF_TIMER_CONCAT_IMPL(x, y) x##y
#define PERF_TIMER_CONCAT(x, y) PERF_TIMER_CONCAT_IMPL(x, y)

// PERF_TIMER is gated at COMPILE TIME by the ENABLE_PROFILING flag (see CMakeLists.txt).
// Release builds define nothing -> every PERF_TIMER expands to a no-op, so there is zero
// instrumentation in the shipped binary (no code, no unwind tables, no optimizer barrier).
// Profiling builds pass -DENABLE_PROFILING -> the RAII ScopedTimer is emitted.
#ifdef ENABLE_PROFILING
    #define PERF_TIMER(name) mssql_profiling::ScopedTimer PERF_TIMER_CONCAT(_perf_timer_, __COUNTER__)("ddbc::" name)
    #define PERF_EVENT(name) mssql_profiling::PerformanceCounter::instance().event("ddbc::" name)
#else
    #define PERF_TIMER(name) do {} while(0)
    #define PERF_EVENT(name) do {} while(0)
#endif
