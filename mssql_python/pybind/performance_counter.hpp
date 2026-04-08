/*
 * Performance Profiling for mssql-python
 * Thread-safe performance counter with Python API
 */

#pragma once

#include <chrono>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
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
    int64_t total_time_us = 0;
    int64_t call_count = 0;
    int64_t min_time_us = INT64_MAX;
    int64_t max_time_us = 0;
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
    std::mutex mutex_;
    bool enabled_ = false;
    bool timeline_enabled_ = false;
    std::chrono::time_point<std::chrono::high_resolution_clock> epoch_;

public:
    static PerformanceCounter& instance() {
        static PerformanceCounter counter;
        return counter;
    }

    void enable() { enabled_ = true; }
    void disable() { enabled_ = false; }
    bool is_enabled() const { return enabled_; }

    void enable_timeline() {
        timeline_enabled_ = true;
        epoch_ = std::chrono::high_resolution_clock::now();
    }
    void disable_timeline() { timeline_enabled_ = false; }
    bool is_timeline_enabled() const { return timeline_enabled_; }

    void record(const std::string& name, int64_t duration_us,
                std::chrono::time_point<std::chrono::high_resolution_clock> start) {
        if (!enabled_) return;
        
        std::lock_guard<std::mutex> lock(mutex_);
        auto& stats = counters_[name];
        stats.total_time_us += duration_us;
        stats.call_count++;
        stats.min_time_us = std::min(stats.min_time_us, duration_us);
        stats.max_time_us = std::max(stats.max_time_us, duration_us);

        if (timeline_enabled_) {
            auto offset = std::chrono::duration_cast<std::chrono::microseconds>(start - epoch_).count();
            timeline_.push_back({name, offset, duration_us});
        }
    }

    py::dict get_stats() {
        std::lock_guard<std::mutex> lock(mutex_);
        py::dict result;
        
        for (const auto& [name, stats] : counters_) {
            py::dict d;
            d["total_us"] = stats.total_time_us;
            d["calls"] = stats.call_count;
            d["avg_us"] = stats.call_count > 0 ? stats.total_time_us / stats.call_count : 0;
            d["min_us"] = stats.min_time_us == INT64_MAX ? 0 : stats.min_time_us;
            d["max_us"] = stats.max_time_us;
            d["platform"] = PROFILING_PLATFORM;
            result[py::str(name)] = d;
        }
        
        return result;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        counters_.clear();
        timeline_.clear();
    }

    void reset_stats_only() {
        std::lock_guard<std::mutex> lock(mutex_);
        counters_.clear();
    }

    py::list get_timeline() {
        std::lock_guard<std::mutex> lock(mutex_);
        py::list result;
        for (const auto& ev : timeline_) {
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
    std::chrono::time_point<std::chrono::high_resolution_clock> start_;
    
public:
    explicit ScopedTimer(const char* name) : name_(name) {
        if (PerformanceCounter::instance().is_enabled()) {
            start_ = std::chrono::high_resolution_clock::now();
        }
    }
    
    ~ScopedTimer() {
        if (PerformanceCounter::instance().is_enabled()) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
            PerformanceCounter::instance().record(name_, duration, start_);
        }
    }
};

} // namespace mssql_profiling

// Convenience macro - use __COUNTER__ for unique variable names even with nested timers
// __COUNTER__ is supported by MSVC, GCC, and Clang
#define PERF_TIMER_CONCAT_IMPL(x, y) x##y
#define PERF_TIMER_CONCAT(x, y) PERF_TIMER_CONCAT_IMPL(x, y)

// PROFILING ENABLED - Creates actual timers
#define PERF_TIMER(name) mssql_profiling::ScopedTimer PERF_TIMER_CONCAT(_perf_timer_, __COUNTER__)("ddbc::" name)

// PROFILING DISABLED - Uncomment below and comment above to make PERF_TIMER a no-op
// #define PERF_TIMER(name) do {} while(0)
