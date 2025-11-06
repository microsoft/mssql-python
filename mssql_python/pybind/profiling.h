// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#ifndef PROFILING_H
#define PROFILING_H

#include <chrono>
#include <string>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include <climits>

#ifdef ENABLE_PROFILING

class ScopedTimer {
public:
    ScopedTimer(const char* name) : name_(name) {
        start_ = std::chrono::high_resolution_clock::now();
    }
    
    ~ScopedTimer() {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
        ProfileStats::instance().record(name_, duration);
    }

private:
    const char* name_;
    std::chrono::high_resolution_clock::time_point start_;
};

class ProfileStats {
public:
    static ProfileStats& instance() {
        static ProfileStats instance;
        return instance;
    }
    
    void record(const std::string& name, long long microseconds) {
        auto& stat = stats_[name];
        stat.total_time += microseconds;
        stat.call_count++;
        if (microseconds < stat.min_time) stat.min_time = microseconds;
        if (microseconds > stat.max_time) stat.max_time = microseconds;
    }
    
    void print_stats() {
        std::cout << "\n=== C++ PROFILING STATS ===" << std::endl;
        std::cout << std::string(100, '=') << std::endl;
        std::cout << std::left 
                  << std::setw(40) << "Function"
                  << std::setw(10) << "Calls"
                  << std::setw(15) << "Total(ms)"
                  << std::setw(15) << "Avg(µs)"
                  << std::setw(12) << "Min(µs)"
                  << std::setw(12) << "Max(µs)"
                  << std::endl;
        std::cout << std::string(100, '-') << std::endl;
        
        for (const auto& pair : stats_) {
            const auto& stat = pair.second;
            std::cout << std::left
                      << std::setw(40) << pair.first
                      << std::setw(10) << stat.call_count
                      << std::setw(15) << std::fixed << std::setprecision(2) << (stat.total_time / 1000.0)
                      << std::setw(15) << std::fixed << std::setprecision(2) << (stat.total_time / (double)stat.call_count)
                      << std::setw(12) << stat.min_time
                      << std::setw(12) << stat.max_time
                      << std::endl;
        }
        std::cout << std::string(100, '=') << std::endl;
    }
    
    void reset() {
        stats_.clear();
    }

private:
    struct Stat {
        long long total_time = 0;
        long long call_count = 0;
        long long min_time = LLONG_MAX;
        long long max_time = 0;
    };
    
    std::unordered_map<std::string, Stat> stats_;
};

#define PROFILE_SCOPE(name) ScopedTimer timer_##__LINE__(name)
#define PROFILE_PRINT() ProfileStats::instance().print_stats()
#define PROFILE_RESET() ProfileStats::instance().reset()

#else

#define PROFILE_SCOPE(name)
#define PROFILE_PRINT()
#define PROFILE_RESET()

#endif // ENABLE_PROFILING

#endif // PROFILING_H
