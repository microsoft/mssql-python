// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <sql.h>
#include <string>
#include <vector>

struct FetchColumnMetadata {
    std::u16string name;
    SQLSMALLINT dataType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    SQLSMALLINT nullable;
};

struct ResultMetadata {
    std::vector<FetchColumnMetadata> columns;
    bool namesValidated = false;
};

// Native-only ownership: cleanup/cancellation may run without the GIL. No ODBC,
// Python, or parent/child handle locks may be acquired while holding this mutex.
class ResultMetadataCache {
  public:
    struct Snapshot {
        uint64_t generation;
        std::shared_ptr<const ResultMetadata> metadata;
    };

    Snapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return {generation_, metadata_};
    }

    void publish(uint64_t generation, std::shared_ptr<const ResultMetadata> metadata) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (generation == generation_) {
            metadata_ = std::move(metadata);
        }
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++generation_;
        metadata_.reset();
    }

  private:
    mutable std::mutex mutex_;
    uint64_t generation_ = 0;
    std::shared_ptr<const ResultMetadata> metadata_;
};

class ResultMetadataFailureGuard {
  public:
    ResultMetadataFailureGuard(ResultMetadataCache& cache, const SQLRETURN& result)
        : cache_(cache), result_(result), exceptions_(std::uncaught_exceptions()) {}

    ~ResultMetadataFailureGuard() {
        if (std::uncaught_exceptions() > exceptions_ ||
            (!SQL_SUCCEEDED(result_) && result_ != SQL_NO_DATA)) {
            cache_.clear();
        }
    }

  private:
    ResultMetadataCache& cache_;
    const SQLRETURN& result_;
    int exceptions_;
};
