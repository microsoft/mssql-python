// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#ifdef _WIN32
#include <Windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include "result_metadata.hpp"

struct SQL_SS_TIME2_STRUCT {
    SQLUSMALLINT hour;
    SQLUSMALLINT minute;
    SQLUSMALLINT second;
    SQLUINTEGER fraction;  // Nanoseconds.
};

struct DateTimeOffset {
    SQLSMALLINT year;
    SQLUSMALLINT month;
    SQLUSMALLINT day;
    SQLUSMALLINT hour;
    SQLUSMALLINT minute;
    SQLUSMALLINT second;
    SQLUINTEGER fraction;  // Nanoseconds.
    SQLSMALLINT timezone_hour;
    SQLSMALLINT timezone_minute;
};

struct ColumnBuffers {
    std::vector<std::vector<SQLCHAR>> charBuffers;
    std::vector<std::vector<SQLWCHAR>> wcharBuffers;
    std::vector<std::vector<SQLINTEGER>> intBuffers;
    std::vector<std::vector<SQLSMALLINT>> smallIntBuffers;
    std::vector<std::vector<SQLREAL>> realBuffers;
    std::vector<std::vector<SQLDOUBLE>> doubleBuffers;
    std::vector<std::vector<SQL_TIMESTAMP_STRUCT>> timestampBuffers;
    std::vector<std::vector<SQLBIGINT>> bigIntBuffers;
    std::vector<std::vector<SQL_DATE_STRUCT>> dateBuffers;
    std::vector<std::vector<SQL_SS_TIME2_STRUCT>> timeBuffers;
    std::vector<std::vector<SQLGUID>> guidBuffers;
    std::vector<std::vector<SQLLEN>> indicators;
    std::vector<std::vector<DateTimeOffset>> datetimeoffsetBuffers;

    ColumnBuffers(SQLSMALLINT numCols, int fetchSize)
        : charBuffers(numCols), wcharBuffers(numCols), intBuffers(numCols),
          smallIntBuffers(numCols), realBuffers(numCols), doubleBuffers(numCols),
          timestampBuffers(numCols), bigIntBuffers(numCols), dateBuffers(numCols),
          timeBuffers(numCols), guidBuffers(numCols),
          indicators(numCols, std::vector<SQLLEN>(fetchSize)),
          datetimeoffsetBuffers(numCols) {}
};

struct FetchColumnBinding {
    SQLUSMALLINT column;
    SQLSMALLINT cType;
    SQLPOINTER data;
    SQLLEN bufferLength;
    SQLLEN* indicators;
};

// Only the statement's fetch operation mutates a plan. Cancellation invalidates
// the metadata generation instead; a shared lease protects lifetime, not mutation.
class FetchBindingPlan {
  public:
    FetchBindingPlan(ResultMetadataCache::Snapshot snapshot, int size,
                     std::string charEncoding, std::string wcharEncoding, int charCtype)
        : metadata(std::move(snapshot.metadata)), generation(snapshot.generation),
          fetchSize(size), charEncoding(std::move(charEncoding)),
          wcharEncoding(std::move(wcharEncoding)), charCtype(charCtype),
          buffers(static_cast<SQLSMALLINT>(metadata->columns.size()), size) {
        bindings.reserve(metadata->columns.size());
    }

    bool matches(const ResultMetadataCache::Snapshot& snapshot, int size,
                 const std::string& charCodec, const std::string& wcharCodec, int cType) const {
        return reusable && driverMayReference.load() && generation == snapshot.generation &&
               metadata == snapshot.metadata && fetchSize == size &&
               charEncoding == charCodec && wcharEncoding == wcharCodec && charCtype == cType;
    }

    template <typename Bind, typename Set, typename Get>
    SQLRETURN attach(SQLHSTMT stmt, Bind bind, Set set, Get get) {
        reusable = false;
        needsReset = true;
        SQLRETURN ret = set(stmt, SQL_ATTR_ROW_ARRAY_SIZE,
                            reinterpret_cast<SQLPOINTER>(static_cast<intptr_t>(fetchSize)), 0);
        if (!SQL_SUCCEEDED(ret)) {
            return ret;
        }
        SQLULEN activeSize = 0;
        ret = get(stmt, SQL_ATTR_ROW_ARRAY_SIZE, &activeSize, 0, nullptr);
        if (!SQL_SUCCEEDED(ret)) {
            return ret;
        }
        if (activeSize != static_cast<SQLULEN>(fetchSize)) {
            throw std::runtime_error("ODBC changed the requested fetch row-array size");
        }
        driverMayReference = true;
        ret = set(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);
        if (!SQL_SUCCEEDED(ret)) {
            return ret;
        }
        for (const auto& column : bindings) {
            ret = bind(stmt, column.column, column.cType, column.data, column.bufferLength,
                       column.indicators);
            if (!SQL_SUCCEEDED(ret)) {
                return ret;
            }
        }
        reusable = true;
        return ret;
    }

    template <typename Unbind, typename Set>
    SQLRETURN detach(SQLHSTMT stmt, Unbind unbind, Set set) {
        reusable = false;
        if (!needsReset) {
            return SQL_SUCCESS;
        }
        SQLRETURN ret = unbind(stmt);
        if (!SQL_SUCCEEDED(ret)) {
            return ret;
        }
        ret = set(stmt, SQL_ATTR_ROWS_FETCHED_PTR, nullptr, 0);
        if (!SQL_SUCCEEDED(ret)) {
            return ret;
        }
        driverMayReference = false;
        ret = set(stmt, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(1), 0);
        if (SQL_SUCCEEDED(ret)) {
            needsReset = false;
        }
        return ret;
    }

    void resetValues() {
        rowsFetched = 0;
        for (auto& column : buffers.indicators) {
            std::fill(column.begin(), column.end(), SQL_NULL_DATA);
        }
    }

    void nativeReleased() noexcept { driverMayReference = false; }

    struct Deleter {
        void operator()(FetchBindingPlan* plan) const noexcept {
            if (plan->driverMayReference.load()) {
                // Final owner only: freeing this allocation could leave driver
                // pointers dangling after failed native cleanup or finalization.
                std::fputs("mssql-python: retaining fetch buffers after unconfirmed native "
                           "cleanup until process exit\n", stderr);
                return;
            }
            delete plan;
        }
    };

    const std::shared_ptr<const ResultMetadata> metadata;
    const uint64_t generation;
    const int fetchSize;
    const std::string charEncoding;
    const std::string wcharEncoding;
    const int charCtype;
    ColumnBuffers buffers;
    std::vector<FetchColumnBinding> bindings;
    SQLULEN rowsFetched = 0;

  private:
    bool reusable = false;
    bool needsReset = false;
    std::atomic<bool> driverMayReference{false};
};

class FetchBindingSlot {
  public:
    bool hasPlan() const noexcept { return hasPlan_.load(std::memory_order_acquire); }

    std::shared_ptr<FetchBindingPlan> snapshot() const {
        if (!hasPlan()) {
            return {};
        }
        std::lock_guard<std::mutex> lock(mutex_);
        return plan_;
    }

    void install(const std::shared_ptr<FetchBindingPlan>& plan) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (plan_) {
            throw std::logic_error("Fetch bindings must be detached before replacement");
        }
        plan_ = plan;
        hasPlan_.store(true, std::memory_order_release);
    }

    void remove(const std::shared_ptr<FetchBindingPlan>& expected) {
        std::shared_ptr<FetchBindingPlan> retired;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (plan_ == expected) {
                retired = std::move(plan_);
                hasPlan_.store(false, std::memory_order_release);
            }
        }
    }

    void nativeReleased() {
        if (!hasPlan()) {
            return;
        }
        std::shared_ptr<FetchBindingPlan> retired;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            retired = std::move(plan_);
            hasPlan_.store(false, std::memory_order_release);
        }
        if (retired) {
            retired->nativeReleased();
        }
    }

    bool eligible() const { return eligible_.load(); }

    void disableReuse() { eligible_ = false; }

  private:
    mutable std::mutex mutex_;
    std::shared_ptr<FetchBindingPlan> plan_;
    std::atomic<bool> hasPlan_{false};
    std::atomic<bool> eligible_{true};
};
