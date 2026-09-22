// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Compile as C++17 with mssql_python/pybind on the include path. No driver or
// Python initialization: these tests exercise the production binding helper.
#ifdef NDEBUG
#error "fetch_bindings_test requires assertions enabled"
#endif

#include "fetch_bindings.hpp"
#include <array>
#include <cassert>
#include <cstdlib>
#include <new>

namespace {
size_t allocationCalls = 0;
size_t liveAllocations = 0;
long failAllocationAfter = -1;
}

void* operator new(std::size_t size) {
    if (failAllocationAfter == 0) {
        throw std::bad_alloc();
    }
    if (failAllocationAfter > 0) {
        --failAllocationAfter;
    }
    void* pointer = std::malloc(size ? size : 1);
    if (!pointer) {
        throw std::bad_alloc();
    }
    ++allocationCalls;
    ++liveAllocations;
    return pointer;
}

void operator delete(void* pointer) noexcept {
    if (pointer) {
        --liveAllocations;
    }
    std::free(pointer);
}

void operator delete(void* pointer, std::size_t) noexcept { ::operator delete(pointer); }

namespace {

struct Driver {
    int calls = 0;
    int failCall = -1;
    int diagnostic = 0;
    int bindCalls = 0;
    int unbindCalls = 0;
    int attributeCalls = 0;
    SQLULEN rowArraySize = 1;
    SQLULEN* rowsFetched = nullptr;
    bool substituteSize = false;
    std::array<FetchColumnBinding, 2> bound{};

    SQLRETURN result() {
        ++calls;
        diagnostic = calls == failCall ? 9000 + calls : 0;
        return calls == failCall ? SQL_ERROR : SQL_SUCCESS;
    }

    SQLRETURN set(SQLHSTMT, SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER) {
        ++attributeCalls;
        if (attribute == SQL_ATTR_ROW_ARRAY_SIZE) {
            rowArraySize = static_cast<SQLULEN>(reinterpret_cast<uintptr_t>(value));
        } else {
            assert(attribute == SQL_ATTR_ROWS_FETCHED_PTR);
            rowsFetched = static_cast<SQLULEN*>(value);
        }
        // Deliberately retain the supplied address even when setup fails.
        return result();
    }

    SQLRETURN get(SQLHSTMT, SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER, SQLINTEGER*) {
        assert(attribute == SQL_ATTR_ROW_ARRAY_SIZE);
        *static_cast<SQLULEN*>(value) = rowArraySize + (substituteSize ? 1 : 0);
        return result();
    }

    SQLRETURN bind(SQLHSTMT, SQLUSMALLINT column, SQLSMALLINT type, SQLPOINTER data,
                   SQLLEN length, SQLLEN* indicators) {
        ++bindCalls;
        bound.at(column - 1) = {column, type, data, length, indicators};
        return result();
    }

    SQLRETURN unbind(SQLHSTMT) {
        ++unbindCalls;
        SQLRETURN ret = result();
        if (SQL_SUCCEEDED(ret)) {
            bound = {};
        }
        return ret;
    }

    SQLRETURN attach(FetchBindingPlan& plan) {
        return plan.attach(
            nullptr,
            [this](auto... args) { return bind(args...); },
            [this](auto... args) { return set(args...); },
            [this](auto... args) { return get(args...); });
    }

    SQLRETURN detach(FetchBindingPlan& plan) {
        return plan.detach(
            nullptr, [this](auto stmt) { return unbind(stmt); },
            [this](auto... args) { return set(args...); });
    }

    void fetch(SQLULEN count) {
        assert(rowsFetched);
        assert(count <= rowArraySize);
        *rowsFetched = count;
        for (SQLULEN i = 0; i < count; ++i) {
            static_cast<SQLINTEGER*>(bound[0].data)[i] = static_cast<SQLINTEGER>(i + 41);
            bound[0].indicators[i] = sizeof(SQLINTEGER);
            static_cast<SQLWCHAR*>(bound[1].data)[i * 17] = 'x';
            bound[1].indicators[i] = sizeof(SQLWCHAR);
        }
    }
};

ResultMetadataCache::Snapshot metadata() {
    auto value = std::make_shared<ResultMetadata>();
    value->namesValidated = true;
    value->columns = {{u"id", SQL_INTEGER, 10, 0, SQL_NULLABLE},
                      {u"name", SQL_WVARCHAR, 16, 0, SQL_NULLABLE}};
    return {42, std::move(value)};
}

std::shared_ptr<FetchBindingPlan> makePlan(const ResultMetadataCache::Snapshot& snapshot,
                                         int size = 2) {
    auto plan = std::shared_ptr<FetchBindingPlan>(
        new FetchBindingPlan(snapshot, size, "utf-16le", "utf-16le", SQL_C_WCHAR),
        FetchBindingPlan::Deleter{});
    plan->buffers.intBuffers[0].resize(size);
    plan->buffers.wcharBuffers[1].resize(size * 17);
    plan->bindings.push_back({1, SQL_C_SLONG, plan->buffers.intBuffers[0].data(),
                              sizeof(SQLINTEGER), plan->buffers.indicators[0].data()});
    plan->bindings.push_back({2, SQL_C_WCHAR, plan->buffers.wcharBuffers[1].data(),
                              17 * sizeof(SQLWCHAR), plan->buffers.indicators[1].data()});
    return plan;
}

void allocationFailuresBeforeBinding() {
    auto snapshot = metadata();
    const size_t before = liveAllocations;
    bool reachedSuccess = false;
    for (long failure = 0; failure < 100; ++failure) {
        failAllocationAfter = failure;
        try {
            auto plan = makePlan(snapshot);
            failAllocationAfter = -1;
            reachedSuccess = true;
        } catch (const std::bad_alloc&) {
            failAllocationAfter = -1;
        }
        assert(liveAllocations == before);
        if (reachedSuccess) {
            break;
        }
    }
    assert(reachedSuccess);
}

void compatibleHitsDoNotAllocateOrBind() {
    auto snapshot = metadata();
    FetchBindingSlot slot;
    auto plan = makePlan(snapshot);
    slot.install(plan);
    Driver driver;
    assert(driver.attach(*plan) == SQL_SUCCESS);
    const auto* data = driver.bound[0].data;
    const auto* indicators = driver.bound[0].indicators;
    const auto* fetched = driver.rowsFetched;
    const size_t allocationsBefore = allocationCalls;
    for (int i = 0; i < 10000; ++i) {
        auto lease = slot.snapshot();
        assert(lease->matches(snapshot, 2, "utf-16le", "utf-16le", SQL_C_WCHAR));
        lease->resetValues();
        assert(lease->rowsFetched == 0);
        assert(lease->buffers.indicators[0][1] == SQL_NULL_DATA);
        driver.fetch(i % 2 + 1);
        assert(driver.bound[0].data == data);
        assert(driver.bound[0].indicators == indicators);
        assert(driver.rowsFetched == fetched);
    }
    assert(allocationCalls == allocationsBefore);
    assert(driver.bindCalls == 2);
    assert(driver.attributeCalls == 2);
    assert(driver.unbindCalls == 0);
    assert(!plan->matches(snapshot, 1, "utf-16le", "utf-16le", SQL_C_WCHAR));
    assert(!plan->matches(snapshot, 2, "utf-8", "utf-16le", SQL_C_WCHAR));
    assert(!plan->matches(snapshot, 2, "utf-16le", "utf-8", SQL_C_WCHAR));
    assert(!plan->matches(snapshot, 2, "utf-16le", "utf-16le", SQL_C_CHAR));
    auto changed = snapshot;
    ++changed.generation;
    assert(!plan->matches(changed, 2, "utf-16le", "utf-16le", SQL_C_WCHAR));
    changed = metadata();
    assert(!plan->matches(changed, 2, "utf-16le", "utf-16le", SQL_C_WCHAR));
    assert(driver.detach(*plan) == SQL_SUCCESS);
    slot.remove(plan);
    assert(driver.rowsFetched == nullptr);
    assert(driver.rowArraySize == 1);
    assert(!slot.snapshot());
}

void partialSetupPreservesOwnershipAndDiagnostics() {
    for (int fail = 1; fail <= 5; ++fail) {
        auto snapshot = metadata();
        FetchBindingSlot slot;
        auto plan = makePlan(snapshot);
        slot.install(plan);
        Driver driver;
        driver.failCall = fail;
        assert(driver.attach(*plan) == SQL_ERROR);
        assert(driver.calls == fail);
        assert(driver.diagnostic == 9000 + fail);
        assert(slot.snapshot() == plan);
        assert(!plan->matches(snapshot, 2, "utf-16le", "utf-16le", SQL_C_WCHAR));
        driver.failCall = -1;
        assert(driver.detach(*plan) == SQL_SUCCESS);
        slot.remove(plan);
    }
}

void partialDetachNeverReleasesStorage() {
    for (int fail = 1; fail <= 3; ++fail) {
        auto plan = makePlan(metadata());
        FetchBindingSlot slot;
        slot.install(plan);
        Driver driver;
        assert(driver.attach(*plan) == SQL_SUCCESS);
        driver.failCall = driver.calls + fail;
        assert(driver.detach(*plan) == SQL_ERROR);
        assert(driver.diagnostic == 9000 + driver.failCall);
        assert(slot.snapshot() == plan);
        if (driver.bound[0].data && driver.rowsFetched) {
            driver.fetch(1);
            assert(plan->buffers.intBuffers[0][0] == 41);
        }
        driver.failCall = -1;
        assert(driver.detach(*plan) == SQL_SUCCESS);
        slot.remove(plan);
    }
}

void releaseWaitsForConversionLease() {
    auto plan = makePlan(metadata());
    FetchBindingSlot slot;
    slot.install(plan);
    Driver driver;
    assert(driver.attach(*plan) == SQL_SUCCESS);
    driver.fetch(1);
    std::weak_ptr<FetchBindingPlan> weak = plan;
    // Simulate the notification sent ONLY after native free/disconnect succeeds.
    slot.nativeReleased();
    assert(!slot.snapshot());
    assert(!weak.expired());
    assert(plan->buffers.intBuffers[0][0] == 41);
    plan.reset();
    assert(weak.expired());
}

void finalOwnerRetainsUnconfirmedDriverPointers() {
    auto plan = makePlan(metadata());
    Driver driver;
    assert(driver.attach(*plan) == SQL_SUCCESS);
    auto* retained = plan.get();
    failAllocationAfter = 0;
    plan.reset();
    failAllocationAfter = -1;
    driver.fetch(1);
    assert(retained->buffers.intBuffers[0][0] == 41);
    // The test owns the deliberately abandoned raw allocation solely to reclaim
    // it after proving that a driver can still access its original pointers.
    retained->nativeReleased();
    delete retained;
}

void substitutedSizeCannotFetch() {
    auto plan = makePlan(metadata());
    Driver driver;
    driver.substituteSize = true;
    bool threw = false;
    try {
        driver.attach(*plan);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);
    assert(driver.bindCalls == 0);
    assert(driver.rowsFetched == nullptr);
    assert(driver.detach(*plan) == SQL_SUCCESS);
}

void noEmergencyRetentionWithoutDriverPointers() {
    const auto snapshot = metadata();
    const size_t before = liveAllocations;
    for (int fail = 1; fail <= 2; ++fail) {
        auto plan = makePlan(snapshot);
        Driver driver;
        driver.failCall = fail;
        assert(driver.attach(*plan) == SQL_ERROR);
        assert(driver.rowsFetched == nullptr);
        assert(driver.bindCalls == 0);
        plan.reset();
        assert(liveAllocations == before);
    }
    auto plan = makePlan(snapshot);
    Driver driver;
    assert(driver.attach(*plan) == SQL_SUCCESS);
    driver.failCall = driver.calls + 3;
    assert(driver.detach(*plan) == SQL_ERROR);
    assert(driver.rowsFetched == nullptr);
    assert(driver.bound[0].data == nullptr);
    // Failed size restoration blocks the next operation but cannot justify
    // abandoning storage after all retained addresses have been cleared.
    plan.reset();
    assert(liveAllocations == before);
}

void errorAndCancellationInvalidateWithoutFreeingBuffers() {
    for (bool exception : {false, true}) {
        ResultMetadataCache cache;
        auto snapshot = metadata();
        cache.publish(0, snapshot.metadata);
        snapshot = cache.snapshot();
        auto plan = makePlan(snapshot);
        FetchBindingSlot slot;
        slot.install(plan);
        Driver driver;
        assert(driver.attach(*plan) == SQL_SUCCESS);
        SQLRETURN ret = SQL_SUCCESS;
        try {
            ResultMetadataFailureGuard failure(cache, ret);
            if (exception) {
                throw std::runtime_error("conversion failed");
            }
            ret = SQL_ERROR;
        } catch (const std::runtime_error&) {
        }
        assert(cache.snapshot().generation != snapshot.generation);
        assert(slot.snapshot() == plan);
        assert(!plan->matches(cache.snapshot(), 2, "utf-16le", "utf-16le", SQL_C_WCHAR));
        driver.fetch(1);
        assert(plan->buffers.intBuffers[0][0] == 41);
        assert(driver.detach(*plan) == SQL_SUCCESS);
        slot.remove(plan);
    }
    ResultMetadataCache cache;
    auto snapshot = metadata();
    cache.publish(0, snapshot.metadata);
    snapshot = cache.snapshot();
    auto plan = makePlan(snapshot);
    Driver driver;
    assert(driver.attach(*plan) == SQL_SUCCESS);
    cache.clear();
    driver.fetch(1);
    assert(plan->buffers.intBuffers[0][0] == 41);
    assert(!plan->matches(cache.snapshot(), 2, "utf-16le", "utf-16le", SQL_C_WCHAR));
    assert(driver.detach(*plan) == SQL_SUCCESS);
}

void emptySlotsDoNotAllocate() {
    FetchBindingSlot slot;
    const size_t before = allocationCalls;
    failAllocationAfter = 0;
    for (int i = 0; i < 10000; ++i) {
        assert(!slot.hasPlan());
        assert(!slot.snapshot());
        slot.nativeReleased();
    }
    failAllocationAfter = -1;
    assert(allocationCalls == before);
}

}  // namespace

int main() {
    allocationFailuresBeforeBinding();
    compatibleHitsDoNotAllocateOrBind();
    partialSetupPreservesOwnershipAndDiagnostics();
    partialDetachNeverReleasesStorage();
    releaseWaitsForConversionLease();
    finalOwnerRetainsUnconfirmedDriverPointers();
    substitutedSizeCannotFetch();
    noEmergencyRetentionWithoutDriverPointers();
    errorAndCancellationInvalidateWithoutFreeingBuffers();
    emptySlotsDoNotAllocate();
    assert(liveAllocations == 0);
}
