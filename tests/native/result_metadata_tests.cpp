// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#ifdef _WIN32
#include <Windows.h>
#endif
#include "result_metadata.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <new>
#include <stdexcept>
#include <thread>

#ifdef NDEBUG
#error Native metadata tests require assertions, including Release builds.
#endif

struct TestHandle {
    ResultMetadataCache resultMetadata;
    std::mutex* childMutex = nullptr;

    ~TestHandle() {
        if (childMutex) {
            bool acquired = false;
            std::thread observer([&] {
                acquired = childMutex->try_lock();
                if (acquired) {
                    childMutex->unlock();
                }
            });
            observer.join();
            assert(acquired);
        }
    }
};

std::weak_ptr<TestHandle> observedHandle;
bool failAllocation = false;
long ownersAtFailure = -1;

static std::shared_ptr<const ResultMetadata> MakeMetadata() {
    auto metadata = std::make_shared<ResultMetadata>();
    metadata->columns.push_back({u"owned", SQL_INTEGER, 10, 0, 1});
    return metadata;
}

static void Populate(ResultMetadataCache& cache) {
    const auto snapshot = cache.snapshot();
    cache.publish(snapshot.generation, MakeMetadata());
}

static void TestSnapshots() {
    ResultMetadataCache cache;
    const auto initial = cache.snapshot();
    assert(!initial.metadata);
    auto metadata = MakeMetadata();
    std::weak_ptr<const ResultMetadata> weak = metadata;
    cache.publish(initial.generation, metadata);
    auto held = cache.snapshot();
    assert(held.metadata == metadata);
    cache.clear();
    assert(!cache.snapshot().metadata);
    assert(cache.snapshot().generation != initial.generation);

    Populate(cache);
    const auto replacement = cache.snapshot();
    cache.publish(initial.generation, metadata);
    assert(cache.snapshot().metadata == replacement.metadata);
    assert(held.metadata->columns.at(0).name == u"owned");
    metadata.reset();
    assert(!weak.expired());
    held.metadata.reset();
    assert(weak.expired());
}

static void TestFailures() {
    ResultMetadataCache cache;
    const SQLRETURN results[] = {SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_NO_DATA,
                                 SQL_ERROR, SQL_INVALID_HANDLE};
    for (SQLRETURN result : results) {
        Populate(cache);
        const auto before = cache.snapshot();
        {
            ResultMetadataFailureGuard guard(cache, result);
        }
        const auto after = cache.snapshot();
        if (SQL_SUCCEEDED(result) || result == SQL_NO_DATA) {
            assert(after.metadata == before.metadata);
            assert(after.generation == before.generation);
        } else {
            assert(!after.metadata);
            assert(after.generation != before.generation);
        }
    }
    Populate(cache);
    SQLRETURN result = SQL_SUCCESS;
    try {
        ResultMetadataFailureGuard guard(cache, result);
        throw std::runtime_error("conversion failure");
    } catch (const std::runtime_error&) {
        assert(!cache.snapshot().metadata);
    }
}

static void TestConcurrentInvalidation() {
    ResultMetadataCache cache;
    const auto metadata = MakeMetadata();
    std::thread invalidator([&] {
        for (int i = 0; i < 1000; ++i) {
            cache.clear();
        }
    });
    for (int i = 0; i < 1000; ++i) {
        const auto snapshot = cache.snapshot();
        cache.publish(snapshot.generation, metadata);
        if (snapshot.metadata) {
            assert(snapshot.metadata->columns.at(0).name == u"owned");
        }
    }
    invalidator.join();
    cache.clear();
    assert(!cache.snapshot().metadata);
}

static void TestChildren() {
    std::mutex childMutex;
    auto first = std::make_shared<TestHandle>();
    auto second = std::make_shared<TestHandle>();
    auto unrelated = std::make_shared<TestHandle>();
    std::vector<std::weak_ptr<TestHandle>> children{first, {}, second};
    Populate(first->resultMetadata);
    Populate(second->resultMetadata);
    Populate(unrelated->resultMetadata);
    const auto held = first->resultMetadata.snapshot();
    ClearChildResultMetadata(childMutex, children);
    assert(!first->resultMetadata.snapshot().metadata);
    assert(!second->resultMetadata.snapshot().metadata);
    assert(unrelated->resultMetadata.snapshot().metadata);
    assert(held.metadata->columns.at(0).name == u"owned");
    ClearChildResultMetadata(childMutex, children);
}

static void TestAllocationFailure() {
    std::mutex childMutex;
    auto owner = std::make_shared<TestHandle>();
    observedHandle = owner;
    std::vector<std::weak_ptr<TestHandle>> children{owner};
    Populate(owner->resultMetadata);
    const auto before = owner->resultMetadata.snapshot();
    failAllocation = true;
    try {
        ClearChildResultMetadata(childMutex, children);
        assert(false);
    } catch (const std::bad_alloc&) {
        assert(ownersAtFailure == 1);
        assert(owner->resultMetadata.snapshot().metadata == before.metadata);
        assert(childMutex.try_lock());
        childMutex.unlock();
    }
    assert(!failAllocation);
    ClearChildResultMetadata(childMutex, children);
    assert(!owner->resultMetadata.snapshot().metadata);
}

static void TestLastOwner() {
    std::mutex childMutex;
    auto owner = std::make_shared<TestHandle>();
    owner->childMutex = &childMutex;
    const std::weak_ptr<TestHandle> weak = owner;
    std::vector<std::weak_ptr<TestHandle>> children{owner};
    // Drop the external owner during invalidation, leaving only the helper's snapshot.
    auto metadata = std::shared_ptr<ResultMetadata>(new ResultMetadata, [&](auto* value) {
        owner.reset();
        delete value;
    });
    const auto generation = owner->resultMetadata.snapshot().generation;
    owner->resultMetadata.publish(generation, std::move(metadata));
    ClearChildResultMetadata(childMutex, children);
    assert(!owner && weak.expired());
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Expected one native metadata test case\n";
        return 2;
    }
    const char* name = argv[1];
    if (std::strcmp(name, "snapshots") == 0) {
        TestSnapshots();
    } else if (std::strcmp(name, "failures") == 0) {
        TestFailures();
    } else if (std::strcmp(name, "concurrent") == 0) {
        TestConcurrentInvalidation();
    } else if (std::strcmp(name, "children") == 0) {
        TestChildren();
    } else if (std::strcmp(name, "allocation") == 0) {
        TestAllocationFailure();
    } else if (std::strcmp(name, "last_owner") == 0) {
        TestLastOwner();
    } else {
        std::cerr << "Unknown native metadata test case: " << name << '\n';
        return 2;
    }
    std::cout << name << " passed\n";
    return 0;
}
