// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <cstdlib>
#include <memory>
#include <new>

struct TestHandle;
extern std::weak_ptr<TestHandle> observedHandle;
extern bool failAllocation;
extern long ownersAtFailure;

// Keep replacement allocation functions opaque to optimized test call sites.
void* operator new(std::size_t size) {
    if (failAllocation) {
        failAllocation = false;
        ownersAtFailure = observedHandle.use_count();
        throw std::bad_alloc();
    }
    // operator new must not recurse; size is a byte count, with no arithmetic.
    if (void* memory = std::malloc(size ? size : 1)) {  // DevSkim: ignore DS161085
        return memory;
    }
    throw std::bad_alloc();
}

void operator delete(void* memory) noexcept {
    std::free(memory);
}

void operator delete(void* memory, std::size_t) noexcept {
    std::free(memory);
}
