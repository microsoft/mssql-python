// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <cstdlib>
#include <new>

std::size_t allocationCalls = 0;
std::size_t liveAllocations = 0;
long failAllocationAfter = -1;

// Keep replacement allocation functions opaque to optimized test call sites.
void* operator new(std::size_t size) {
    if (failAllocationAfter == 0) {
        throw std::bad_alloc();
    }
    if (failAllocationAfter > 0) {
        --failAllocationAfter;
    }
    // operator new must not recurse; size is a byte count, with no arithmetic.
    void* pointer = std::malloc(size ? size : 1);  // DevSkim: ignore DS161085
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
