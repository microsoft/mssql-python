// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// param_bind_cache.hpp — handle-owned reuse of native parameter bindings.
//
// Owns the data model and helpers that let a re-executed prepared statement skip
// the SQLBindParameter loop when its parameter shape has not changed:
//
//     DetectParamTypes  ->  BindParameters  ->  SQLExecute
//     (param_detect.hpp)    (ddbc_bindings.cpp, uses this)
//
// A SqlHandle keeps one ExecuteBindingCache: its single generation of native
// input buffers plus the exact SQLBindParameter arguments ODBC currently holds.
// On the next execute, if every parameter presents identical binding metadata
// and the freshly rebuilt buffers land at the same addresses and byte lengths,
// the bind loop is skipped. The reuse is native-only: the cache holds C++
// storage (std::string / numeric buffers), never a Python object, so teardown
// is safe even on the GIL-less connection-destruction path. The reuse decision
// and the byte-exact verification that gates the skip live in BindParameters in
// ddbc_bindings.cpp; this header is only the data model and the small predicates
// and buffer helpers it depends on.
//
// Header-only, like param_detect.hpp: the helpers run once per parameter per
// execute, and the build compiles with -O3 but without LTO, so keeping them
// inline in the using translation unit avoids turning inlined code into real
// calls across a .cpp boundary on the hot path.

#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "logger_bridge.hpp"
#include "param_detect.hpp"  // ParamInfo, MAX_INLINE_BINARY, ODBC types and constants

// One entry per bound parameter: the exact SQLBindParameter arguments ODBC holds.
struct ParameterBinding {
    SQLSMALLINT direction;
    SQLSMALLINT cType;
    SQLSMALLINT sqlType;
    SQLULEN columnSize;
    SQLSMALLINT scale;
    SQLPOINTER data;
    SQLLEN length;
    SQLLEN* indicator;
};

// Native-only ownership: cleanup is safe even on GIL-less connection teardown.
// One generation per statement, never keyed by a recycled raw ODBC handle.
struct ExecuteBindingCache {
    std::vector<ParameterBinding> bindings;
    std::vector<std::shared_ptr<void>> buffers;
    std::string encoding;
    bool reusable = false;
};

// Allocate a ParamType buffer, own it as a void* in paramBuffers for book-keeping,
// and return a typed pointer to it. ctorArgs forward to ParamType's constructor.
// The reuse overload below extends this; keep both together so the header is
// self-contained and the reuse overload sees the base definition, not just a
// declaration, at instantiation.
template <typename ParamType, typename... CtorArgs>
ParamType* AllocateParamBuffer(std::vector<std::shared_ptr<void>>& paramBuffers,
                               CtorArgs&&... ctorArgs) {
    paramBuffers.emplace_back(new ParamType(std::forward<CtorArgs>(ctorArgs)...),
                              std::default_delete<ParamType>());
    LOG("AllocateParamBuffer: New owned buffer");
    return static_cast<ParamType*>(paramBuffers.back().get());
}

// Current-generation buffers being built, plus the previous generation to reuse
// in place when a buffer's byte size is unchanged.
struct ExecuteParamBuffers {
    std::vector<std::shared_ptr<void>>& current;
    const std::vector<std::shared_ptr<void>>* previous;
};

template <typename T>
static bool UpdateParamBuffer(T& target, T&& value) {
    target = std::move(value);
    return true;
}

template <typename Char>
static bool UpdateParamBuffer(std::basic_string<Char>& target, std::basic_string<Char>&& value) {
    // Equal byte lengths keep both the address and ODBC BufferLength unchanged.
    // Do not assign a string: even a capacity-preserving assignment may move it.
    if (target.size() != value.size())
        return false;
    std::copy(value.begin(), value.end(), target.begin());
    return true;
}

template <typename ParamType, typename... CtorArgs>
ParamType* AllocateParamBuffer(ExecuteParamBuffers& buffers, CtorArgs&&... ctorArgs) {
    ParamType value(std::forward<CtorArgs>(ctorArgs)...);
    const size_t index = buffers.current.size();
    if (buffers.previous && index < buffers.previous->size()) {
        const auto& previous = (*buffers.previous)[index];
        auto* target = static_cast<ParamType*>(previous.get());
        if (UpdateParamBuffer(*target, std::move(value))) {
            buffers.current.push_back(previous);
            return target;
        }
    }
    return AllocateParamBuffer<ParamType>(buffers.current, std::move(value));
}

static bool SameParameterShape(const ParameterBinding& binding, const ParamInfo& info) {
    return binding.direction == info.inputOutputType && binding.cType == info.paramCType &&
           binding.sqlType == info.paramSQLType && binding.columnSize == info.columnSize &&
           binding.scale == info.decimalDigits;
}

static bool CanCacheParameters(const std::vector<ParamInfo>& infos) {
    // Bound retained storage to SQL Server's scalar parameter limit. NULL/DAE and
    // descriptor-based/complex types deliberately use the existing uncached path.
    if (infos.empty() || infos.size() > 2100)
        return false;
    for (const auto& info : infos) {
        if (info.isDAE || info.inputOutputType != SQL_PARAM_INPUT)
            return false;
        switch (info.paramCType) {
            case SQL_C_CHAR:
            case SQL_C_WCHAR:
            case SQL_C_BINARY:
                if (info.columnSize > MAX_INLINE_BINARY)
                    return false;
                break;
            case SQL_C_BIT:
            case SQL_C_STINYINT:
            case SQL_C_TINYINT:
            case SQL_C_SSHORT:
            case SQL_C_SHORT:
            case SQL_C_UTINYINT:
            case SQL_C_USHORT:
            case SQL_C_SBIGINT:
            case SQL_C_SLONG:
            case SQL_C_LONG:
            case SQL_C_UBIGINT:
            case SQL_C_ULONG:
            case SQL_C_FLOAT:
            case SQL_C_DOUBLE:
                break;
            default:
                return false;
        }
    }
    return true;
}
