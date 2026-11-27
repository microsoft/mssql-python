// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
#pragma once

#include "ddbc_bindings.h"
#include "py_type_cache.hpp"
#include <cmath>
#include <limits>

enum SQLVECTORTYPE { SQL_VECTOR_TYPE_FLOAT32 = 0, SQL_VECTOR_TYPE_FLOAT16 = 1 };

#pragma pack(push, 8)
struct SQL_SS_VECTOR_STRUCT {
    SQLSMALLINT dimension;
    SQLVECTORTYPE type;
    union {
        float* f32;
        float* f16;
    } data;
};
#pragma pack(pop)

inline size_t VectorBufferSize(size_t dimensions) {
    return sizeof(SQL_SS_VECTOR_STRUCT) + dimensions * sizeof(float);
}

inline void ValidateVectorShape(size_t dimensions, int subtype) {
    if (subtype != 0 && subtype != 1)
        throw py::value_error("Invalid vector subtype; expected FLOAT32 (0) or FLOAT16 (1)");
    if (dimensions == 0 || dimensions > (subtype == 0 ? 1998u : 3996u))
        throw py::value_error("Invalid vector dimensions for the selected subtype");
}

struct VectorBinding {
    std::vector<float> elements;
    SQL_SS_VECTOR_STRUCT value{};
    SQLLEN indicator = SQL_NULL_DATA;

    VectorBinding(size_t dimensions, int subtype) {
        ValidateVectorShape(dimensions, subtype);
        elements.resize(dimensions);
        value.dimension = static_cast<SQLSMALLINT>(dimensions);
        value.type = static_cast<SQLVECTORTYPE>(subtype);
        value.data.f32 = elements.data();
    }

    explicit VectorBinding(py::handle source)
        : VectorBinding(source.attr("dimensions").cast<size_t>(),
                        source.attr("base_type").cast<std::string>() == "float16" ? 1 : 0) {
        auto values = source.attr("values").cast<py::tuple>();
        if (values.size() != elements.size())
            throw py::value_error("Vector dimensions do not match its values");
        for (size_t i = 0; i < elements.size(); ++i) {
            double number = values[i].cast<double>();
            if (!std::isfinite(number) || std::abs(number) > std::numeric_limits<float>::max())
                throw py::value_error("Vector elements must be finite float32 values");
            elements[i] = static_cast<float>(number);
        }
        indicator = static_cast<SQLLEN>(VectorBufferSize(elements.size()));
    }

    VectorBinding(const VectorBinding&) = delete;
    VectorBinding& operator=(const VectorBinding&) = delete;

    py::object toPython() const {
        if (indicator == SQL_NULL_DATA) return py::none();
        ValidateVectorShape(value.dimension, value.type);
        if (static_cast<size_t>(value.dimension) > elements.size() ||
            indicator != static_cast<SQLLEN>(VectorBufferSize(value.dimension)))
            throw py::value_error("ODBC returned an invalid vector length");
        py::tuple values(value.dimension);
        for (size_t i = 0; i < static_cast<size_t>(value.dimension); ++i)
            values[i] = py::float_(elements[i]);
        // The shape check above covers what ODBC can get wrong, and SQL Server cannot
        // store non-finite elements, so skip the public constructor's per-element checks.
        PyTypeCache::initialize();
        return py::reinterpret_borrow<py::object>(PyTypeCache::vector_from_trusted)(
            values, value.type == SQL_VECTOR_TYPE_FLOAT16 ? "float16" : "float32");
    }
};
