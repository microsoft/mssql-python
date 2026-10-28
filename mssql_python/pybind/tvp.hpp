// One-row ODBC variable-row binding. Included after PrepareParamBuffer so
// scalar and TVP cells share the same conversion and buffer ownership rules.
#pragma once

struct TableParamState {
    py::object value;
    py::object iterator;
    py::list sizes;
    py::list row;
    std::u16string typeName;
    std::u16string schema;
    std::vector<ParamInfo> columns;
    std::vector<SQLLEN> indicators;
    std::vector<void*> data;
    std::vector<SQLLEN> lengths;
    std::vector<std::shared_ptr<void>> rowBuffers;
    SQLLEN indicator = SQL_DATA_AT_EXEC;
    size_t rowIndex = 0;
    bool finished = false;

    explicit TableParamState(const py::handle& table)
        : value(py::reinterpret_borrow<py::object>(table)),
          sizes(table.attr("_odbc_columns")().cast<py::list>()),
          typeName(table.attr("type_name").cast<std::u16string>()),
          schema(table.attr("schema").cast<std::u16string>()),
          columns(sizes.size()), indicators(sizes.size(), SQL_DATA_AT_EXEC),
          data(sizes.size()), lengths(sizes.size()) {
        iterator = steal(PyObject_GetIter(table.attr("rows").ptr()));
        if (!iterator) throw py::error_already_set();
        for (size_t i = 0; i < sizes.size(); ++i) {
            py::tuple spec = sizes[i].cast<py::tuple>();
            columns[i].paramSQLType = spec[0].cast<SQLSMALLINT>();
            columns[i].paramCType = spec[1].cast<SQLSMALLINT>();
            columns[i].columnSize = spec[2].cast<SQLULEN>();
            columns[i].decimalDigits = spec[3].cast<SQLSMALLINT>();
        }
    }

    bool next() {
        // Drop the previous window before entering user code for the next row.
        row = py::list();
        rowBuffers.clear();
        if (PyErr_CheckSignals() != 0) throw py::error_already_set();
        py::object item = steal(PyIter_Next(iterator.ptr()));
        if (!item) {
            if (PyErr_Occurred()) throw py::error_already_set();
            finished = true;
            return false;
        }
        py::object checked = value.attr("_validate_row")(item, rowIndex++);
        row = py::reinterpret_steal<py::list>(PySequence_List(checked.ptr()));
        if (!row) throw py::error_already_set();
        for (size_t i = 0; i < columns.size(); ++i) {
            const ParamInfo& declared = columns[i];
            if (row[i].is_none()) {
                indicators[i] = SQL_NULL_DATA;
                continue;
            }
            if (declared.paramCType == SQL_C_GUID) {
                row[i] = row[i].attr("bytes_le");
            }
            // ApplyInputSizeOverride preserves decimal coefficients (no float or
            // decimal-context arithmetic) and fractional time via isoformat.
            ParamInfo converted;
            ApplyInputSizeOverride(row.ptr(), sizes[i].ptr(), i, converted);
            converted.isDAE = false;
            data[i] = nullptr;
            lengths[i] = 0;
            SQLLEN* lengthOrIndicator = nullptr;
            PrepareParamBuffer(row[i], converted, rowBuffers, "utf-8", static_cast<int>(i),
                               data[i], lengths[i], lengthOrIndicator);
            if (lengthOrIndicator) lengths[i] = *lengthOrIndicator;
            switch (declared.paramCType) {
                case SQL_C_BIT: case SQL_C_TINYINT: case SQL_C_UTINYINT:
                    lengths[i] = sizeof(SQLCHAR); break;
                case SQL_C_SHORT: lengths[i] = sizeof(SQLSMALLINT); break;
                case SQL_C_LONG: lengths[i] = sizeof(SQLINTEGER); break;
                case SQL_C_SBIGINT: lengths[i] = sizeof(int64_t); break;
                case SQL_C_FLOAT: lengths[i] = sizeof(float); break;
                case SQL_C_DOUBLE: lengths[i] = sizeof(double); break;
                case SQL_C_TYPE_DATE: lengths[i] = sizeof(SQL_DATE_STRUCT); break;
                case SQL_C_TYPE_TIME: lengths[i] = sizeof(SQL_TIME_STRUCT); break;
                case SQL_C_TYPE_TIMESTAMP: lengths[i] = sizeof(SQL_TIMESTAMP_STRUCT); break;
            }
            indicators[i] = SQL_DATA_AT_EXEC;
        }
        return true;
    }

    void releasePythonState() {
        value = py::object();
        iterator = py::object();
        sizes.release().dec_ref();
        row.release().dec_ref();
    }
};

static SQLRETURN BindTableValuedParameter(const SqlHandlePtr& handle, int paramIndex,
                                         const py::handle& value, ParamInfo& info) {
    info.table = std::make_shared<TableParamState>(value);
    TableParamState& table = *info.table;
    SQLHANDLE hStmt = handle->get();
    if (!hStmt || handle->isImplicitlyFreed())
        throw std::runtime_error("Statement closed while initializing TVP rows");
    SQLSMALLINT record = static_cast<SQLSMALLINT>(paramIndex + 1);
    // msodbcsql18 enforces ColumnSize as a cumulative row ceiling even for
    // variable-row DAE (1 rejects the second row). This is a descriptor limit,
    // not an allocation: all columns are DAE tokens with one row of storage.
    SQLRETURN rc = SQLBindParameter_ptr(
        hStmt, record, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_SS_TABLE,
        std::numeric_limits<SQLINTEGER>::max(), 0,
        nullptr, 0, &table.indicator);
    if (!SQL_SUCCEEDED(rc)) return rc;
    SQLHDESC descriptor = nullptr;
    rc = SQLGetStmtAttr_ptr(hStmt, SQL_ATTR_IMP_PARAM_DESC, &descriptor, 0, nullptr);
    if (!SQL_SUCCEEDED(rc)) return rc;
    rc = SQLSetDescField_ptr(descriptor, record, SQL_CA_SS_TYPE_NAME,
                            table.typeName.data(),
                            static_cast<SQLINTEGER>(table.typeName.size() * sizeof(SQLWCHAR)));
    if (!SQL_SUCCEEDED(rc)) return rc;
    rc = SQLSetDescField_ptr(descriptor, record, SQL_CA_SS_SCHEMA_NAME,
                            table.schema.data(),
                            static_cast<SQLINTEGER>(table.schema.size() * sizeof(SQLWCHAR)));
    if (!SQL_SUCCEEDED(rc)) return rc;
    rc = SQLGetStmtAttr_ptr(hStmt, SQL_ATTR_APP_PARAM_DESC, &descriptor, 0, nullptr);
    if (!SQL_SUCCEEDED(rc)) return rc;
    rc = SQLSetDescField_ptr(descriptor, record, SQL_DESC_DATA_PTR, &table, 0);
    if (!SQL_SUCCEEDED(rc)) return rc;
    rc = SQLSetStmtAttr_ptr(hStmt, SQL_SOPT_SS_PARAM_FOCUS,
                           reinterpret_cast<SQLPOINTER>(static_cast<intptr_t>(record)),
                           SQL_IS_INTEGER);
    if (!SQL_SUCCEEDED(rc)) return rc;
    try {
        for (size_t i = 0; i < table.columns.size(); ++i) {
            const ParamInfo& column = table.columns[i];
            rc = SQLBindParameter_ptr(
                hStmt, static_cast<SQLUSMALLINT>(i + 1), SQL_PARAM_INPUT,
                column.paramCType, column.paramSQLType, column.columnSize, column.decimalDigits,
                &table.columns[i], 0, &table.indicators[i]);
            if (!SQL_SUCCEEDED(rc)) ThrowParameterError(handle, rc, "Failed to bind TVP column");
        }
    } catch (...) {
        SQLSetStmtAttr_ptr(hStmt, SQL_SOPT_SS_PARAM_FOCUS, nullptr, SQL_IS_INTEGER);
        throw;
    }
    return SQLSetStmtAttr_ptr(hStmt, SQL_SOPT_SS_PARAM_FOCUS, nullptr, SQL_IS_INTEGER);
}

template <typename PutData>
static bool SendTableData(SQLPOINTER token, std::vector<ParamInfo>& params,
                          PutData putData, SQLRETURN& rc) {
    for (auto& param : params) {
        if (!param.table) continue;
        auto& table = *param.table;
        if (token == &table) {
            if (table.finished) throw std::runtime_error("TVP requested rows after completion");
            SQLLEN count = table.next() ? 1 : 0;
            // For the table token, the driver interprets DataPtr as the
            // row count (not a pointer to row data). Column tokens carry data.
            rc = putData(reinterpret_cast<SQLPOINTER>(static_cast<intptr_t>(count)), count);
            return true;
        }
        for (size_t i = 0; i < table.columns.size(); ++i) {
            if (token != &table.columns[i]) continue;
            if (table.indicators[i] == SQL_NULL_DATA) {
                rc = putData(nullptr, SQL_NULL_DATA);
                return true;
            }
            const auto cType = table.columns[i].paramCType;
            if (cType == SQL_C_WCHAR || cType == SQL_C_CHAR || cType == SQL_C_BINARY) {
                rc = stream_dae_chunks(table.data[i], static_cast<size_t>(table.lengths[i]), putData);
            } else {
                rc = putData(table.data[i], table.lengths[i]);
            }
            return true;
        }
    }
    return false;
}
