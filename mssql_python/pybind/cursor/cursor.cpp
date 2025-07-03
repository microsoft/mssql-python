// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "cursor.h"
#include "../connection/connection.h"
#include <chrono>
#include <thread>
#include <sstream>

// Constructor
Cursor::Cursor(std::shared_ptr<ConnectionHandle> connection)
    : _connection(connection),
      _closed(false),
      _resultSetEmpty(false),
      _isPrepared(false),
      _rowcount(-1),
      _arraysize(1),
      _bufferLength(1024),
      _numCols(0) {
    initialize();
}

// Destructor
Cursor::~Cursor() {
    try {
        if (!_closed && _hstmt) {
            close();
        }
    } catch (...) {
        // Ignore exceptions in destructor
    }
}

// Initialize cursor
void Cursor::initialize() {
    allocateStatementHandle();
}

// Allocate statement handle
void Cursor::allocateStatementHandle() {
    _hstmt = _connection->allocStatementHandle();
    if (!_hstmt) {
        throw py::value_error("Failed to allocate statement handle");
    }
}

// Close the cursor
void Cursor::close() {
    if (_closed) {
        return; // Already closed
    }
    
    if (_hstmt) {
        try {
            _hstmt->free();
            _hstmt = nullptr;
        } catch (const std::exception& e) {
            LOG(std::string("Error closing cursor: ") + e.what());
        } catch (...) {
            LOG("Unknown error closing cursor");
        }
    }
    
    _closed = true;
}

// Check if cursor is closed
void Cursor::checkClosed() const {
    if (_closed) {
        throw py::value_error("Operation cannot be performed: the cursor is closed.");
    }
}

// Execute a query
void Cursor::execute(const std::wstring& query, const py::list& parameters) {
    checkClosed();
    
    // Reset cursor state
    _resultSetEmpty = false;
    _rowcount = -1;
    _description = py::list();
    _numCols = 0;
    _columnNames.clear();
    _columnTypes.clear();
    _columnSizes.clear();
    _columnPrecisions.clear();
    _columnScales.clear();
    _columnNullables.clear();
    
    // If we have parameters, bind them
    bool hasParameters = parameters.size() > 0;
    
    if (hasParameters) {
        // Prepare the statement
        SQLRETURN ret = SQLPrepare_ptr(_hstmt->handle(), 
                                    const_cast<SQLWCHAR*>(query.c_str()), 
                                    SQL_NTS);
        if (!SQL_SUCCEEDED(ret)) {
            throw py::value_error("Failed to prepare statement");
        }
        
        _isPrepared = true;
        _lastExecutedStmt = query;
        
        // Bind parameters
        bindParameters(parameters);
        
        // Execute the prepared statement
        ret = SQLExecute_ptr(_hstmt->handle());
        if (!SQL_SUCCEEDED(ret)) {
            throw py::value_error("Failed to execute prepared statement");
        }
    } else {
        // No parameters, direct execution
        SQLRETURN ret = SQLExecDirect_ptr(_hstmt->handle(), 
                                      const_cast<SQLWCHAR*>(query.c_str()), 
                                      SQL_NTS);
        if (!SQL_SUCCEEDED(ret)) {
            throw py::value_error("Failed to execute statement");
        }
        
        _isPrepared = false;
        _lastExecutedStmt = query;
    }
    
    // Get row count
    SQLRowCount_ptr(_hstmt->handle(), &_rowcount);
    
    // Prepare result set description if available
    SQLNumResultCols_ptr(_hstmt->handle(), &_numCols);
    
    if (_numCols > 0) {
        prepareDescription();
    }
}

// Bind parameters for execution
void Cursor::bindParameters(const py::list& parameters) {
    // Implementation depends on parameter types
    // For simplicity, this is a basic implementation
    for (size_t i = 0; i < parameters.size(); ++i) {
        py::object param = parameters[i];
        
        if (py::isinstance<py::str>(param)) {
            // String parameter
            std::wstring wstr = param.cast<std::wstring>();
            
            // Bind as WCHAR
            SQLBindParameter_ptr(_hstmt->handle(), i + 1, SQL_PARAM_INPUT, 
                             SQL_C_WCHAR, SQL_WVARCHAR, 
                             wstr.size(), 0, 
                             const_cast<SQLWCHAR*>(wstr.c_str()), 
                             wstr.size() * sizeof(SQLWCHAR), 
                             nullptr);
        } 
        else if (py::isinstance<py::int_>(param)) {
            // Integer parameter
            long long val = param.cast<long long>();
            SQLBindParameter_ptr(_hstmt->handle(), i + 1, SQL_PARAM_INPUT, 
                             SQL_C_SBIGINT, SQL_BIGINT, 
                             0, 0, 
                             &val, 
                             sizeof(long long), 
                             nullptr);
        }
        else if (py::isinstance<py::float_>(param)) {
            // Float parameter
            double val = param.cast<double>();
            SQLBindParameter_ptr(_hstmt->handle(), i + 1, SQL_PARAM_INPUT, 
                             SQL_C_DOUBLE, SQL_DOUBLE, 
                             0, 0, 
                             &val, 
                             sizeof(double), 
                             nullptr);
        }
        else if (py::isinstance<py::none>(param)) {
            // NULL parameter
            SQLBindParameter_ptr(_hstmt->handle(), i + 1, SQL_PARAM_INPUT, 
                             SQL_C_DEFAULT, SQL_VARCHAR, 
                             1, 0, 
                             nullptr, 
                             0, 
                             nullptr);
        }
        else {
            // Default to string for other types
            std::string str = py::str(param).cast<std::string>();
            
            // Bind as CHAR
            SQLBindParameter_ptr(_hstmt->handle(), i + 1, SQL_PARAM_INPUT, 
                             SQL_C_CHAR, SQL_VARCHAR, 
                             str.size(), 0, 
                             const_cast<char*>(str.c_str()), 
                             str.size(), 
                             nullptr);
        }
    }
}

// Execute many with a sequence of parameters
void Cursor::executemany(const std::wstring& query, const py::list& seq_of_parameters) {
    checkClosed();
    
    if (seq_of_parameters.size() == 0) {
        return;
    }
    
    // Prepare the statement once
    SQLRETURN ret = SQLPrepare_ptr(_hstmt->handle(), 
                               const_cast<SQLWCHAR*>(query.c_str()), 
                               SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        throw py::value_error("Failed to prepare statement");
    }
    
    _isPrepared = true;
    _lastExecutedStmt = query;
    _rowcount = 0;
    
    // Execute for each parameter set
    for (auto params : seq_of_parameters) {
        // Bind parameters
        bindParameters(params.cast<py::list>());
        
        // Execute the prepared statement
        ret = SQLExecute_ptr(_hstmt->handle());
        if (!SQL_SUCCEEDED(ret)) {
            throw py::value_error("Failed to execute prepared statement");
        }
        
        // Accumulate row count
        SQLLEN rows = 0;
        SQLRowCount_ptr(_hstmt->handle(), &rows);
        if (rows >= 0) {
            if (_rowcount < 0) {
                _rowcount = rows;
            } else {
                _rowcount += rows;
            }
        }
    }
}

// Prepare column descriptions
void Cursor::prepareDescription() {
    _description = py::list();
    _columnNames.resize(_numCols);
    _columnTypes.resize(_numCols);
    _columnSizes.resize(_numCols);
    _columnPrecisions.resize(_numCols);
    _columnScales.resize(_numCols);
    _columnNullables.resize(_numCols);
    
    for (SQLSMALLINT i = 0; i < _numCols; ++i) {
        SQLWCHAR colName[256] = {0};
        SQLSMALLINT colNameLen = 0;
        SQLSMALLINT dataType = 0;
        SQLULEN colSize = 0;
        SQLSMALLINT decimalDigits = 0;
        SQLSMALLINT nullable = 0;
        
        SQLRETURN ret = SQLDescribeCol_ptr(_hstmt->handle(), i + 1, 
                                       colName, sizeof(colName) / sizeof(SQLWCHAR), 
                                       &colNameLen, &dataType, &colSize, 
                                       &decimalDigits, &nullable);
        
        if (SQL_SUCCEEDED(ret)) {
            std::wstring wname(colName);
            #ifdef _WIN32
            _columnNames[i] = std::string(wname.begin(), wname.end());
            #else
            _columnNames[i] = WideToUTF8(wname);
            #endif
            _columnTypes[i] = dataType;
            _columnSizes[i] = colSize;
            _columnPrecisions[i] = colSize;
            _columnScales[i] = decimalDigits;
            _columnNullables[i] = (nullable == SQL_NULLABLE);
            
            // Create description tuple for this column
            // (name, type_code, display_size, internal_size, precision, scale, null_ok)
            py::tuple desc = py::make_tuple(
                _columnNames[i],        // name
                dataType,               // type_code
                colSize,                // display_size
                colSize,                // internal_size
                colSize,                // precision
                decimalDigits,          // scale
                (nullable == SQL_NULLABLE) // null_ok
            );
            
            _description.append(desc);
        }
    }
}

// Fetch one row
py::object Cursor::fetchone() {
    checkClosed();
    
    if (_numCols == 0) {
        return py::none();
    }
    
    SQLRETURN ret = SQLFetch_ptr(_hstmt->handle());
    
    if (ret == SQL_NO_DATA) {
        _resultSetEmpty = true;
        return py::none();
    }
    
    if (!SQL_SUCCEEDED(ret)) {
        throw py::value_error("Failed to fetch row");
    }
    
    // Create row tuple
    py::tuple row(_numCols);
    
    for (SQLSMALLINT i = 0; i < _numCols; ++i) {
        SQLLEN indicator = 0;
        
        // Determine data type and fetch accordingly
        switch (_columnTypes[i]) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR: {
                // Get data size first
                SQLLEN dataSize = 0;
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_CHAR, nullptr, 0, &dataSize);
                
                if (dataSize == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    // Allocate buffer and fetch
                    std::string buffer(dataSize + 1, '\0');
                    SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_CHAR, 
                               &buffer[0], buffer.size(), &indicator);
                    
                    if (indicator == SQL_NULL_DATA) {
                        row[i] = py::none();
                    } else {
                        buffer.resize(indicator);
                        row[i] = py::str(buffer);
                    }
                }
                break;
            }
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR: {
                // Get data size first
                SQLLEN dataSize = 0;
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_WCHAR, nullptr, 0, &dataSize);
                
                if (dataSize == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    // Allocate buffer and fetch
                    std::wstring buffer(dataSize / sizeof(SQLWCHAR) + 1, L'\0');
                    SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_WCHAR, 
                               &buffer[0], buffer.size() * sizeof(SQLWCHAR), &indicator);
                    
                    if (indicator == SQL_NULL_DATA) {
                        row[i] = py::none();
                    } else {
                        buffer.resize(indicator / sizeof(SQLWCHAR));
                        #ifdef _WIN32
                        row[i] = py::str(buffer);
                        #else
                        row[i] = py::str(WideToUTF8(buffer));
                        #endif
                    }
                }
                break;
            }
            case SQL_INTEGER:
            case SQL_SMALLINT:
            case SQL_TINYINT: {
                int value = 0;
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_LONG, &value, sizeof(value), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    row[i] = py::int_(value);
                }
                break;
            }
            case SQL_BIGINT: {
                long long value = 0;
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_SBIGINT, &value, sizeof(value), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    row[i] = py::int_(value);
                }
                break;
            }
            case SQL_REAL:
            case SQL_FLOAT:
            case SQL_DOUBLE: {
                double value = 0.0;
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_DOUBLE, &value, sizeof(value), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    row[i] = py::float_(value);
                }
                break;
            }
            case SQL_BIT: {
                unsigned char value = 0;
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_BIT, &value, sizeof(value), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    row[i] = py::bool_(value != 0);
                }
                break;
            }
            case SQL_DATE:
            case SQL_TYPE_DATE: {
                SQL_DATE_STRUCT date = {0};
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_DATE, &date, sizeof(date), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    // Import datetime module
                    py::module datetime = py::module::import("datetime");
                    row[i] = datetime.attr("date")(date.year, date.month, date.day);
                }
                break;
            }
            case SQL_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP: {
                SQL_TIMESTAMP_STRUCT ts = {0};
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_TIMESTAMP, &ts, sizeof(ts), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    // Import datetime module
                    py::module datetime = py::module::import("datetime");
                    row[i] = datetime.attr("datetime")(ts.year, ts.month, ts.day, 
                                                      ts.hour, ts.minute, ts.second, 
                                                      ts.fraction / 1000);
                }
                break;
            }
            case SQL_TIME:
            case SQL_TYPE_TIME: {
                SQL_TIME_STRUCT time = {0};
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_TIME, &time, sizeof(time), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    // Import datetime module
                    py::module datetime = py::module::import("datetime");
                    row[i] = datetime.attr("time")(time.hour, time.minute, time.second);
                }
                break;
            }
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY: {
                // Get data size first
                SQLLEN dataSize = 0;
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_BINARY, nullptr, 0, &dataSize);
                
                if (dataSize == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else {
                    // Allocate buffer and fetch
                    std::vector<unsigned char> buffer(dataSize);
                    SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_BINARY, 
                               buffer.data(), buffer.size(), &indicator);
                    
                    if (indicator == SQL_NULL_DATA) {
                        row[i] = py::none();
                    } else {
                        buffer.resize(indicator);
                        row[i] = py::bytes((char*)buffer.data(), buffer.size());
                    }
                }
                break;
            }
            default: {
                // For all other types, convert to string
                char buffer[1024] = {0};
                SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row[i] = py::none();
                } else if (indicator > sizeof(buffer) - 1) {
                    // Data truncated, need larger buffer
                    std::string largeBuf(indicator + 1, '\0');
                    SQLGetData_ptr(_hstmt->handle(), i + 1, SQL_C_CHAR, 
                               &largeBuf[0], largeBuf.size(), &indicator);
                    largeBuf.resize(indicator);
                    row[i] = py::str(largeBuf);
                } else {
                    buffer[indicator] = '\0';
                    row[i] = py::str(buffer);
                }
                break;
            }
        }
    }
    
    return row;
}

// Fetch many rows
py::list Cursor::fetchmany(int size) {
    checkClosed();
    
    if (_numCols == 0 || _resultSetEmpty) {
        return py::list();
    }
    
    // If size is not provided, use arraysize
    if (size <= 0) {
        size = _arraysize;
    }
    
    py::list rows;
    for (int i = 0; i < size; ++i) {
        py::object row = fetchone();
        if (row.is_none()) {
            break; // No more rows
        }
        rows.append(row);
    }
    
    return rows;
}

// Fetch all rows
py::list Cursor::fetchall() {
    checkClosed();
    
    if (_numCols == 0 || _resultSetEmpty) {
        return py::list();
    }
    
    py::list rows;
    while (true) {
        py::object row = fetchone();
        if (row.is_none()) {
            break; // No more rows
        }
        rows.append(row);
    }
    
    return rows;
}

// Check if there's another result set
bool Cursor::nextset() {
    checkClosed();
    
    SQLRETURN ret = SQLMoreResults_ptr(_hstmt->handle());
    
    if (ret == SQL_NO_DATA) {
        return false;
    }
    
    if (!SQL_SUCCEEDED(ret)) {
        throw py::value_error("Failed to check for more results");
    }
    
    // Update column information for the new result set
    _resultSetEmpty = false;
    SQLNumResultCols_ptr(_hstmt->handle(), &_numCols);
    
    if (_numCols > 0) {
        prepareDescription();
    }
    
    return true;
}

// Reset the cursor
void Cursor::reset() {
    if (_closed) {
        return;
    }
    
    if (_hstmt) {
        _hstmt->free();
        _hstmt = nullptr;
    }
    
    allocateStatementHandle();
    
    _resultSetEmpty = false;
    _rowcount = -1;
    _description = py::list();
    _numCols = 0;
    _lastExecutedStmt = L"";
    _isPrepared = false;
}

// Get row count
SQLLEN Cursor::getRowCount() const {
    return _rowcount;
}

// Get description
py::list Cursor::getDescription() const {
    return _description;
}

// Check if cursor is closed
bool Cursor::isClosed() const {
    return _closed;
}

// Set input sizes (no-op for now)
void Cursor::setinputsizes(const py::list& sizes) {
    // This is a no-op in ODBC, but included for API compliance
}

// Set output size (no-op for now)
void Cursor::setoutputsize(int size, int column) {
    // This is a no-op in ODBC, but included for API compliance
    _bufferLength = size;
}

// CursorHandle implementation
CursorHandle::CursorHandle(std::shared_ptr<ConnectionHandle> connection) {
    _cursor = std::make_shared<Cursor>(connection);
}

CursorHandle::~CursorHandle() {
    try {
        if (!_cursor->isClosed()) {
            _cursor->close();
        }
    } catch (...) {
        // Ignore exceptions in destructor
    }
}

void CursorHandle::close() {
    _cursor->close();
}

void CursorHandle::execute(const std::string& query, const py::object& parameters) {
    // Convert query to wide string
    #ifdef _WIN32
    std::wstring wquery(query.begin(), query.end());
    #else
    std::wstring wquery = UTF8ToWide(query);
    #endif
    
    // Handle parameters based on type
    if (parameters.is_none()) {
        // No parameters
        _cursor->execute(wquery);
    } else if (py::isinstance<py::list>(parameters) || py::isinstance<py::tuple>(parameters)) {
        // List or tuple of parameters
        _cursor->execute(wquery, parameters.cast<py::list>());
    } else {
        // Single parameter, wrap in list
        py::list params;
        params.append(parameters);
        _cursor->execute(wquery, params);
    }
}

void CursorHandle::executemany(const std::string& query, const py::list& seq_of_parameters) {
    // Convert query to wide string
    #ifdef _WIN32
    std::wstring wquery(query.begin(), query.end());
    #else
    std::wstring wquery = UTF8ToWide(query);
    #endif
    
    _cursor->executemany(wquery, seq_of_parameters);
}

py::object CursorHandle::fetchone() {
    return _cursor->fetchone();
}

py::list CursorHandle::fetchmany(int size) {
    return _cursor->fetchmany(size);
}

py::list CursorHandle::fetchall() {
    return _cursor->fetchall();
}

bool CursorHandle::nextset() {
    return _cursor->nextset();
}

void CursorHandle::setinputsizes(const py::list& sizes) {
    _cursor->setinputsizes(sizes);
}

void CursorHandle::setoutputsize(int size, int column) {
    _cursor->setoutputsize(size, column);
}

py::list CursorHandle::getDescription() const {
    return _cursor->getDescription();
}

SQLLEN CursorHandle::getRowCount() const {
    return _cursor->getRowCount();
}

void CursorHandle::setArraySize(int size) {
    if (size <= 0) {
        throw py::value_error("Array size must be positive");
    }
}

int CursorHandle::getArraySize() const {
    return 1; // Default array size
}

bool CursorHandle::isClosed() const {
    return _cursor->isClosed();
}
