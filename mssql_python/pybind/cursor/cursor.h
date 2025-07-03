#pragma once
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "../ddbc_bindings.h"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>

// Forward declaration for ConnectionHandle
class ConnectionHandle;

// Class to represent a database cursor in C++
class Cursor {
public:
    Cursor(std::shared_ptr<ConnectionHandle> connection);
    ~Cursor();

    // Close the cursor and free resources
    void close();

    // Execute a query with optional parameters
    void execute(const std::wstring& query, const py::list& parameters = py::list());
    
    // Execute many with a sequence of parameters
    void executemany(const std::wstring& query, const py::list& seq_of_parameters);
    
    // Fetch methods
    py::object fetchone();
    py::list fetchmany(int size = 0);
    py::list fetchall();
    
    // Check if the cursor is closed
    bool isClosed() const;
    
    // Get number of rows affected by last operation
    SQLLEN getRowCount() const;
    
    // Reset the cursor for reuse
    void reset();

    // Get the result set description
    py::list getDescription() const;

    // Set input sizes for parameters
    void setinputsizes(const py::list& sizes);
    
    // Set output size for columns
    void setoutputsize(int size, int column = -1);

    // Check if there's another result set
    bool nextset();

private:
    // Initialize cursor
    void initialize();
    
    // Allocate statement handle
    void allocateStatementHandle();
    
    // Check if cursor is closed and raise exception if it is
    void checkClosed() const;
    
    // Prepare column descriptions after execute
    void prepareDescription();
    
    // Bind parameters for execution
    void bindParameters(const py::list& parameters);
    
    // Get column information
    std::string getColumnName(int column) const;
    int getColumnType(int column) const;
    int getColumnSize(int column) const;
    int getColumnPrecision(int column) const;
    int getColumnScale(int column) const;
    bool isColumnNullable(int column) const;
    
    std::shared_ptr<ConnectionHandle> _connection;
    SqlHandlePtr _hstmt;
    bool _closed;
    bool _resultSetEmpty;
    std::wstring _lastExecutedStmt;
    bool _isPrepared;
    SQLLEN _rowcount;
    int _arraysize;
    int _bufferLength;
    py::list _description;
    SQLSMALLINT _numCols;
    
    // Column metadata storage
    std::vector<std::string> _columnNames;
    std::vector<SQLSMALLINT> _columnTypes;
    std::vector<SQLULEN> _columnSizes;
    std::vector<SQLSMALLINT> _columnPrecisions;
    std::vector<SQLSMALLINT> _columnScales;
    std::vector<bool> _columnNullables;
};

// Python wrapper for the Cursor class
class CursorHandle {
public:
    CursorHandle(std::shared_ptr<ConnectionHandle> connection);
    ~CursorHandle();
    
    // Main DB-API 2.0 cursor methods
    void close();
    void execute(const std::string& query, const py::object& parameters = py::none());
    void executemany(const std::string& query, const py::list& seq_of_parameters);
    py::object fetchone();
    py::list fetchmany(int size = 0);
    py::list fetchall();
    bool nextset();
    void setinputsizes(const py::list& sizes);
    void setoutputsize(int size, int column = -1);
    
    // Properties
    py::list getDescription() const;
    SQLLEN getRowCount() const;
    void setArraySize(int size);
    int getArraySize() const;
    bool isClosed() const;
    
private:
    std::shared_ptr<Cursor> _cursor;
};
