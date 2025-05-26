// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#ifndef BCP_WRAPPER_H
#define BCP_WRAPPER_H

#include <string>
#include <vector>
#include <optional>
#include "ddbc_bindings.h" // For SQLRETURN and other ODBC types/macros
#include "../connection/connection.h" // For the Connection class

class BCPWrapper {
public:
    // Constructor: Requires a reference to an active Connection object.
    // The BCPWrapper does not take ownership of the Connection object.
    BCPWrapper(std::shared_ptr<Connection> conn);

    // Destructor: Ensures BCP operations are properly terminated if active.
    ~BCPWrapper();

    // Initializes a BCP operation for a specific table, data file, error file, and direction.
    // Maps to ODBC bcp_init.
    SQLRETURN bcp_initialize_operation(const std::wstring& table,
                                       const std::wstring& data_file,
                                       const std::wstring& error_file,
                                       const std::wstring& direction);

    // Sets various BCP control options using an integer value.
    // Maps to ODBC bcp_control.
    SQLRETURN bcp_control(const std::wstring& property_name, int value);

    // Sets various BCP control options using a string value.
    // Maps to ODBC bcp_control.
    SQLRETURN bcp_control(const std::wstring& property_name, const std::wstring& value);

    // Sets the bulk copy mode (e.g., "native", "char").
    // This might internally call bcp_control or affect column format definitions.
    SQLRETURN set_bulk_mode(const std::wstring& mode);

    // Reads column format information from a BCP format file.
    // Maps to ODBC bcp_readfmt.
    SQLRETURN read_format_file(const std::wstring& file_path);

    // Specifies the total number of columns in the user data file.
    // Maps to ODBC bcp_columns.
    SQLRETURN define_columns(int num_cols);

    // Defines the format of data in the data file for a specific column.
    // Maps to ODBC bcp_colfmt.
    SQLRETURN define_column_format(int col_num, // 1-based column number in the definition sequence
                                   int prefix_len,
                                   int data_len,
                                   const std::wstring& terminator,
                                   int server_col_type, // Intended file data type (e.g. SQLCHARACTER)
                                   const std::optional<std::wstring>& col_name, // Optional column name
                                   int server_col,  // 1-based column number in the server table
                                   int file_col     // 1-based column number in the host file
                                   );

    // Executes the BCP operation, transferring data.
    // Maps to ODBC bcp_exec.
    SQLRETURN exec_bcp();

    // Completes the BCP operation and releases associated resources.
    // Maps to ODBC bcp_done.
    SQLRETURN finish();
    
    // Optional method to explicitly close/cleanup BCP resources.
    // May call finish() if BCP operation is still active.
    SQLRETURN close();

private:
    std::shared_ptr<Connection> _conn; // Reference to the database connection object
    bool _bcp_initialized; // Flag to track if bcp_init has been called successfully
    bool _bcp_finished;    // Flag to track if bcp_finish (or bcp_done) has been called

    // Helper to get the HDBC handle from the Connection object
    // SQLHDBC get_hdbc() const; // Implementation would be in .cpp
};

#endif // BCP_WRAPPER_H