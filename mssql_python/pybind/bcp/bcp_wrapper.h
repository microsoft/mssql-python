// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "ddbc_bindings.h" // For SQLRETURN and other ODBC types/macros
#include "../connection/connection.h" // For the Connection class
#include <optional> // For std::optional

#define BCP_OUT_CHARACTER_MODE       0x00000001  // Corresponds to -c option: character data format
#define BCP_OUT_NATIVE_MODE          0x00000002  // Corresponds to -n option: native data format
#define BCP_OUT_WIDE_CHARACTER_MODE  0x00000080  // Corresponds to -w option: Unicode character format
#define BCP_OUT_WIDE_NATIVE_MODE     0x00000100  // Corresponds to -N option: Unicode native format

class BCPWrapper {
public:
    // Constructor: Requires a reference to an active Connection object.
    // The BCPWrapper does not take ownership of the Connection object.
    BCPWrapper(Connection& conn); // Changed to Connection&

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
    SQLRETURN set_bulk_mode(const std::wstring& mode, 
                            const std::optional<std::vector<unsigned char>>& field_terminator = std::nullopt,
                            const std::optional<std::vector<unsigned char>>& row_terminator = std::nullopt
                        );

    // Reads column format information from a BCP format file.
    // Maps to ODBC bcp_readfmt.
    SQLRETURN read_format_file(const std::wstring& file_path);

    // Specifies the total number of columns in the user data file.
    // Maps to ODBC bcp_columns.
    SQLRETURN define_columns(int num_cols);

    // Defines the format of data in the data file for a specific column.
    // Maps to ODBC bcp_colfmt.
    SQLRETURN define_column_format(int file_col_idx,         // User data file column number (1-based), maps to idxUserDataCol
                                   int user_data_type,       // Data type in user file (e.g., SQLCHARACTER), maps to eUserDataType
                                   int indicator_length,     // Length of prefix/indicator (0, 1, 2, 4, 8, or SQL_VARLEN_DATA), maps to cbIndicator
                                   long long user_data_length, // Max length of data in user file (bytes), maps to cbUserData
                                   const std::optional<std::vector<unsigned char>>& terminator_bytes, // Terminator sequence
                                   int server_col_idx        // Server table column number (1-based), maps to idxServerCol
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
    Connection& _conn; // Changed to Connection&
    bool _bcp_initialized; // Flag to track if bcp_init has been called successfully
    bool _bcp_finished;    // Flag to track if bcp_finish (or bcp_done) has been called

    // Helper to get the HDBC handle from the Connection object
    // SQLHDBC get_hdbc() const; // Implementation would be in .cpp
};