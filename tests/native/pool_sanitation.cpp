// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection/connection.h"
#include "connection/connection_pool.h"
#include "logger_bridge.hpp"
#include <pybind11/embed.h>
#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>
#include <unordered_map>

SQLSMALLINT SQLNumResultCols_wrap(SqlHandlePtr statementHandle, py::handle messages);
SQLRETURN FetchOne_wrap(SqlHandlePtr statementHandle, py::list& row,
                       const std::string& charEncoding, const std::string& wcharEncoding,
                       int charCtype, py::handle messages);
py::list SQLGetAllDiagRecords(SqlHandlePtr handle);

namespace {
struct Handle {
    SQLSMALLINT type;
    SQLHANDLE parent;
    bool autocommit = true;
    bool transaction = false;
    bool pendingWork = false;
    bool resetPending = false;
    SQLULEN loginTimeout = 0;
};

std::unordered_map<SQLHANDLE, std::unique_ptr<Handle>> handles;
std::vector<std::string> calls;
std::string failNext;
int commits = 0;
int logins = 0;
std::vector<SQLULEN> loginTimeouts;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

SQLRETURN record(const std::string& operation) {
    calls.push_back(operation);
    if (operation == failNext) {
        failNext.clear();
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

SQLRETURN SQL_API allocate(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* out) {
    auto ret = record(type == SQL_HANDLE_STMT ? "allocate_statement" : "allocate");
    if (!SQL_SUCCEEDED(ret)) {
        return ret;
    }
    auto handle = std::make_unique<Handle>(Handle{type, parent});
    *out = handle.get();
    handles.emplace(*out, std::move(handle));
    return SQL_SUCCESS;
}

SQLRETURN SQL_API freeHandle(SQLSMALLINT, SQLHANDLE handle) {
    auto ret = record("free");
    if (SQL_SUCCEEDED(ret)) {
        handles.erase(handle);
    }
    return ret;
}

SQLRETURN SQL_API setEnv(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER) {
    return SQL_SUCCESS;
}

SQLRETURN SQL_API login(SQLHDBC dbc, SQLHWND, SQLWCHAR*, SQLSMALLINT,
                       SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*, SQLUSMALLINT) {
    ++logins;
    loginTimeouts.push_back(handles.at(dbc)->loginTimeout);
    return record("login");
}

SQLRETURN SQL_API setAttr(SQLHDBC dbc, SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER length) {
    std::string operation = "attribute";
    if (attribute == SQL_ATTR_AUTOCOMMIT) {
        operation = value ? "on" : "off";
    } else if (attribute == SQL_ATTR_RESET_CONNECTION) {
        operation = "reset";
    } else if (attribute == SQL_ATTR_TXN_ISOLATION) {
        operation = "isolation";
    }
    auto ret = record(operation);
    if (!SQL_SUCCEEDED(ret)) {
        return ret;
    }
    auto& handle = *handles.at(dbc);
    if (attribute == SQL_ATTR_AUTOCOMMIT) {
        if (value) {
            if (handle.pendingWork) {
                ++commits;
            }
            handle.transaction = handle.pendingWork = false;
        }
        handle.autocommit = value != nullptr;
    } else if (attribute == SQL_ATTR_RESET_CONNECTION) {
        handle.resetPending = true;
    } else if (attribute == SQL_ATTR_LOGIN_TIMEOUT && length == SQL_IS_INTEGER) {
        handle.loginTimeout = reinterpret_cast<SQLULEN>(value);
    }
    // A reset is deferred until execution. It must not establish clean proof.
    return SQL_SUCCESS;
}

SQLRETURN SQL_API getAttr(SQLHDBC dbc, SQLINTEGER attribute, SQLPOINTER value,
                         SQLINTEGER, SQLINTEGER*) {
    auto ret = record(attribute == SQL_ATTR_AUTOCOMMIT ? "get" : "alive");
    if (SQL_SUCCEEDED(ret)) {
        *static_cast<SQLINTEGER*>(value) = attribute == SQL_ATTR_AUTOCOMMIT
            ? (handles.at(dbc)->autocommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF)
            : SQL_CD_FALSE;
    }
    return ret;
}

SQLRETURN SQL_API endTran(SQLSMALLINT, SQLHANDLE dbc, SQLSMALLINT completion) {
    auto ret = record(completion == SQL_ROLLBACK ? "rollback" : "commit");
    if (SQL_SUCCEEDED(ret) && !handles.at(dbc)->autocommit) {
        auto& handle = *handles.at(dbc);
        if (completion == SQL_COMMIT && handle.pendingWork) {
            ++commits;
        }
        handle.pendingWork = false;
        // Model the empty manual-mode transaction that motivated PR #777.
        handle.transaction = true;
    }
    return ret;
}

SQLRETURN SQL_API executeDirect(SQLHSTMT statement, SQLWCHAR*, SQLINTEGER) {
    auto& handle = *handles.at(handles.at(statement)->parent);
    if (handle.resetPending) {
        handle.transaction = handle.pendingWork = false;
        handle.resetPending = false;
    }
    auto ret = record("rollback_batch");
    if (SQL_SUCCEEDED(ret)) {
        handle.transaction = handle.pendingWork = false;
    }
    return ret;
}

SQLRETURN SQL_API disconnect(SQLHDBC dbc) {
    record("disconnect");
    if (handles.at(dbc)->pendingWork) {
        return SQL_ERROR;
    }
    handles.at(dbc)->transaction = handles.at(dbc)->pendingWork = false;
    for (auto it = handles.begin(); it != handles.end();) {
        if (it->second->parent == dbc) {
            it = handles.erase(it);
        } else {
            ++it;
        }
    }
    return SQL_SUCCESS;
}

SQLRETURN SQL_API getInfo(SQLHDBC dbc, SQLUSMALLINT, SQLPOINTER value,
                         SQLSMALLINT, SQLSMALLINT* length) {
    *length = sizeof(dbc);
    if (value) {
        std::memcpy(value, &dbc, sizeof(dbc));
    }
    return SQL_SUCCESS;
}

SQLRETURN SQL_API diagnostic(SQLSMALLINT, SQLHANDLE, SQLSMALLINT recordNumber,
                            SQLWCHAR* state, SQLINTEGER* native, SQLWCHAR* message,
                            SQLSMALLINT, SQLSMALLINT* length) {
    if (recordNumber != 1) {
        return SQL_NO_DATA;
    }
    const SQLWCHAR sqlstate[] = {'H', 'Y', '0', '0', '0', 0};
    const SQLWCHAR text[] = {'f', 'a', 'i', 'l', 'e', 'd', 0};
    std::copy(std::begin(sqlstate), std::end(sqlstate), state);
    std::copy(std::begin(text), std::end(text), message);
    *native = 0;
    *length = 6;
    return SQL_SUCCESS;
}

void expectCalls(std::initializer_list<const char*> expected) {
    std::vector<std::string> wanted(expected.begin(), expected.end());
    if (calls != wanted) {
        std::string actual;
        for (const auto& call : calls) {
            actual += call + " ";
        }
        throw std::runtime_error("Unexpected ODBC calls: " + actual);
    }
}

SQLRETURN SQL_API freeStatement(SQLHSTMT, SQLUSMALLINT option) {
    require(option == SQL_UNBIND, "Unexpected statement cleanup");
    return record("statement_unbind");
}

struct DiagnosticRecord {
    std::u16string state;
    std::u16string message;
    SQLRETURN result = SQL_SUCCESS;
};
std::vector<DiagnosticRecord> fetchRecords;
SQLRETURN fetchResult = SQL_SUCCESS_WITH_INFO;
SQLRETURN numberResult = SQL_SUCCESS;
SQLINTEGER diagnosticCount = 0;
bool writeDiagnosticCount = true;
SQLRETURN stateResult = SQL_SUCCESS;

SQLRETURN SQL_API fetchDiagnostic(SQLSMALLINT type, SQLHANDLE, SQLSMALLINT number,
                                 SQLWCHAR* state, SQLINTEGER* native, SQLWCHAR* message,
                                 SQLSMALLINT capacity, SQLSMALLINT* length) {
    require(type == SQL_HANDLE_STMT && number > 0, "Invalid record lookup");
    record("diag_record");
    if (static_cast<size_t>(number) > fetchRecords.size()) return SQL_NO_DATA;
    const auto& item = fetchRecords[number - 1];
    require(item.state.size() == 5, "Invalid test SQLSTATE");
    std::copy(item.state.begin(), item.state.end(), state);
    state[5] = 0;
    *native = 42;
    const auto size = std::min(item.message.size(), static_cast<size_t>(capacity - 1));
    std::copy_n(item.message.begin(), size, message);
    message[size] = 0;
    *length = static_cast<SQLSMALLINT>(item.message.size());
    return item.result;
}

SQLRETURN SQL_API fetchDiagnosticField(SQLSMALLINT type, SQLHANDLE, SQLSMALLINT number,
                                      SQLSMALLINT identifier, SQLPOINTER value,
                                      SQLSMALLINT capacity, SQLSMALLINT* length) {
    require(type == SQL_HANDLE_STMT, "Wrong diagnostic handle type");
    if (identifier == SQL_DIAG_NUMBER) {
        record("diag_number");
        require(number == 0 && capacity == 0 && length == nullptr,
                "SQL_DIAG_NUMBER must read the numeric header field");
        if (writeDiagnosticCount) *static_cast<SQLINTEGER*>(value) = diagnosticCount;
        return numberResult;
    }
    record("diag_state");
    require(identifier == SQL_DIAG_SQLSTATE && number > 0, "Unexpected diagnostic field");
    if (stateResult != SQL_SUCCESS) return stateResult;
    if (static_cast<size_t>(number) > fetchRecords.size()) return SQL_NO_DATA;
    const auto& state = fetchRecords[number - 1].state;
    require(capacity >= static_cast<SQLSMALLINT>(6 * sizeof(SQLWCHAR)), "SQLSTATE buffer too small");
    std::copy(state.begin(), state.end(), static_cast<SQLWCHAR*>(value));
    static_cast<SQLWCHAR*>(value)[5] = 0;
    return SQL_SUCCESS;
}

SQLRETURN SQL_API fetchColumnCount(SQLHSTMT, SQLSMALLINT* count) {
    record("column_count");
    *count = 0;
    return fetchResult;
}

SQLRETURN SQL_API fetchNullLob(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN,
                               SQLLEN* length) {
    record("get_lob");
    *length = SQL_NULL_DATA;
    return fetchResult;
}

SQLRETURN SQL_API fetchNoData(SQLHANDLE) {
    record("fetch_no_data");
    return SQL_NO_DATA;
}

struct FetchDiagnosticScope {
    SQLGetDiagRecFunc oldRecord = SQLGetDiagRec_ptr;
    SQLGetDiagFieldFunc oldField = SQLGetDiagField_ptr;
    SQLNumResultColsFunc oldCount = SQLNumResultCols_ptr;
    SQLGetDataFunc oldData = SQLGetData_ptr;
    SQLFetchFunc oldFetch = SQLFetch_ptr;
    SQLFreeStmtFunc oldFreeStmt = SQLFreeStmt_ptr;

    FetchDiagnosticScope() {
        fetchRecords.clear();
        fetchResult = SQL_SUCCESS_WITH_INFO;
        numberResult = stateResult = SQL_SUCCESS;
        diagnosticCount = 0;
        writeDiagnosticCount = true;
        SQLGetDiagRec_ptr = fetchDiagnostic;
        SQLGetDiagField_ptr = fetchDiagnosticField;
        SQLNumResultCols_ptr = fetchColumnCount;
        SQLGetData_ptr = fetchNullLob;
        SQLFetch_ptr = fetchNoData;
        SQLFreeStmt_ptr = freeStatement;
    }
    ~FetchDiagnosticScope() {
        SQLGetDiagRec_ptr = oldRecord;
        SQLGetDiagField_ptr = oldField;
        SQLNumResultCols_ptr = oldCount;
        SQLGetData_ptr = oldData;
        SQLFetch_ptr = oldFetch;
        SQLFreeStmt_ptr = oldFreeStmt;
    }
};

void expectMessage(const py::list& messages, size_t index, const std::string& state,
                   const std::string& text) {
    auto message = messages[index].cast<py::tuple>();
    require(message[0].cast<std::string>() == "[" + state + "] (42)" &&
            message[1].cast<std::string>() == text, "Diagnostic message changed");
}

void expectFailure(const std::function<void()>& action) {
    try {
        action();
    } catch (const std::runtime_error&) {
        return;
    }
    throw std::runtime_error("Expected native failure");
}

std::unique_ptr<ConnectionHandle> acquire(bool pooled = true,
                                          const py::dict& attrs = py::dict()) {
    return std::make_unique<ConnectionHandle>(u"native pool test", pooled, attrs);
}

void checkParked() {
    require(commits == 0, "Cleanup committed pending work");
    for (const auto& item : handles) {
        if (item.second->type == SQL_HANDLE_DBC) {
            require(item.second->autocommit && !item.second->transaction,
                    "Pooled DBC retained a transaction or manual mode");
        }
    }
}

void warm() {
    auto connection = acquire();
    connection->close();
    checkParked();
}

void startWork(const SqlHandlePtr& statement) {
    auto dbc = handles.at(statement->get())->parent;
    handles.at(dbc)->transaction = handles.at(dbc)->pendingWork = true;
}

void resetScenario() {
    failNext.clear();
    auto& manager = ConnectionPoolManager::getInstance();
    manager.closePools();
    manager.configure(1, 30);
    manager.setAccepting(true);
    calls.clear();
    commits = logins = 0;
    loginTimeouts.clear();
}
}  // namespace

int main() {
    py::scoped_interpreter interpreter;
    mssql_python::logging::LoggerBridge::updateLevel(1000);
    SQLAllocHandle_ptr = allocate;
    SQLFreeHandle_ptr = freeHandle;
    SQLSetEnvAttr_ptr = setEnv;
    SQLDriverConnect_ptr = login;
    SQLSetConnectAttr_ptr = setAttr;
    SQLGetConnectAttr_ptr = getAttr;
    SQLExecDirect_ptr = executeDirect;
    SQLEndTran_ptr = endTran;
    SQLDisconnect_ptr = disconnect;
    SQLGetInfo_ptr = getInfo;
    SQLGetDiagRec_ptr = diagnostic;

    int passed = 0;
    auto run = [&](const char* name, const std::function<void()>& test) {
        resetScenario();
        test();
        ++passed;
        std::cout << "PASS " << name << '\n';
    };
    try {
        for (SQLRETURN origin : {SQLRETURN(SQL_SUCCESS_WITH_INFO), SQLRETURN(SQL_NO_DATA)}) {
            run("successful zero-record header avoids enumeration without clearing messages", [origin] {
                auto connection = acquire();
                auto statement = connection->allocStatementHandle();
                FetchDiagnosticScope diagnostics;
                fetchResult = origin;
                py::list messages;
                messages.append(py::make_tuple("existing", "preserved"));
                calls.clear();
                require(SQLNumResultCols_wrap(statement, messages) == 0, "Column count changed");
                expectCalls({"column_count", "diag_number"});
                require(messages.size() == 1, "Zero-record gate cleared existing messages");
                statement.reset();
                connection->close();
            });
        }
        for (int scenario = 0; scenario < 9; ++scenario) {
            run("missing unsupported info unknown and nonzero headers retain enumeration", [scenario] {
                auto connection = acquire();
                auto statement = connection->allocStatementHandle();
                FetchDiagnosticScope diagnostics;
                fetchRecords = {{u"01000", u"PRINT message"},
                                {u"01004", u"visible truncation", SQL_SUCCESS_WITH_INFO}};
                switch (scenario) {
                    case 0: SQLGetDiagField_ptr = nullptr; break;
                    case 1: numberResult = SQL_ERROR; break;
                    case 2: numberResult = SQL_SUCCESS_WITH_INFO; break;
                    case 3: diagnosticCount = -1; break;
                    case 4: writeDiagnosticCount = false; break;
                    case 5: diagnosticCount = 2; break;
                    case 6: numberResult = SQL_NO_DATA; break;
                    case 7: numberResult = SQL_INVALID_HANDLE; break;
                    case 8: diagnosticCount = 1; break;  // Never cap enumeration at the header.
                }
                py::list messages;
                calls.clear();
                SQLNumResultCols_wrap(statement, messages);
                if (scenario == 0) {
                    expectCalls({"column_count", "diag_record", "diag_record", "diag_record"});
                } else {
                    expectCalls({"column_count", "diag_number", "diag_record", "diag_record",
                                 "diag_record"});
                }
                require(messages.size() == 2, "Fallback lost warning or PRINT records");
                expectMessage(messages, 0, "01000", "PRINT message");
                expectMessage(messages, 1, "01004", "visible truncation");
                statement.reset();
                connection->close();
            });
        }
        for (bool empty : {false, true}) {
            run("actual SQLFetch NO_DATA preserves return and any messages", [empty] {
                auto connection = acquire();
                auto statement = connection->allocStatementHandle();
                FetchDiagnosticScope diagnostics;
                if (!empty) {
                    diagnosticCount = 1;
                    fetchRecords = {{u"01000", u"final PRINT"}};
                }
                py::list rows, messages;
                calls.clear();
                auto ret = FetchOne_wrap(statement, rows, "utf-16le", "utf-16le",
                                         SQL_C_WCHAR, messages);
                require(ret == SQL_NO_DATA && rows.empty(), "NO_DATA result changed");
                if (empty) {
                    expectCalls({"statement_unbind", "fetch_no_data", "diag_number"});
                    require(messages.empty(), "Empty diagnostics created a message");
                } else {
                    expectCalls({"statement_unbind", "fetch_no_data", "diag_number",
                                 "diag_record", "diag_record"});
                    require(messages.size() == 1, "NO_DATA lost its PRINT record");
                    expectMessage(messages, 0, "01000", "final PRINT");
                }
                statement.reset();
                connection->close();
            });
        }
        for (int scenario = 0; scenario < 4; ++scenario) {
            run("LOB continuation filters only internal truncation and retains unrelated warnings", [scenario] {
                auto connection = acquire();
                auto statement = connection->allocStatementHandle();
                FetchDiagnosticScope diagnostics;
                diagnosticCount = 3;
                fetchRecords = {{u"01004", u"internal truncation"},
                                {u"01000", u"PRINT message"},
                                {u"01S02", u"option changed", SQL_SUCCESS_WITH_INFO}};
                if (scenario == 1) SQLGetDiagField_ptr = nullptr;
                if (scenario == 2) stateResult = SQL_ERROR;
                if (scenario == 3) stateResult = SQL_SUCCESS_WITH_INFO;
                py::list messages;
                calls.clear();
                auto value = FetchLobColumnData(statement->get(), 1, SQL_C_WCHAR,
                                                true, false, "utf-16le", messages);
                require(value.is_none() && messages.size() == 2,
                        "LOB continuation lost warnings or exposed internal truncation");
                expectMessage(messages, 0, "01000", "PRINT message");
                expectMessage(messages, 1, "01S02", "option changed");
                require(std::count(calls.begin(), calls.end(), "diag_number") ==
                            (scenario == 1 ? 0 : 1),
                        "LOB header probe count changed");
                statement.reset();
                connection->close();
            });
        }
        run("no probe for success absent messages or explicit diagnostic enumeration", [] {
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            FetchDiagnosticScope diagnostics;
            py::list messages;
            fetchResult = SQL_SUCCESS;
            calls.clear();
            SQLNumResultCols_wrap(statement, messages);
            expectCalls({"column_count"});
            fetchResult = SQL_SUCCESS_WITH_INFO;
            calls.clear();
            SQLNumResultCols_wrap(statement, {});
            SQLNumResultCols_wrap(statement, py::none());
            expectCalls({"column_count", "column_count"});
            fetchRecords = {{u"01000", u"explicit diagnostic"}};
            calls.clear();
            messages = SQLGetAllDiagRecords(statement);
            expectCalls({"diag_record", "diag_record"});
            expectMessage(messages, 0, "01000", "explicit diagnostic");
            statement.reset();
            connection->close();
        });
        run("new login is not clean proof; repeated empty leases skip all sanitation", [] {
            auto connection = acquire();
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            for (int i = 0; i < 100; ++i) {
                calls.clear();
                connection = acquire();
                expectCalls({"alive"});
                calls.clear();
                connection->setAutocommit(true);
                expectCalls({});
                calls.clear();
                connection->close(true);
                expectCalls({});
                checkParked();
            }
            require(logins == 1, "Empty leases did not reuse the physical connection");
        });
        run("new login and repeated setters do not establish clean proof", [] {
            auto connection = acquire();
            calls.clear();
            connection->setAutocommit(true);
            connection->setAutocommit(true);
            expectCalls({"on", "on"});
            connection->close();
            connection = acquire();
            calls.clear();
            connection->setAutocommit(true);
            expectCalls({});
            connection->close();
        });
        run("manual mode is never elided; only successful sanitation restores proof", [] {
            warm();
            auto connection = acquire();
            calls.clear();
            connection->setAutocommit(false);
            connection->setAutocommit(false);
            connection->setAutocommit(true);
            connection->setAutocommit(true);
            expectCalls({"off", "off", "on", "on"});
            connection->close();
            connection = acquire();
            calls.clear();
            connection->setAutocommit(true);
            expectCalls({});
            connection->close();
        });
        run("failed setter preserves metadata invalidation and sanitation requirement", [] {
            warm();
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            auto generation = statement->resultMetadata.snapshot().generation;
            statement->resultMetadata.publish(generation, std::make_shared<ResultMetadata>());
            calls.clear();
            failNext = "on";
            expectFailure([&] { connection->setAutocommit(true); });
            expectCalls({"on"});
            auto snapshot = statement->resultMetadata.snapshot();
            require(!snapshot.metadata && snapshot.generation == generation + 1,
                    "Setter did not invalidate metadata before ODBC failure");
            statement.reset();
            calls.clear();
            connection->setAutocommit(true);
            expectCalls({"on"});
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
        });
        run("timeout=30 is applied before login and preserves repeated empty-lease fast path", [] {
            py::dict attrs;
            attrs[py::int_(SQL_ATTR_LOGIN_TIMEOUT)] = py::int_(30);
            auto connection = acquire(true, attrs);
            connection->setAutocommit(true);
            require(loginTimeouts == std::vector<SQLULEN>{30},
                    "Login timeout was not applied as 30 before SQLDriverConnect");
            require(std::count(calls.begin(), calls.end(), "attribute") == 1,
                    "Login timeout was not applied exactly once");
            calls.clear();
            connection->close(true);
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            for (int i = 0; i < 100; ++i) {
                connection = acquire(true, attrs);
                connection->setAutocommit(true);
                calls.clear();
                connection->close(true);
                expectCalls({});
                checkParked();
            }
            require(logins == 1 && loginTimeouts == std::vector<SQLULEN>{30},
                    "Timed leases did not reuse the physical connection with timeout 30");
        });
        run("valid scalar timeout cannot make dirty work clean", [] {
            warm();
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            startWork(statement);
            statement.reset();
            connection->setAttr(SQL_ATTR_LOGIN_TIMEOUT, py::int_(30));
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            checkParked();
            connection = acquire();
            calls.clear();
            connection->close();
            expectCalls({});
        });
        for (bool timeoutFirst : {true, false}) {
            run("scalar timeout never clears an unknown attribute's permanent invalidation", [timeoutFirst] {
                py::dict attrs;
                auto setTimeout = [&] {
                    attrs[py::int_(SQL_ATTR_LOGIN_TIMEOUT)] = py::int_(30);
                };
                if (timeoutFirst) {
                    setTimeout();
                }
                attrs[py::int_(SQL_ATTR_AUTOCOMMIT)] = py::int_(SQL_AUTOCOMMIT_ON);
                if (!timeoutFirst) {
                    setTimeout();
                }
                auto connection = acquire(true, attrs);
                require(loginTimeouts == std::vector<SQLULEN>{30},
                        "Timeout was lost when combined with another attribute");
                connection->close();
                connection = acquire();
                connection->setAttr(SQL_ATTR_LOGIN_TIMEOUT, py::int_(30));
                connection->close();
                connection = acquire();
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            });
        }
        for (const py::object& value : std::vector<py::object>{
                 py::str("30"), py::bytes("30"), py::bool_(true),
                 py::int_(-1), py::int_(uint64_t{1} << 32)}) {
            run("nonscalar and out-of-range login timeouts permanently disable proof", [&value] {
                warm();
                auto connection = acquire();
                connection->setAttr(SQL_ATTR_LOGIN_TIMEOUT, value);
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
                connection = acquire();
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            });
        }
        for (const py::object& value : std::vector<py::object>{
                 py::float_(30.0), py::eval("1 << 100")}) {
            run("unsupported or overflowing timeout conversion fails closed", [&value] {
                warm();
                auto connection = acquire();
                expectFailure([&] { connection->setAttr(SQL_ATTR_LOGIN_TIMEOUT, value); });
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
                connection = acquire();
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            });
        }
        run("failed login-timeout application releases capacity without login", [] {
            py::dict attrs;
            attrs[py::int_(SQL_ATTR_LOGIN_TIMEOUT)] = py::int_(30);
            failNext = "attribute";
            expectFailure([&] { acquire(true, attrs); });
            require(logins == 0, "Connected despite failed login-timeout application");
            auto connection = acquire(true, attrs);
            require(loginTimeouts == std::vector<SQLULEN>{30},
                    "Failed timeout application did not release pool capacity");
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
        });
        run("manual mode rolls back once and parks in autocommit", [] {
            warm();
            auto connection = acquire();
            connection->setAutocommit(false);
            calls.clear();
            connection->close();
            expectCalls({"get", "rollback", "on"});
            checkParked();
        });
        run("statement allocation invalidates even without execution", [] {
            warm();
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            statement.reset();
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            connection = acquire();
            calls.clear();
            connection->close();
            expectCalls({});
        });
        run("failed statement allocation invalidates clean proof", [] {
            warm();
            auto connection = acquire();
            failNext = "allocate_statement";
            expectFailure([&] { connection->allocStatementHandle(); });
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
        });
        run("explicit transaction and retained statement alias across leases", [] {
            warm();
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            startWork(statement);
            auto generation = statement->resultMetadata.snapshot().generation;
            statement->resultMetadata.publish(generation, std::make_shared<ResultMetadata>());
            connection->close();
            auto metadata = statement->resultMetadata.snapshot();
            require(!metadata.metadata && metadata.generation == generation + 1,
                    "Pool sanitation must invalidate result metadata exactly once");
            checkParked();
            connection = acquire();
            calls.clear();
            connection->setAutocommit(true);
            expectCalls({"on"});
            startWork(statement);
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            checkParked();
            statement.reset();
            connection = acquire();
            connection->close();
            connection = acquire();
            calls.clear();
            connection->close();
            expectCalls({});
        });
        run("failed cursor free cannot enable the next lease's fast path", [] {
            warm();
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            startWork(statement);
            failNext = "free";
            require(statement->freeHandle() == SQL_ERROR, "Expected failed free");
            connection->close();
            connection = acquire();
            startWork(statement);
            connection->close();
            checkParked();
        });
        for (const auto* operation : {"get", "reset", "allocate_statement", "rollback_batch", "free"}) {
            run(operation, [operation] {
                auto connection = acquire();
                auto statement = connection->allocStatementHandle();
                startWork(statement);
                statement.reset();
                calls.clear();
                failNext = operation;
                expectFailure([&] { connection->close(); });
                require(std::find(calls.begin(), calls.end(), "disconnect") != calls.end(),
                        "Failed sanitation did not disconnect");
                require(commits == 0, "Failure cleanup committed work");
                connection = acquire();
                require(logins == 2, "Discard did not release capacity / replace DBC");
                connection->close();
                checkParked();
            });
        }
        for (const auto* operation : {"rollback", "on"}) {
            run("manual-mode sanitation failure", [operation] {
                auto connection = acquire();
                connection->setAutocommit(false);
                calls.clear();
                failNext = operation;
                expectFailure([&] { connection->close(); });
                require(std::find(calls.begin(), calls.end(), "disconnect") != calls.end(),
                        "Failed manual-mode sanitation did not disconnect");
                if (std::string(operation) == "rollback") {
                    require(std::find(calls.begin(), calls.end(), "on") == calls.end(),
                            "Enabled autocommit after failed rollback");
                }
                require(commits == 0, "Failure cleanup committed work");
                connection = acquire();
                require(logins == 2, "Discard did not release capacity / replace DBC");
                connection->close();
                checkParked();
            });
        }
        for (auto info : {SQL_DRIVER_HDBC, SQL_DRIVER_HENV, SQL_DRIVER_HSTMT, SQL_DRIVER_HLIB}) {
            run("raw handle exposure permanently disables proof", [info] {
                warm();
                auto connection = acquire();
                connection->getInfo(static_cast<SQLUSMALLINT>(info));
                connection->close();
                connection = acquire();
                calls.clear();
                connection->setAutocommit(true);
                expectCalls({"on"});
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            });
        }
        run("ordinary getinfo invalidates the current lease only", [] {
            warm();
            auto connection = acquire();
            connection->getInfo(SQL_DBMS_NAME);
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            connection = acquire();
            calls.clear();
            connection->close();
            expectCalls({});
        });
        run("arbitrary attrs_before disable proof", [] {
            py::dict attrs;
            attrs[py::int_(SQL_ATTR_AUTOCOMMIT)] = py::int_(SQL_AUTOCOMMIT_OFF);
            auto connection = acquire(true, attrs);
            connection->close();
            connection = acquire();
            calls.clear();
            connection->setAutocommit(true);
            expectCalls({"on"});
            calls.clear();
            connection->close();
            expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
        });
        for (bool fail : {false, true}) {
            run("generic set_attr including failure disables proof", [fail] {
                warm();
                auto connection = acquire();
                if (fail) {
                    failNext = "attribute";
                    expectFailure([&] { connection->setAttr(SQL_ATTR_LOGIN_TIMEOUT, py::int_(30)); });
                } else {
                    connection->setAttr(SQL_ATTR_AUTOCOMMIT, py::int_(SQL_AUTOCOMMIT_OFF));
                }
                calls.clear();
                connection->close();
                if (fail) {
                    expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
                } else {
                    expectCalls({"get", "rollback", "on"});
                }
                connection = acquire();
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            });
        }
        for (const auto* operation : {"commit", "rollback", "on", "off", "get"}) {
            run("failed native operation invalidates clean proof", [operation] {
                warm();
                auto connection = acquire();
                if (std::string(operation) == "on") {
                    // A proven ON->ON is deliberately skipped; inject failure
                    // only after an operation invalidates that proof.
                    auto statement = connection->allocStatementHandle();
                }
                failNext = operation;
                expectFailure([&] {
                    if (std::string(operation) == "commit") {
                        connection->commit();
                    } else if (std::string(operation) == "rollback") {
                        connection->rollback();
                    } else if (std::string(operation) == "get") {
                        connection->getAutocommit();
                    } else {
                        connection->setAutocommit(std::string(operation) == "on");
                    }
                });
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            });
        }
        for (const auto* operation : {"reset", "isolation"}) {
            run("failed deferred reset replaces the physical connection", [operation] {
                auto connection = acquire();
                connection->setAutocommit(false);
                connection->close();
                failNext = operation;
                connection = acquire();
                require(logins == 2, "Failed reset was reused");
                calls.clear();
                connection->close();
                expectCalls({"get", "reset", "allocate_statement", "rollback_batch", "free"});
            });
        }
        run("abandonment rolls back and releases capacity", [] {
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            startWork(statement);
            connection.reset();
            require(commits == 0, "Abandoned connection committed work");
            connection = acquire();
            require(logins == 2, "Abandonment did not release capacity");
            connection->close();
            checkParked();
        });
        for (bool autocommit : {true, false}) {
            run("unpooled close does not run pooled sanitation", [autocommit] {
                auto connection = acquire(false);
                connection->setAutocommit(autocommit);
                calls.clear();
                connection->close(true);
                if (autocommit) {
                    expectCalls({"get", "disconnect", "free"});
                } else {
                    expectCalls({"get", "rollback", "disconnect", "free"});
                }
            });
        }
        for (const auto* operation : {"get", "rollback"}) {
            run("unpooled failure discards without pool sanitation", [operation] {
                auto connection = acquire(false);
                connection->setAutocommit(false);
                failNext = operation;
                expectFailure([&] { connection->close(true); });
                require(std::find(calls.begin(), calls.end(), "disconnect") != calls.end(),
                        "Unpooled failure did not disconnect");
                require(commits == 0, "Unpooled error cleanup committed work");
            });
        }
        run("raw unpooled disconnect failure preserves connection and children", [] {
            auto connection = acquire(false);
            connection->setAutocommit(false);
            auto statement = connection->allocStatementHandle();
            startWork(statement);
            calls.clear();
            expectFailure([&] { connection->close(); });
            expectCalls({"disconnect"});
            require(handles.count(statement->get()) == 1, "Live child was freed");
            auto sibling = connection->allocStatementHandle();
            connection->rollback();
            connection->close();
            require(statement->isImplicitlyFreed() && sibling->isImplicitlyFreed(),
                    "Successful disconnect did not invalidate children");
        });
        ConnectionPoolManager::getInstance().closePools();
        std::cout << passed << " native pool tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL after " << passed << " tests: " << error.what() << '\n';
        failNext.clear();
        ConnectionPoolManager::getInstance().closePools();
        return 1;
    }
}
