// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection/connection.h"
#include "connection/connection_pool.h"
#include "logger_bridge.hpp"
#include "performance_counter.hpp"
#include <pybind11/embed.h>
#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>
#include <thread>
#include <unordered_map>

SQLRETURN SQLResetStmt_wrap(SqlHandlePtr statementHandle);

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

SQLRETURN SQL_API freeStatement(SQLHSTMT, SQLUSMALLINT option) {
    return record(option == SQL_CLOSE ? "statement_close" : "statement_reset_params");
}

SQLRETURN SQL_API setStatementAttr(SQLHSTMT, SQLINTEGER attribute, SQLPOINTER, SQLINTEGER) {
    require(attribute == SQL_ATTR_PARAMSET_SIZE, "Unexpected statement attribute");
    return record("statement_paramset_size");
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
    SQLFreeStmt_ptr = freeStatement;
    SQLSetStmtAttr_ptr = setStatementAttr;

    int passed = 0;
    auto run = [&](const char* name, const std::function<void()>& test) {
        resetScenario();
        test();
        ++passed;
        std::cout << "PASS " << name << '\n';
    };
    try {
        run("profiler generation rejects in-flight timers across all aggregate boundaries", [] {
            auto& counter = mssql_profiling::PerformanceCounter::instance();
            counter.disable();
            counter.set_sample_every(1);
            for (int boundary = 0; boundary < 3; ++boundary) {
                counter.enable();
                {
                    mssql_profiling::ScopedTimer timer("old");
                    if (boundary == 0) {
                        counter.reset();
                    } else if (boundary == 1) {
                        counter.reset_stats_only();
                    } else {
                        counter.disable();
                        counter.enable();
                    }
                }
                require(counter.get_stats().empty(), "Old timer crossed aggregate boundary");
            }
            counter.disable();
            {
                mssql_profiling::ScopedTimer disabled("disabled");
                counter.enable();
            }
            require(counter.get_stats().empty(), "Disabled timer recorded after enable");
            counter.disable();
        });
        run("profiler concurrent recording and sparse events stay unscaled", [] {
            auto& counter = mssql_profiling::PerformanceCounter::instance();
            counter.disable();
            for (uint32_t every : {1u, 16u}) {
                counter.set_sample_every(every);
                counter.enable();
                std::vector<std::thread> workers;
                for (int thread = 0; thread < 8; ++thread) {
                    workers.emplace_back([&counter] {
                        for (int i = 0; i < 10000; ++i) {
                            counter.event("event");
                        }
                    });
                }
                for (auto& worker : workers) worker.join();
                counter.disable();
                auto entry = counter.get_stats()["event"].cast<py::dict>();
                auto count = entry["calls"].cast<int64_t>();
                require(every == 1 ? count == 80000 : count > 3500 && count < 6500,
                        "Unexpected sample count (or periodic/whole-call counting)");
                require(entry["total_us"].cast<double>() == 0.0,
                        "Events acquired duration or were extrapolated");
            }
            counter.set_sample_every(1);
        });
        run("statement reset retains all three ODBC steps and metadata invalidation", [] {
            auto connection = acquire();
            auto statement = connection->allocStatementHandle();
            auto generation = statement->resultMetadata.snapshot().generation;
            statement->resultMetadata.publish(generation, std::make_shared<ResultMetadata>());
            calls.clear();
            require(SQLResetStmt_wrap(statement) == SQL_SUCCESS, "Statement reset failed");
            expectCalls({"statement_close", "statement_reset_params", "statement_paramset_size"});
            const auto snapshot = statement->resultMetadata.snapshot();
            require(!snapshot.metadata && snapshot.generation == generation + 1,
                    "Statement reset lost metadata invalidation");
            statement.reset();
            connection->close();
        });
        for (const auto* operation : {"statement_close", "statement_reset_params",
                                      "statement_paramset_size"}) {
            run("statement reset preserves first failure", [operation] {
                auto connection = acquire();
                auto statement = connection->allocStatementHandle();
                calls.clear();
                failNext = operation;
                require(SQLResetStmt_wrap(statement) == SQL_ERROR, "Reset swallowed failure");
                require(calls.back() == operation, "ODBC work continued after reset failure");
                statement.reset();
                connection->close();
            });
        }
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
#ifdef ENABLE_PROFILING
        run("autocommit profiler distinguishes full, eligible and skipped setters", [] {
            auto connection = acquire();
            auto& counter = mssql_profiling::PerformanceCounter::instance();
            counter.reset();
            counter.enable();
            connection->setAutocommit(true);
            counter.disable();
            auto stats = counter.get_stats();
            require(stats["ddbc::Connection::setAutocommit::full_path"]["calls"].cast<int>() == 1,
                    "Full setter was not counted");
            require(!stats.contains("ddbc::Connection::setAutocommit::skip"),
                    "New login incorrectly counted as a skip");
            connection->close();
            connection = acquire();
            counter.reset();
            counter.enable();
            connection->setAutocommit(true);
            counter.disable();
            stats = counter.get_stats();
            require(stats["ddbc::Connection::setAutocommit::proof_eligible"]["calls"].cast<int>() == 1 &&
                    stats["ddbc::Connection::setAutocommit::skip"]["calls"].cast<int>() == 1 &&
                    !stats.contains("ddbc::Connection::setAutocommit::full_path"),
                    "Proven setter was not counted as eligible and skipped");
            connection->close();
            counter.reset();
        });
#endif
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
