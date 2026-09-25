// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "ddbc_bindings.h"
#include <pybind11/embed.h>
#include <algorithm>
#include <array>
#include <initializer_list>
#include <iostream>
#include <stdexcept>

SQLRETURN FetchMany_wrap(SqlHandlePtr, py::list&, int, const std::string&,
                         const std::string&, int, py::handle);

namespace {

enum class Call { bind, unbind, rowsPointer, rowArraySize, getArraySize, freeHandle };
enum class Failure { none, unbind, rowsPointer };
constexpr SQLLEN integerBytes = sizeof(SQLINTEGER);

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

struct Driver {
    Failure failure;
    std::array<Call, 32> calls{};
    size_t callCount = 0;
    SQLULEN arraySize = 1;
    SQLULEN* rowsFetched = nullptr;
    SQLINTEGER* values = nullptr;
    SQLLEN* indicators = nullptr;
    int diagnostic = 0;
    std::weak_ptr<FetchBindingPlan> owner;
    bool checkOwner = false;
    bool prematureRelease = false;

    SQLRETURN record(Call call) noexcept {
        diagnostic = 0;  // Another ODBC call would overwrite the original failure.
        if (checkOwner && call != Call::freeHandle && owner.expired()) {
            prematureRelease = true;
        }
        if (callCount == calls.size()) {
            return SQL_ERROR;
        }
        calls[callCount++] = call;
        return SQL_SUCCESS;
    }

    SQLRETURN fail() noexcept {
        diagnostic = 42;
        return SQL_ERROR;
    }

    void expect(std::initializer_list<Call> expected) {
        require(callCount == expected.size(), "Unexpected ODBC call after failed cleanup");
        require(std::equal(expected.begin(), expected.end(), calls.begin()),
                "Incorrect cleanup call order");
        callCount = 0;
    }
};

SQLRETURN SQL_API setAttribute(SQLHSTMT handle, SQLINTEGER attribute, SQLPOINTER value,
                               SQLINTEGER) {
    auto& driver = *static_cast<Driver*>(handle);
    if (attribute == SQL_ATTR_ROWS_FETCHED_PTR) {
        if (!SQL_SUCCEEDED(driver.record(Call::rowsPointer))) {
            return SQL_ERROR;
        }
        if (!value && driver.failure == Failure::rowsPointer) {
            return driver.fail();
        }
        driver.rowsFetched = static_cast<SQLULEN*>(value);
        return SQL_SUCCESS;
    }
    if (attribute == SQL_ATTR_ROW_ARRAY_SIZE) {
        if (!SQL_SUCCEEDED(driver.record(Call::rowArraySize))) {
            return SQL_ERROR;
        }
        driver.arraySize = static_cast<SQLULEN>(reinterpret_cast<uintptr_t>(value));
        return SQL_SUCCESS;
    }
    return SQL_ERROR;
}

SQLRETURN SQL_API getAttribute(SQLHSTMT handle, SQLINTEGER attribute, SQLPOINTER value,
                               SQLINTEGER, SQLINTEGER*) {
    auto& driver = *static_cast<Driver*>(handle);
    if (attribute != SQL_ATTR_ROW_ARRAY_SIZE ||
        !SQL_SUCCEEDED(driver.record(Call::getArraySize))) {
        return SQL_ERROR;
    }
    *static_cast<SQLULEN*>(value) = driver.arraySize;
    return SQL_SUCCESS;
}

SQLRETURN SQL_API bindColumn(SQLHSTMT handle, SQLUSMALLINT column, SQLSMALLINT type,
                             SQLPOINTER values, SQLLEN length, SQLLEN* indicators) {
    auto& driver = *static_cast<Driver*>(handle);
    if (!SQL_SUCCEEDED(driver.record(Call::bind)) || column != 1 || type != SQL_C_LONG ||
        length != integerBytes) {
        return SQL_ERROR;
    }
    driver.values = static_cast<SQLINTEGER*>(values);
    driver.indicators = indicators;
    return SQL_SUCCESS;
}

SQLRETURN SQL_API freeStatement(SQLHSTMT handle, SQLUSMALLINT option) {
    auto& driver = *static_cast<Driver*>(handle);
    if (option != SQL_UNBIND || !SQL_SUCCEEDED(driver.record(Call::unbind))) {
        return SQL_ERROR;
    }
    if (driver.failure == Failure::unbind) {
        return driver.fail();
    }
    driver.values = nullptr;
    driver.indicators = nullptr;
    return SQL_SUCCESS;
}

SQLRETURN SQL_API freeHandle(SQLSMALLINT, SQLHANDLE handle) {
    auto& driver = *static_cast<Driver*>(handle);
    driver.record(Call::freeHandle);
    driver.values = nullptr;
    driver.indicators = nullptr;
    driver.rowsFetched = nullptr;
    return SQL_SUCCESS;
}

void checkRetained(const SqlHandlePtr& statement, Driver& driver,
                   FetchBindingPlan* address, SQLINTEGER* values, SQLLEN* indicators,
                   SQLULEN* rowsFetched) {
    require(driver.owner.use_count() == 1, "The statement must be the only plan owner");
    auto retained = statement->fetchBindings.snapshot();
    require(retained && retained.get() == address, "Failed detach discarded the owned plan");
    require(!retained->matches({retained->generation, retained->metadata}, 2,
                              "ascii", "utf-16le", SQL_C_CHAR),
            "Failed detach left the plan eligible for reuse");
    require(retained->buffers.intBuffers[0].data() == values &&
                retained->buffers.indicators[0].data() == indicators &&
                &retained->rowsFetched == rowsFetched,
            "Failed detach replaced driver-referenced storage");
    require(driver.rowsFetched == rowsFetched && driver.arraySize == 2,
            "Failed cleanup continued resetting statement attributes");
    *driver.rowsFetched = 1;
    require(retained->rowsFetched == 1, "Driver rows-fetched pointer lost its owner");
    if (driver.failure == Failure::unbind) {
        require(driver.values == values && driver.indicators == indicators,
                "Failed unbind lost the column addresses");
        driver.values[0] = 73;
        driver.indicators[0] = integerBytes;
        require(retained->buffers.intBuffers[0][0] == 73 &&
                    retained->buffers.indicators[0][0] == integerBytes,
                "Driver column pointers lost their owners");
    }
    bool replacementRejected = false;
    try {
        statement->fetchBindings.install(retained);
    } catch (const std::logic_error&) {
        replacementRejected = true;
    }
    require(replacementRejected, "A still-bound plan could be replaced");
    require(!driver.prematureRelease, "An ODBC callback observed expired ownership");
}

void testCleanup(Failure failure) {
    Driver driver{Failure::none};
    auto statement = std::make_shared<SqlHandle>(
        SQL_HANDLE_STMT, &driver, std::make_shared<ConnectionCleanupState>());
    auto metadata = std::make_shared<ResultMetadata>();
    metadata->columns.push_back({u"value", SQL_INTEGER, 10});
    metadata->namesValidated = true;
    statement->resultMetadata.publish(0, metadata);
    auto plan = std::shared_ptr<FetchBindingPlan>(
        new FetchBindingPlan(statement->resultMetadata.snapshot(), 2,
                             "ascii", "utf-16le", SQL_C_CHAR),
        FetchBindingPlan::Deleter{});
    plan->buffers.intBuffers[0].resize(2);
    auto* values = plan->buffers.intBuffers[0].data();
    auto* indicators = plan->buffers.indicators[0].data();
    auto* rowsFetched = &plan->rowsFetched;
    auto* address = plan.get();
    plan->bindings.push_back({1, SQL_C_LONG, values, integerBytes, indicators});
    statement->fetchBindings.install(plan);
    require(SQL_SUCCEEDED(plan->attach(statement->get())), "Initial binding failed");
    require(plan->matches(statement->resultMetadata.snapshot(), 2,
                          "ascii", "utf-16le", SQL_C_CHAR),
            "Initial plan was not reusable");
    driver.expect({Call::rowArraySize, Call::getArraySize, Call::rowsPointer, Call::bind});
    driver.owner = plan;
    std::weak_ptr<const ResultMetadata> metadataLifetime = metadata;
    metadata.reset();
    plan.reset();
    driver.checkOwner = true;
    driver.failure = failure;

    for (int attempt = 0; attempt < 3; ++attempt) {
        py::list rows;
        // Both formerly-compatible and resized fetches must retry cleanup, not rebind/fetch.
        SQLRETURN result = attempt == 0
            ? statement->detachFetchBindings()
            : FetchMany_wrap(statement, rows, attempt == 1 ? 2 : 3,
                             "ascii", "utf-16le", SQL_C_CHAR, py::none());
        require(result == SQL_ERROR && rows.empty(), "Cleanup failure was not propagated");
        require(driver.diagnostic == 42, "Cleanup overwrote the first error diagnostic");
        if (failure == Failure::unbind) {
            driver.expect({Call::unbind});
        } else {
            driver.expect({Call::unbind, Call::rowsPointer});
        }
        require(!statement->resultMetadata.snapshot().metadata,
                "Failed cleanup must invalidate the result metadata");
        checkRetained(statement, driver, address, values, indicators, rowsFetched);
        require(metadataLifetime.use_count() == 1, "Plan no longer owns its native metadata");
    }

    driver.failure = Failure::none;
    require(SQL_SUCCEEDED(statement->detachFetchBindings()), "Cleanup retry failed");
    driver.expect({Call::unbind, Call::rowsPointer, Call::rowArraySize});
    require(!driver.values && !driver.indicators && !driver.rowsFetched && driver.arraySize == 1,
            "Successful cleanup left driver pointers installed");
    require(!statement->fetchBindings.hasPlan() && driver.owner.expired(),
            "Successful cleanup retained the plan");
    // Metadata is the first plan member and is destroyed after all column buffers.
    // Expiry also rejects the emergency deleter's deliberate terminal-retention path.
    require(metadataLifetime.expired(), "Successful cleanup leaked the plan allocation");
    require(!driver.prematureRelease, "Storage was released before the final ODBC use");
    require(SQL_SUCCEEDED(statement->freeHandle()), "Statement free failed");
    driver.expect({Call::freeHandle});
}

}  // namespace

int main(int argc, char** argv) {
    py::scoped_interpreter interpreter{};
    try {
        require(argc == 2, "Expected unbind or rows_pointer");
        std::string mode = argv[1];
        require(mode == "unbind" || mode == "rows_pointer", "Unknown failure mode");
        SQLSetStmtAttr_ptr = setAttribute;
        SQLGetStmtAttr_ptr = getAttribute;
        SQLBindCol_ptr = bindColumn;
        SQLFreeStmt_ptr = freeStatement;
        SQLFreeHandle_ptr = freeHandle;
        testCleanup(mode == "unbind" ? Failure::unbind : Failure::rowsPointer);
        std::cout << "PASS " << mode
                  << ": retained ownership, blocked reuse, successful retry and destruction\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
