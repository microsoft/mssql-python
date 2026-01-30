#!/bin/bash

# Script to reproduce the segfault issue with SQLAlchemy
# This script will:
# 1. Clone SQLAlchemy from gerrit
# 2. Fetch the specific review that includes the reconnect test
# 3. Install SQLAlchemy and dependencies
# 4. Run the reconnect test multiple times to trigger the segfault

set -e
set -x

WORK_DIR="/tmp/sqlalchemy-segfault-test"
DB_URI="mssql+mssqlpython://scott:tiger^5HHH@localhost:1433/test?Encrypt=No"

echo "========================================"
echo "SQLAlchemy Segfault Reproduction Test"
echo "========================================"
echo ""
echo "This test will attempt to reproduce a segfault that occurs"
echo "when SQLAlchemy's connection pool invalidates connections"
echo "with active cursors/statement handles."
echo ""

# Clean up any previous test directory
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

# Clone SQLAlchemy from gerrit
echo "Cloning SQLAlchemy from gerrit..."
git clone https://gerrit.sqlalchemy.org/sqlalchemy/sqlalchemy

cd sqlalchemy

# Fetch the specific review that includes the reconnect test
echo "Fetching gerrit review 6149..."
git fetch https://gerrit.sqlalchemy.org/sqlalchemy/sqlalchemy refs/changes/49/6149/14
git checkout FETCH_HEAD

# Install SQLAlchemy and test dependencies
echo "Installing SQLAlchemy and dependencies..."
pip3 install --user -e .
pip3 install --user pytest greenlet typing-extensions

# Run the test suite multiple times to trigger the segfault
echo ""
echo "========================================"
echo "Running reconnect tests to reproduce segfault..."
echo "========================================"
echo "This will run the test suite in a loop until it crashes or completes 10 runs."
echo ""

CRASH_DETECTED=0
for i in {1..10}; do
    echo ""
    echo "=== Test Run $i ==="
    
    if ! pytest test/engine/test_reconnect.py::RealReconnectTest \
        --dburi "$DB_URI" \
        --disable-asyncio -s -v 2>&1; then
        
        echo ""
        echo "*** CRASH DETECTED ON RUN $i ***"
        CRASH_DETECTED=1
        break
    fi
    
    echo "Run $i completed successfully"
    sleep 1
done

echo ""
if [ $CRASH_DETECTED -eq 0 ]; then
    echo "========================================"
    echo "No crash detected in 10 runs."
    echo "The issue may be intermittent or already fixed."
    echo "========================================"
else
    echo "========================================"
    echo "*** SEGFAULT REPRODUCED! ***"
    echo "========================================"
fi

echo ""
echo "Test directory: $WORK_DIR"
