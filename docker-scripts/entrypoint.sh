#!/bin/bash
set -e
set -x
export ACCEPT_EULA='Y'
export MSSQL_SA_PASSWORD='wh0_CAR;ES!!'
export MSSQL_PID=Developer

# Start SQL Server in the background
/opt/mssql/bin/sqlservr & PID=$!

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to start..."
sleep 30s

# Clone SQLAlchemy from gerrit
echo ""
echo "========================================="
echo "Cloning SQLAlchemy from gerrit..."
echo "========================================="
cd /
git clone https://gerrit.sqlalchemy.org/sqlalchemy/sqlalchemy

cd /sqlalchemy

# Fetch the specific review
echo ""
echo "Fetching gerrit review 6149..."
git fetch https://gerrit.sqlalchemy.org/sqlalchemy/sqlalchemy refs/changes/49/6149/14
git checkout FETCH_HEAD

# Install SQLAlchemy and test dependencies
echo ""
echo "Installing SQLAlchemy and dependencies..."
pip3 install -e .
pip3 install pytest greenlet typing-extensions

# Run the test suite multiple times to trigger the segfault
echo ""
echo "========================================="
echo "Testing PyPI version (expected to SEGFAULT)"
echo "========================================="
echo "This will run the test suite in a loop until it crashes or completes 10 runs."
echo ""

CRASH_DETECTED=0
for i in {1..10}; do
    echo ""
    echo "=== Test Run $i ==="

    if ! pytest test/engine/test_reconnect.py::RealReconnectTest \
        --dburi "mssql+mssqlpython://scott:tiger^5HHH@localhost:1433/test?Encrypt=No" \
        --disable-asyncio -s -v 2>&1; then

        echo ""
        echo "*** CRASH DETECTED ON RUN $i ***"
        CRASH_DETECTED=1
        break
    fi

    echo "Run $i completed successfully"
    sleep 1
done

if [ $CRASH_DETECTED -eq 0 ]; then
    echo ""
    echo "*** No crash in 10 runs. The issue is intermittent and may require more runs. ***"
else
    echo ""
    echo "========================================="
    echo "*** SEGFAULT REPRODUCED! ***"
    echo "This demonstrates the use-after-free bug"
    echo "========================================="
fi

# Shut down SQL Server cleanly
echo ""
echo "Shutting down SQL Server..."
kill ${PID} 2>/dev/null || true
sleep 5s
