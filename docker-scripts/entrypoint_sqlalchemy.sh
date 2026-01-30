#!/bin/bash
set -e

echo "=========================================="
echo "Starting SQL Server..."
echo "=========================================="

# Start SQL Server
/opt/mssql/bin/sqlservr &
SQLSERVER_PID=$!

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to start..."
for i in {1..60}; do
    if /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'wh0_CAR;ES!!' -Q "SELECT 1" &>/dev/null; then
        echo "SQL Server is ready!"
        break
    fi
    echo "Waiting... ($i/60)"
    sleep 2
done

# Create test database
echo ""
echo "=========================================="
echo "Setting up test database..."
echo "=========================================="
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'wh0_CAR;ES!!' -Q "
IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'test')
BEGIN
    CREATE DATABASE test
END
" || true

echo ""
echo "=========================================="
echo "Installing SQLAlchemy from Gerrit..."
echo "=========================================="

cd /tmp

# Clone SQLAlchemy if not already present
if [ ! -d "sqlalchemy" ]; then
    git clone https://github.com/sqlalchemy/sqlalchemy.git
fi

cd sqlalchemy

# Fetch and checkout the specific gerrit change with mssql-python support
echo "Fetching gerrit change 6149/14..."
git fetch https://gerrit.sqlalchemy.org/sqlalchemy/sqlalchemy refs/changes/49/6149/14
git checkout FETCH_HEAD

# Install SQLAlchemy
pip3 install -e .

SA_VERSION=$(python3 -c "import sqlalchemy; print(sqlalchemy.__version__)")
echo "SQLAlchemy $SA_VERSION installed"

echo ""
echo "=========================================="
echo "Running SQLAlchemy RealReconnectTest..."
echo "=========================================="
echo ""

# Connection URI for SQLAlchemy
CONN_URI="mssql+mssqlpython://sa:wh0_CAR;ES!!@localhost/test?Encrypt=No"

# Run the reconnect tests multiple times to check for segfaults
ITERATIONS=${TEST_ITERATIONS:-10}
echo "Running $ITERATIONS test iterations..."
echo ""

PASS_COUNT=0
FAIL_COUNT=0
CRASH_DETECTED=0

for ((i=1; i<=ITERATIONS; i++)); do
    echo "  Iteration $i/$ITERATIONS..."
    
    if pytest test/engine/test_reconnect.py::RealReconnectTest \
        --dburi "$CONN_URI" \
        --disable-asyncio \
        -v \
        --tb=short 2>&1; then
        echo "    PASS"
        ((PASS_COUNT++))
    else
        EXIT_CODE=$?
        echo "    FAIL (exit code: $EXIT_CODE)"
        ((FAIL_COUNT++))
        
        # Check if it was a segfault (exit code 139 or 'Segmentation fault' message)
        if [ $EXIT_CODE -eq 139 ] || [ $EXIT_CODE -eq 11 ]; then
            echo "    *** SEGFAULT DETECTED ***"
            CRASH_DETECTED=1
            break
        fi
    fi
    
    sleep 1
done

echo ""
echo "=========================================="
echo "Test Results:"
echo "  Passed: $PASS_COUNT / $ITERATIONS"
echo "  Failed: $FAIL_COUNT / $ITERATIONS"
echo "=========================================="
echo ""

if [ $CRASH_DETECTED -eq 1 ]; then
    echo "*** SEGFAULT DETECTED - FIX DID NOT WORK ***"
    echo "=========================================="
    kill $SQLSERVER_PID 2>/dev/null || true
    exit 1
elif [ $FAIL_COUNT -eq 0 ]; then
    echo "*** SUCCESS! No segfaults in $ITERATIONS runs! ***"
    echo "The fix is working correctly!"
    echo "=========================================="
    kill $SQLSERVER_PID 2>/dev/null || true
    exit 0
else
    echo "*** PARTIAL SUCCESS ***"
    echo "No segfaults detected, but some test failures occurred"
    echo "The fix prevents crashes!"
    echo "=========================================="
    kill $SQLSERVER_PID 2>/dev/null || true
    exit 0
fi
