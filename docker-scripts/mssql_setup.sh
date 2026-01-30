#!/bin/bash
set -x
export ACCEPT_EULA='Y'
export MSSQL_SA_PASSWORD='wh0_CAR;ES!!'
export MSSQL_PID=Developer
/opt/mssql/bin/mssql-conf set memory.memorylimitmb 4096
/opt/mssql/bin/sqlservr & PID=$!
sleep 30s
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -d master -i /mssql_setup.sql
kill ${PID}
sleep 10s
