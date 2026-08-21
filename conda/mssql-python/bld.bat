@echo on
REM Repackage the prebuilt, ESRP-signed wheel into a conda package (offline), and
REM vendor the ODBC Driver 18 payload INTO it (v1.11.0 model: libs ship inside).
REM PKG_NAME / PKG_VERSION are exported by conda-build; WHEELS_DIR + MSSQL_ODBC_VERSION
REM by the pipeline.
setlocal enabledelayedexpansion
"%PYTHON%" -m pip install --no-deps --no-index --find-links "%WHEELS_DIR%" %PKG_NAME%==%PKG_VERSION% -vv
if errorlevel 1 exit 1

REM Extract the python-agnostic py3-none-win odbc wheel into the SAME site-packages
REM so mssql_python_odbc\libs\ sits beside mssql_python\ (the loader finds the driver
REM there). WHEELS_DIR is staged per-target, so a single matching odbc wheel is present.
set "SP=%PREFIX%\Lib\site-packages"
if not exist "%SP%" mkdir "%SP%"
set "ODBC_WHL="
for %%W in ("%WHEELS_DIR%\mssql_python_odbc-%MSSQL_ODBC_VERSION%-py3-none-win_*.whl") do set "ODBC_WHL=%%~fW"
if not defined ODBC_WHL (
  echo ERROR: no mssql_python_odbc==%MSSQL_ODBC_VERSION% py3-none-win wheel in "%WHEELS_DIR%"
  exit 1
)
echo Extracting "!ODBC_WHL!" into "%SP%"
tar -xf "!ODBC_WHL!" -C "%SP%"
if errorlevel 1 exit 1
