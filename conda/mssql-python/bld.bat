@echo on
REM Repackage the prebuilt, signed mssql-python wheel into a conda package (offline) and
REM vendor the ODBC Driver 18 payload inside it. conda-build exports PKG_NAME /
REM PKG_VERSION / CONDA_PY; the pipeline exports WHEELS_DIR + MSSQL_ODBC_VERSION.
setlocal enabledelayedexpansion

set "SP=%PREFIX%\Lib\site-packages"
if not exist "%SP%" mkdir "%SP%"
set "PKG_UNDERSCORE=%PKG_NAME:-=_%"

REM Native leg: pip installs the matching wheel. Cross win-arm64 leg: the arm64 host
REM Python can't run on the x64 agent, so extract the cp%CONDA_PY% wheel (a zip) with tar.
"%PYTHON%" -c "import sys" >nul 2>&1
if errorlevel 1 (
  echo Host Python "%PYTHON%" not executable here ^(non-emulated cross-build^); extracting cp%CONDA_PY% win_arm64 code wheel without Python.
  set "CODE_WHL="
  for %%W in ("%WHEELS_DIR%\!PKG_UNDERSCORE!-%PKG_VERSION%-cp%CONDA_PY%-*-win_arm64.whl") do if exist "%%~fW" set "CODE_WHL=%%~fW"
  if not defined CODE_WHL (
    echo ERROR: no %PKG_NAME%==%PKG_VERSION% cp%CONDA_PY% win_arm64 wheel in "%WHEELS_DIR%"
    exit /b 1
  )
  echo Extracting "!CODE_WHL!" into "%SP%"
  tar -xf "!CODE_WHL!" -C "%SP%"
  if errorlevel 1 exit /b 1
  REM The win-arm64 wheel bundles the x64 mssql_py_core (no arm64 build exists yet); an
  REM x64 .pyd can't load on arm64 and fails the PE-arch assert, so strip it. Bulk copy
  REM then raises a clean "not available" error (lazy import); the rest of the DBAPI works.
  echo Removing x64 mssql_py_core from the win-arm64 package; bulk copy is unavailable on win-arm64 until an arm64 build ships.
  if exist "%SP%\mssql_py_core" rmdir /s /q "%SP%\mssql_py_core"
  if exist "%SP%\mssql_py_core.libs" rmdir /s /q "%SP%\mssql_py_core.libs"
) else (
  "%PYTHON%" -m pip install --no-deps --no-index --find-links "%WHEELS_DIR%" %PKG_NAME%==%PKG_VERSION% -vv
  if errorlevel 1 exit /b 1
)

REM Extract the py3-none-win odbc wheel into the SAME site-packages so
REM mssql_python_odbc\libs\ sits beside mssql_python\ and the loader finds the driver.
set "ODBC_WHL="
for %%W in ("%WHEELS_DIR%\mssql_python_odbc-%MSSQL_ODBC_VERSION%-py3-none-win_*.whl") do if exist "%%~fW" set "ODBC_WHL=%%~fW"
if not defined ODBC_WHL (
  echo ERROR: no mssql_python_odbc==%MSSQL_ODBC_VERSION% py3-none-win wheel in "%WHEELS_DIR%"
  exit /b 1
)
echo Extracting "!ODBC_WHL!" into "%SP%"
tar -xf "!ODBC_WHL!" -C "%SP%"
if errorlevel 1 exit /b 1
