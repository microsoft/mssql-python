@echo on
REM Repackage the prebuilt, ESRP-signed wheel into a conda package (offline), and
REM vendor the ODBC Driver 18 payload INTO it (v1.11.0 model: libs ship inside).
REM PKG_NAME / PKG_VERSION are exported by conda-build; WHEELS_DIR + MSSQL_ODBC_VERSION
REM by the pipeline.
setlocal enabledelayedexpansion

REM Site-packages of the (possibly cross-targeted) host env, plus the underscore form
REM of the package name used in wheel filenames (mssql-python -> mssql_python).
set "SP=%PREFIX%\Lib\site-packages"
if not exist "%SP%" mkdir "%SP%"
set "PKG_UNDERSCORE=%PKG_NAME:-=_%"

REM Install the prebuilt code wheel. On a NATIVE leg the host Python runs and pip
REM installs it (pip auto-selects the wheel matching this host). On a CROSS win-arm64
REM build the win-arm64 host Python CANNOT execute on the x64 agent (Windows has no
REM reverse emulation), so extract the wheel (a zip) WITHOUT Python -- the same
REM approach build.sh uses for the osx-arm64 cross-build. conda-build exports CONDA_PY
REM (e.g. 314), which selects the matching cp tag of the win_arm64 slice.
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
  REM No arm64 Windows build of mssql_py_core exists yet, so the win-arm64 code wheel
  REM bundles the x64 (win_amd64) one. An x64 .pyd cannot load on arm64 -- it would
  REM crash bulk copy AND fails the PE-arch assert -- so REMOVE it from the win-arm64
  REM package. Bulk copy then raises a clean "not available" error (the import is lazy);
  REM the rest of the DBAPI is unaffected (ddbc_bindings is native arm64). Restore once
  REM an arm64 mssql_py_core ships in the feed.
  echo Removing x64 mssql_py_core from the win-arm64 package; bulk copy is unavailable on win-arm64 until an arm64 build ships.
  if exist "%SP%\mssql_py_core" rmdir /s /q "%SP%\mssql_py_core"
  if exist "%SP%\mssql_py_core.libs" rmdir /s /q "%SP%\mssql_py_core.libs"
) else (
  "%PYTHON%" -m pip install --no-deps --no-index --find-links "%WHEELS_DIR%" %PKG_NAME%==%PKG_VERSION% -vv
  if errorlevel 1 exit /b 1
)

REM Extract the python-agnostic py3-none-win odbc wheel into the SAME site-packages
REM so mssql_python_odbc\libs\ sits beside mssql_python\ (the loader finds the driver
REM there). WHEELS_DIR is staged per-target, so a single matching odbc wheel is present.
set "ODBC_WHL="
for %%W in ("%WHEELS_DIR%\mssql_python_odbc-%MSSQL_ODBC_VERSION%-py3-none-win_*.whl") do if exist "%%~fW" set "ODBC_WHL=%%~fW"
if not defined ODBC_WHL (
  echo ERROR: no mssql_python_odbc==%MSSQL_ODBC_VERSION% py3-none-win wheel in "%WHEELS_DIR%"
  exit /b 1
)
echo Extracting "!ODBC_WHL!" into "%SP%"
tar -xf "!ODBC_WHL!" -C "%SP%"
if errorlevel 1 exit /b 1
