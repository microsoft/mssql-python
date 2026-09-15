@echo on
REM Repackage the prebuilt, signed mssql-python wheel into a conda package (offline) and
REM vendor the ODBC Driver 18 payload inside it. conda-build exports PKG_NAME /
REM PKG_VERSION / CONDA_PY; the pipeline exports WHEELS_DIR + MSSQL_ODBC_VERSION.
setlocal enabledelayedexpansion

set "SP=%PREFIX%\Lib\site-packages"
if not exist "%SP%" mkdir "%SP%"
set "PKG_UNDERSCORE=%PKG_NAME:-=_%"

REM Target arch comes from the conda TARGET platform (win-64 / win-arm64), NOT from
REM whether the host Python can execute -- a native win-arm64 host runs its own Python yet
REM still needs the win_arm64 payload. conda-build sets target_platform; CONDA_SUBDIR is
REM the same value. This is the single source for both the code-wheel and ODBC driver arch.
set "ODBC_ARCH=win_amd64"
set "TGT_PLATFORM=%target_platform%"
if not defined TGT_PLATFORM set "TGT_PLATFORM=%CONDA_SUBDIR%"
if /i "%TGT_PLATFORM%"=="win-arm64" set "ODBC_ARCH=win_arm64"

REM Native leg: the host Python runs, so pip installs the matching wheel. Cross leg: the
REM target Python can't run on this agent (e.g. win_arm64 built on x64), so extract the
REM cp%CONDA_PY% wheel (a zip) with tar. This is orthogonal to the target arch above.
"%PYTHON%" -c "import sys" >nul 2>&1
if errorlevel 1 (
  echo Host Python "%PYTHON%" not executable here ^(non-emulated cross-build^); extracting cp%CONDA_PY% !ODBC_ARCH! code wheel without Python.
  set "CODE_WHL="
  for %%W in ("%WHEELS_DIR%\!PKG_UNDERSCORE!-%PKG_VERSION%-cp%CONDA_PY%-*-!ODBC_ARCH!.whl") do if exist "%%~fW" set "CODE_WHL=%%~fW"
  if not defined CODE_WHL (
    echo ERROR: no %PKG_NAME%==%PKG_VERSION% cp%CONDA_PY% !ODBC_ARCH! wheel in "%WHEELS_DIR%"
    exit /b 1
  )
  echo Extracting "!CODE_WHL!" into "%SP%"
  tar -xf "!CODE_WHL!" -C "%SP%"
  if errorlevel 1 exit /b 1
  REM win-arm64 cross can't run the arm64 Python, so statically prove the extracted
  REM binding is for THIS interpreter (the osx-arm64 twin bug shipped a cp310 .so in
  REM every build); a wrong-Python .pyd would only fail at the user's import.
  if not exist "%SP%\mssql_python\ddbc_bindings.cp%CONDA_PY%-*.pyd" (
    echo ERROR: extracted "!CODE_WHL!" has no mssql_python\ddbc_bindings.cp%CONDA_PY% pyd ^(wrong-Python binding^).
    exit /b 1
  )
  REM Keep mssql_py_core when the wheel provides a matching-arch native ext so bulk copy
  REM ships (PR #737 makes the win-arm64 wheel vendor the arm64 core). If only the legacy
  REM x64 core is present (a pre-#737 wheel), strip it so the package never carries a core
  REM that can't load on the target -- the .pyd name encodes the arch. Bulk copy then lazily
  REM reports "not available"; the rest of the DBAPI works. Mirrors the ddbc check above.
  if exist "%SP%\mssql_py_core\mssql_py_core.cp%CONDA_PY%-!ODBC_ARCH!.pyd" (
    echo Keeping matching-arch mssql_py_core; bulk copy enabled on the !ODBC_ARCH! package.
  ) else (
    echo No cp%CONDA_PY%-!ODBC_ARCH! mssql_py_core in the wheel; removing the mismatched core ^(bulk copy unavailable until the arm64-core wheel ships^).
    if exist "%SP%\mssql_py_core" rmdir /s /q "%SP%\mssql_py_core"
    if exist "%SP%\mssql_py_core.libs" rmdir /s /q "%SP%\mssql_py_core.libs"
  )
) else (
  "%PYTHON%" -m pip install --no-deps --no-index --find-links "%WHEELS_DIR%" %PKG_NAME%==%PKG_VERSION% -vv
  if errorlevel 1 exit /b 1
)

REM Extract the arch-specific odbc wheel into the SAME site-packages so
REM mssql_python_odbc\libs\ sits beside mssql_python\ and the loader finds the driver.
REM The py3-none tag only means "no Python bytecode" -- the vendored driver DLLs ARE
REM arch-specific, so match the EXACT target arch (%ODBC_ARCH%, derived from the conda
REM target platform above) rather than py3-none-win_* which also matches the other arch's
REM wheel and could vendor an x64 driver into a win-arm64 package. The exact
REM tag is unique per version+arch, so no ambiguous multi-match is possible.
set "ODBC_WHL="
for %%W in ("%WHEELS_DIR%\mssql_python_odbc-%MSSQL_ODBC_VERSION%-py3-none-%ODBC_ARCH%.whl") do if exist "%%~fW" set "ODBC_WHL=%%~fW"
if not defined ODBC_WHL (
  echo ERROR: no mssql_python_odbc==%MSSQL_ODBC_VERSION% py3-none-%ODBC_ARCH% wheel in "%WHEELS_DIR%"
  exit /b 1
)
echo Extracting "!ODBC_WHL!" into "%SP%"
tar -xf "!ODBC_WHL!" -C "%SP%"
if errorlevel 1 exit /b 1
