@echo on
REM Repackage the prebuilt, ESRP-signed wheel into a PYTHON-AGNOSTIC conda package.
REM
REM Windows conda site-packages (`Lib\site-packages`) is NOT Python-version-pathed,
REM so ONE package serves every Python -- the analog of the PyPI `py3-none-win_*`
REM wheel. There is therefore NO `python` in host (see meta.yaml `# [not win]`), so
REM `%PYTHON%`/pip are unavailable here; instead extract the wheel (a zip) directly
REM with `tar` into the env's site-packages. conda-build then emits ONE package with
REM no `pyXY` build string, resolvable by every mssql-python (binding) Python.
REM PKG_NAME / PKG_VERSION are exported by conda-build; WHEELS_DIR by the pipeline.
setlocal enabledelayedexpansion
set "SP=%PREFIX%\Lib\site-packages"
if not exist "%SP%" mkdir "%SP%"
REM Wheels use underscores (mssql_python_odbc); the conda package name uses hyphens.
set "PKG_UNDERSCORE=%PKG_NAME:-=_%"
set "WHL="
for %%W in ("%WHEELS_DIR%\%PKG_UNDERSCORE%-%PKG_VERSION%-py3-none-win_*.whl") do set "WHL=%%~fW"
if not defined WHL (
  echo ERROR: no %PKG_NAME%==%PKG_VERSION% py3-none-win wheel found in "%WHEELS_DIR%"
  exit 1
)
echo Extracting "!WHL!" into "%SP%"
tar -xf "!WHL!" -C "%SP%"
if errorlevel 1 exit 1
