@echo on
REM Repackage the prebuilt, ESRP-signed wheel into a conda package (offline).
REM PKG_NAME / PKG_VERSION are exported by conda-build; WHEELS_DIR by the pipeline.
"%PYTHON%" -m pip install --no-deps --no-index --find-links "%WHEELS_DIR%" %PKG_NAME%==%PKG_VERSION% -vv
if errorlevel 1 exit 1
