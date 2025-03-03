:: Usage -
:: 1)   "cd mssql_python/pybind"
:: 2)a) "build.bat"         - Generates debug build of DDBC bindings
:: 2)b) "build.bat Release" - Generates release build of DDBC bindings
:: 2)c) "build.bat Debug"   - Generates debug build of DDBC bindings

rmdir /s /q build
mkdir build
cd build
del CMakeCache.txt

IF "%1"=="" (
  set BUILD_TYPE=Debug
) ELSE (
  set BUILD_TYPE=%1
)

cmake -DPython3_EXECUTABLE="python3" -DCMAKE_BUILD_TYPE=%BUILD_TYPE% ..
msbuild ddbc_bindings.sln /p:Configuration=%BUILD_TYPE%
move /Y %BUILD_TYPE%\ddbc_bindings.pyd ..\..\ddbc_bindings.pyd
cd ..
