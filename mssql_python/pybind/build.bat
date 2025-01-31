mkdir build
cd build
del CMakeCache.txt
cmake -DPython3_EXECUTABLE="python3" -DCMAKE_BUILD_TYPE=Release ..
msbuild ddbc_bindings.sln /p:Configuration=Release
move /Y Release\ddbc_bindings.pyd ..\..\ddbc_bindings.pyd
cd ..
rmdir /s /q build