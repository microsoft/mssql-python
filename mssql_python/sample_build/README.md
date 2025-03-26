# Sample Extension Builder

This is a minimalistic example of building a Python C++ extension using pybind11 for multiple target architectures on Windows.

## Prerequisites

1. **Visual Studio 2022** with C++ development tools
   - For x64/x86 builds: "Desktop development with C++" workload
   - For ARM64 builds: "C++ ARM64 build tools" component must be installed
   - Make sure the Windows SDK is installed (10.0.22621.0 or newer)

2. **CMake** (3.15 or newer)
   - Download from https://cmake.org/download/
   - Make sure it's in your PATH

3. **Python with pybind11**
   - Install pybind11: `pip install pybind11`
   - The script will attempt to install pybind11 if not found

## Building the Extension

The `build.bat` script supports building for three architectures:
- x64 (AMD64)
- x86 (32-bit)
- ARM64

### Basic Usage

```
build.bat [architecture] [build_type]
```

Where:
- `architecture` is one of: `x64`, `x86`, `arm64`, or `all` (default: `x64`)
- `build_type` is either `Debug` or `Release` (default: `Debug`)

### Examples

1. Build for x64 in debug mode (default):
   ```
   build.bat
   ```

2. Build for ARM64 in release mode:
   ```
   build.bat arm64 Release
   ```

3. Build for x86 in debug mode:
   ```
   build.bat x86
   ```

4. Build for all architectures in release mode:
   ```
   build.bat all Release
   ```

## Output Files

The built extensions will be copied to the sample_build directory with architecture-specific names:
- `sample_extension_x64.pyd`
- `sample_extension_x86.pyd`
- `sample_extension_arm64.pyd`

## Testing the Extension

You can test the extension in Python:

```python
import sample_extension

# Print the architecture the extension was built for
print(sample_extension.get_architecture())

# Test the add function
result = sample_extension.add(2, 3)
print(f"2 + 3 = {result}")

# Create a Pet object
pet = sample_extension.Pet("Rex")
print(f"Pet name: {pet.get_name()}")
pet.set_name("Fido")
print(f"New pet name: {pet.name}")
```

## Notes for Cross-Compilation

### Building for ARM64

To build for ARM64, you must have the "C++ ARM64 build tools" component installed in Visual Studio. 
This can be added through the Visual Studio Installer:

1. Open Visual Studio Installer
2. Modify your Visual Studio 2022 installation
3. Under "Desktop development with C++", check "C++ ARM64 build tools"
4. Click "Modify" to install the component

The build script automatically:
- Detects if you're on a native ARM64 machine or cross-compiling from x64
- Sets up the appropriate Visual Studio environment variables
- Configures CMake with the correct architecture settings
- Shows a helpful error message if ARM64 build tools are missing

### Distributing ARM64 Binaries

When distributing ARM64 binaries:
- ARM64 binaries can run on Windows 11 on ARM devices
- They provide native performance on ARM64 hardware
- Make sure to include the architecture in the package name to avoid confusion

## Troubleshooting

1. **ARM64 build fails with "build tools not found" error**
   - Ensure the "C++ ARM64 build tools" component is installed in Visual Studio
   - Try repairing your Visual Studio installation

2. **CMake can't find pybind11**
   - Verify pybind11 is installed: `pip list | findstr pybind11`
   - The build script will attempt to install pybind11 if not found

3. **Build fails with compiler errors**
   - Check that Visual Studio has the necessary components for the target architecture
   - For ARM64, make sure you have the "C++ ARM64 build tools" installed

4. **Wrong architecture detected**
   - The sample code detects architecture using preprocessor macros
   - You can verify the built architecture using the `get_architecture()` function