import os
import importlib.util
import sys
import platform

# Get current Python version and architecture
python_version = f"cp{sys.version_info.major}{sys.version_info.minor}"
if platform.machine().lower() in ('amd64', 'x86_64', 'x64'):
    architecture = "amd64" if sys.platform == 'win32' else "x86_64"
elif platform.machine().lower() in ('arm64', 'aarch64'):
    architecture = "arm64"
else:
    architecture = platform.machine().lower()

# Determine extension based on platform
if sys.platform == 'win32':
    extension = '.pyd'
else:  # macOS or Linux
    extension = '.so'

# Find the specifically matching module file
module_dir = os.path.dirname(__file__)
expected_module = f"ddbc_bindings.{python_version}-{architecture}{extension}"
module_path = os.path.join(module_dir, expected_module)

if not os.path.exists(module_path):
    # Fallback to searching for any matching module if the specific one isn't found
    module_files = [f for f in os.listdir(module_dir) if f.startswith('ddbc_bindings.') and f.endswith(extension)]
    if not module_files:
        raise ImportError(f"No ddbc_bindings module found for {python_version}-{architecture} with extension {extension}")
    module_path = os.path.join(module_dir, module_files[0])
    print(f"Warning: Using fallback module file {module_files[0]} instead of {expected_module}")

# Use the original module name 'ddbc_bindings' that the C extension was compiled with
name = "ddbc_bindings"
spec = importlib.util.spec_from_file_location(name, module_path)
module = importlib.util.module_from_spec(spec)
sys.modules[name] = module
spec.loader.exec_module(module)

# Copy all attributes from the loaded module to this module
for attr in dir(module):
    if not attr.startswith('__'):
        globals()[attr] = getattr(module, attr)