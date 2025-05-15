import os
import importlib.util
import sys
import platform

# Get current Python version and architecture
python_version = f"cp{sys.version_info.major}{sys.version_info.minor}"
if platform.machine().lower() in ('amd64', 'x86_64', 'x64'):
    architecture = "amd64"
elif platform.machine().lower() in ('arm64', 'aarch64'):
    architecture = "arm64"
else:
    architecture = platform.machine().lower()

# Find the specifically matching PYD file
module_dir = os.path.dirname(__file__)
expected_pyd = f"ddbc_bindings.{python_version}-{architecture}.pyd"
pyd_path = os.path.join(module_dir, expected_pyd)

if not os.path.exists(pyd_path):
    # Fallback to searching for any matching PYD if the specific one isn't found
    pyd_files = [f for f in os.listdir(module_dir) if f.startswith('ddbc_bindings.') and f.endswith('.pyd')]
    if not pyd_files:
        raise ImportError(f"No ddbc_bindings PYD module found for {python_version}-{architecture}")
    pyd_path = os.path.join(module_dir, pyd_files[0])
    print(f"Warning: Using fallback PYD file {pyd_files[0]} instead of {expected_pyd}")

# Use the original module name 'ddbc_bindings' that the C extension was compiled with
name = "ddbc_bindings"
spec = importlib.util.spec_from_file_location(name, pyd_path)
module = importlib.util.module_from_spec(spec)
sys.modules[name] = module
spec.loader.exec_module(module)

# Copy all attributes from the loaded module to this module
for attr in dir(module):
    if not attr.startswith('__'):
        globals()[attr] = getattr(module, attr)