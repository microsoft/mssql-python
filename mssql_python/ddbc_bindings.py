import os
import importlib.util
import sys
import platform

def normalize_architecture(platform_name, architecture):
    """
    Normalize architecture names for the given platform.
    
    Args:
        platform_name (str): Platform name ('windows', 'darwin', 'linux')
        architecture (str): Architecture string to normalize
        
    Returns:
        str: Normalized architecture name
        
    Raises:
        ImportError: If architecture is not supported for the given platform
        OSError: If platform is not supported
    """
    arch_lower = architecture.lower()
    
    if platform_name == "windows":
        arch_map = {
            "win64": "x64", "amd64": "x64", "x64": "x64",
            "win32": "x86", "x86": "x86",
            "arm64": "arm64"
        }
        if arch_lower in arch_map:
            return arch_map[arch_lower]
        else:
            supported = list(set(arch_map.keys()))
            raise ImportError(f"Unsupported architecture '{architecture}' for platform '{platform_name}'; expected one of {supported}")
    
    elif platform_name == "darwin":
        # For macOS, return runtime architecture
        return platform.machine().lower()
    
    elif platform_name == "linux":
        arch_map = {
            "x64": "x86_64", "amd64": "x86_64", "x86_64": "x86_64",
            "arm64": "arm64", "aarch64": "arm64"
        }
        if arch_lower in arch_map:
            return arch_map[arch_lower]
        else:
            supported = list(set(arch_map.keys()))
            raise ImportError(f"Unsupported architecture '{architecture}' for platform '{platform_name}'; expected one of {supported}")
    
    else:
        supported_platforms = ["windows", "darwin", "linux"]
        raise OSError(f"Unsupported platform '{platform_name}'; expected one of {supported_platforms}")

# Get current Python version and architecture
python_version = f"cp{sys.version_info.major}{sys.version_info.minor}"

platform_name = platform.system().lower()
raw_architecture = platform.machine().lower()

# Special handling for macOS universal2 binaries
if platform_name == 'darwin':
    architecture = "universal2"
else:
    architecture = normalize_architecture(platform_name, raw_architecture)
    
    # Handle Windows-specific naming for binary files
    if platform_name == 'windows' and architecture == 'x64':
        architecture = "amd64"

# Validate supported platforms
if platform_name not in ['windows', 'darwin', 'linux']:
    supported_platforms = ['windows', 'darwin', 'linux']
    raise ImportError(f"Unsupported platform '{platform_name}' for mssql-python; expected one of {supported_platforms}")

# Determine extension based on platform
if platform_name == 'windows':
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