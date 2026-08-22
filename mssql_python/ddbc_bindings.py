"""
Dynamic loading of platform-specific DDBC bindings for mssql-python.

This module handles the runtime loading of the appropriate compiled extension
module based on the current platform, architecture, and Python version.
"""

import os
import importlib.util
import sys
import platform
import sysconfig
import warnings


def normalize_architecture(platform_name_param, architecture_param):
    """
    Normalize architecture names for the given platform.

    Args:
        platform_name_param (str): Platform name ('windows', 'darwin', 'linux')
        architecture_param (str): Architecture string to normalize

    Returns:
        str: Normalized architecture name

    Raises:
        ImportError: If architecture is not supported for the given platform
        OSError: If platform is not supported
    """
    arch_lower = architecture_param.lower()

    if platform_name_param == "windows":
        arch_map = {
            "win64": "x64",
            "amd64": "x64",
            "x64": "x64",
            "win32": "x86",
            "x86": "x86",
            "arm64": "arm64",
        }
        if arch_lower in arch_map:
            return arch_map[arch_lower]
        supported = list(set(arch_map.keys()))
        raise ImportError(
            f"Unsupported architecture '{architecture_param}' for platform "
            f"'{platform_name_param}'; expected one of {supported}"
        )

    if platform_name_param == "darwin":
        # For macOS, return runtime architecture
        return platform.machine().lower()

    if platform_name_param == "linux":
        arch_map = {
            "x64": "x86_64",
            "amd64": "x86_64",
            "x86_64": "x86_64",
            "arm64": "arm64",
            "aarch64": "arm64",
        }
        if arch_lower in arch_map:
            return arch_map[arch_lower]
        supported = list(set(arch_map.keys()))
        raise ImportError(
            f"Unsupported architecture '{architecture_param}' for platform "
            f"'{platform_name_param}'; expected one of {supported}"
        )

    supported_platforms_list = ["windows", "darwin", "linux"]
    raise OSError(
        f"Unsupported platform '{platform_name_param}'; expected one of "
        f"{supported_platforms_list}"
    )


def get_interpreter_architecture(platform_name_param):
    """
    Get the raw architecture string of the running interpreter.

    On Windows, ``platform.machine()`` reports the host CPU rather than the
    architecture the interpreter was built for. The two differ when an x64
    interpreter runs emulated on a Windows ARM64 machine (the python.org
    "64-bit" installer is x64), so the loader would look for an arm64 binary
    that the installed win_amd64 wheel never shipped. ``sysconfig.get_platform()``
    is derived from the interpreter build and matches the wheel tag, so it is
    the source of truth on Windows.

    Args:
        platform_name_param (str): Platform name ('windows', 'darwin', 'linux')

    Returns:
        str: Lower case architecture string accepted by normalize_architecture()
    """
    if platform_name_param == "windows":
        # 'win-amd64' -> 'amd64', 'win-arm64' -> 'arm64', 'win32' -> 'win32'
        return sysconfig.get_platform().lower().removeprefix("win-")
    return platform.machine().lower()


def get_module_architecture(platform_name_param):
    """
    Get the architecture token used in the compiled ddbc_bindings filename.

    Args:
        platform_name_param (str): Platform name ('windows', 'darwin', 'linux')

    Returns:
        str: 'universal2' on macOS, otherwise the normalized architecture with
        the Windows x64 build renamed to 'amd64' to match the shipped binary
    """
    # Special handling for macOS universal2 binaries
    if platform_name_param == "darwin":
        return "universal2"

    architecture_name = normalize_architecture(
        platform_name_param, get_interpreter_architecture(platform_name_param)
    )

    # Handle Windows-specific naming for binary files
    if platform_name_param == "windows" and architecture_name == "x64":
        architecture_name = "amd64"
    return architecture_name


def find_module_path(module_dir_param, python_version_param, architecture_param, extension_param):
    """
    Find the compiled ddbc_bindings module file for the running interpreter.

    Args:
        module_dir_param (str): Directory that contains the compiled module
        python_version_param (str): Python version tag such as 'cp312'
        architecture_param (str): Architecture token from get_module_architecture()
        extension_param (str): File extension, '.pyd' on Windows and '.so' elsewhere

    Returns:
        str: Path of the exactly matching module file. If it does not exist, the
        first ddbc_bindings file with the right extension is returned instead
        and a RuntimeWarning is emitted.

    Raises:
        ImportError: If no ddbc_bindings file with the right extension exists
    """
    expected_module = f"ddbc_bindings.{python_version_param}-{architecture_param}{extension_param}"
    module_path_found = os.path.join(module_dir_param, expected_module)

    if os.path.exists(module_path_found):
        return module_path_found

    # Fallback to searching for any matching module if the specific one isn't found
    module_files = [
        f
        for f in os.listdir(module_dir_param)
        if f.startswith("ddbc_bindings.") and f.endswith(extension_param)
    ]
    if not module_files:
        raise ImportError(
            f"No ddbc_bindings module found for {python_version_param}-{architecture_param} "
            f"with extension {extension_param}"
        )
    warnings.warn(
        f"Using fallback module file {module_files[0]} instead of {expected_module}",
        RuntimeWarning,
    )
    return os.path.join(module_dir_param, module_files[0])


# Get current Python version and architecture
python_version = f"cp{sys.version_info.major}{sys.version_info.minor}"

platform_name = platform.system().lower()
architecture = get_module_architecture(platform_name)

# Validate supported platforms
if platform_name not in ["windows", "darwin", "linux"]:
    supported_platforms = ["windows", "darwin", "linux"]
    raise ImportError(
        f"Unsupported platform '{platform_name}' for mssql-python; expected one "
        f"of {supported_platforms}"
    )

# Determine extension based on platform
if platform_name == "windows":
    extension = ".pyd"
else:  # macOS or Linux
    extension = ".so"

# Find the specifically matching module file
module_dir = os.path.dirname(__file__)
module_path = find_module_path(module_dir, python_version, architecture, extension)


# Use the original module name 'ddbc_bindings' that the C extension was compiled with
module_name = "ddbc_bindings"
spec = importlib.util.spec_from_file_location(module_name, module_path)
module = importlib.util.module_from_spec(spec)
sys.modules[module_name] = module
spec.loader.exec_module(module)

# Copy all attributes from the loaded module to this module
for attr in dir(module):
    if not attr.startswith("__"):
        globals()[attr] = getattr(module, attr)
