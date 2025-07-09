"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides helper functions for the mssql_python package.
"""

from mssql_python import ddbc_bindings
from mssql_python.exceptions import raise_exception
from mssql_python.logging_config import get_logger, ENABLE_LOGGING
import platform
from pathlib import Path
from mssql_python.ddbc_bindings import normalize_architecture

logger = get_logger()


def add_driver_to_connection_str(connection_str):
    """
    Add the DDBC driver to the connection string if not present.

    Args:
        connection_str (str): The original connection string.

    Returns:
        Union[str, Tuple[str, dict]]: Either the connection string with driver added,
        or a tuple of (connection string, attrs_before dict)
    """
    driver_name = "Driver={ODBC Driver 18 for SQL Server}"
    try:
        # Strip any leading or trailing whitespace from the connection string
        connection_str = connection_str.strip()
        result = add_driver_name_to_app_parameter(connection_str)
        
        # Handle both regular string and tuple return types
        attrs_before = None
        if isinstance(result, tuple):
            connection_str, attrs_before = result
        else:
            connection_str = result

        # Split the connection string into individual attributes
        connection_attributes = connection_str.split(";")
        final_connection_attributes = []

        # Iterate through the attributes and exclude any existing driver attribute
        for attribute in connection_attributes:
            if attribute.lower().split("=")[0] == "driver":
                continue
            final_connection_attributes.append(attribute)

        # Join the remaining attributes back into a connection string
        connection_str = ";".join(final_connection_attributes)

        # Insert the driver attribute at the beginning of the connection string
        final_connection_attributes.insert(0, driver_name)
        connection_str = ";".join(final_connection_attributes)

        if attrs_before:
            return connection_str, attrs_before
        return connection_str

    except Exception as e:
        raise Exception(
            "Invalid connection string, Please follow the format: "
            "Server=server_name;Database=database_name;UID=user_name;PWD=password"
        ) from e

def check_error(handle_type, handle, ret):
    """
    Check for errors and raise an exception if an error is found.

    Args:
        handle_type: The type of the handle (e.g., SQL_HANDLE_ENV, SQL_HANDLE_DBC).
        handle: The SqlHandle object associated with the operation.
        ret: The return code from the DDBC function call.

    Raises:
        RuntimeError: If an error is found.
    """
    if ret < 0:
        error_info = ddbc_bindings.DDBCSQLCheckError(handle_type, handle, ret)
        if ENABLE_LOGGING:
            logger.error("Error: %s", error_info.ddbcErrorMsg)
        raise_exception(error_info.sqlState, error_info.ddbcErrorMsg)


def add_driver_name_to_app_parameter(connection_string):
    """
    Modifies the input connection string by appending the APP name and handling AAD auth.

    Args:
        connection_string (str): The input connection string.

    Returns:
        Union[str, Tuple[str, bytes]]: Either the modified connection string, 
        or a tuple of (connection string, token bytes) if AAD auth is needed
    """
    import sys

    # Split the input string into key-value pairs
    parameters = connection_string.split(";")

    # Initialize variables
    app_found = False
    modified_parameters = []
    has_aad_interactive = False

    # Iterate through the key-value pairs
    for param in parameters:
        param = param.strip()
        if not param:
            continue
            
        if sys.platform.startswith("win"):
            _, auth_value = param.split("=", 1)
            if auth_value.lower() == "activedirectoryinteractive":
                has_aad_interactive = True
                # Only keep the auth parameter on Windows
                if platform.system().lower() == "windows":
                    modified_parameters.append(param)
                continue
        elif param.lower().startswith("app="):
            app_found = True
            key, _ = param.split("=", 1)
            modified_parameters.append(f"{key}=MSSQL-Python")
        else:
            modified_parameters.append(param)

    # If APP key is not found, append it
    if not app_found:
        modified_parameters.append("APP=MSSQL-Python")

    # Handle AAD Interactive auth for non-Windows platforms
    if has_aad_interactive and platform.system().lower() != "windows":
        try:
            from azure.identity import InteractiveBrowserCredential
            import struct
        except ImportError:
            raise ImportError("Please install azure-identity: pip install azure-identity")

        credential = InteractiveBrowserCredential()
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        return ";".join(modified_parameters) + ";", {1256: token_struct}

    return ";".join(modified_parameters) + ";"


def detect_linux_distro():
    """
    Detect Linux distribution for driver path selection.

    Returns:
        str: Distribution name ('debian_ubuntu', 'rhel', 'alpine', etc.)
    """
    import os

    distro_name = "debian_ubuntu"  # default

    try:
        if os.path.exists("/etc/os-release"):
            with open("/etc/os-release", "r") as f:
                content = f.read()
            for line in content.split("\n"):
                if line.startswith("ID="):
                    distro_id = line.split("=", 1)[1].strip('"\'')
                    if distro_id in ["ubuntu", "debian"]:
                        distro_name = "debian_ubuntu"
                    elif distro_id in ["rhel", "centos", "fedora"]:
                        distro_name = "rhel"
                    elif distro_id == "alpine":
                        distro_name = "alpine"
                    else:
                        distro_name = distro_id  # use as-is
                    break
    except Exception:
        pass  # use default

    return distro_name

def get_driver_path(module_dir, architecture):
    """
    Get the platform-specific ODBC driver path.

    Args:
        module_dir (str): Base module directory
        architecture (str): Target architecture (x64, arm64, x86, etc.)

    Returns:
        str: Full path to the ODBC driver file

    Raises:
        RuntimeError: If driver not found or unsupported platform
    """

    platform_name = platform.system().lower()
    normalized_arch = normalize_architecture(platform_name, architecture)

    if platform_name == "windows":
        driver_path = Path(module_dir) / "libs" / "windows" / normalized_arch / "msodbcsql18.dll"

    elif platform_name == "darwin":
        driver_path = Path(module_dir) / "libs" / "macos" / normalized_arch / "lib" / "libmsodbcsql.18.dylib"

    elif platform_name == "linux":
        distro_name = detect_linux_distro()
        driver_path = Path(module_dir) / "libs" / "linux" / distro_name / normalized_arch / "lib" / "libmsodbcsql-18.5.so.1.1"

    else:
        raise RuntimeError(f"Unsupported platform: {platform_name}")

    driver_path_str = str(driver_path)

    # Check if file exists
    if not driver_path.exists():
        raise RuntimeError(f"ODBC driver not found at: {driver_path_str}")

    return driver_path_str
