"""
PEP 517 Build Backend for mssql-python.

This module wraps setuptools' build backend and adds automatic
ddbc_bindings compilation before building wheels.

Usage in pyproject.toml:
    [build-system]
    requires = ["setuptools>=61.0", "wheel", "pybind11"]
    build-backend = "build_ddbc.build_backend"
    backend-path = ["."]
"""

import sys
from pathlib import Path

# Import setuptools build backend - we'll wrap its functions
from setuptools.build_meta import (
    build_wheel as _setuptools_build_wheel,
    build_sdist as _setuptools_build_sdist,
    get_requires_for_build_wheel as _get_requires_for_build_wheel,
    get_requires_for_build_sdist as _get_requires_for_build_sdist,
    prepare_metadata_for_build_wheel as _prepare_metadata_for_build_wheel,
)

from .compiler import compile_ddbc


# =============================================================================
# PEP 517 Required Hooks
# =============================================================================

def get_requires_for_build_wheel(config_settings=None):
    """Return build requirements for wheel."""
    return _get_requires_for_build_wheel(config_settings)


def get_requires_for_build_sdist(config_settings=None):
    """Return build requirements for sdist."""
    return _get_requires_for_build_sdist(config_settings)


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    """Prepare wheel metadata."""
    return _prepare_metadata_for_build_wheel(metadata_directory, config_settings)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    """
    Build a wheel, compiling ddbc_bindings first.

    This is the main hook - it compiles the native extension before
    delegating to setuptools to create the wheel.
    """
    print("[build_backend] Starting wheel build...")

    # Check if we should skip compilation (e.g., for sdist-only builds)
    skip_compile = False
    if config_settings:
        skip_compile = config_settings.get("--skip-ddbc-compile", False)

    if not skip_compile:
        # Extract build options from config_settings
        arch = None
        coverage = False

        if config_settings:
            arch = config_settings.get("--arch")
            coverage = config_settings.get("--coverage", False)

        print("[build_backend] Compiling ddbc_bindings...")
        try:
            compile_ddbc(arch=arch, coverage=coverage, verbose=True)
            print("[build_backend] Compilation successful!")
        except FileNotFoundError:
            # If build scripts don't exist, assume pre-compiled binaries
            print("[build_backend] Build scripts not found, assuming pre-compiled binaries")
        except RuntimeError as e:
            print(f"[build_backend] Compilation failed: {e}")
            raise
    else:
        print("[build_backend] Skipping ddbc compilation (--skip-ddbc-compile)")

    # Now build the wheel using setuptools
    print("[build_backend] Creating wheel...")
    return _setuptools_build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    """
    Build a source distribution.

    For sdist, we don't compile - just package the source including build scripts.
    """
    print("[build_backend] Building source distribution...")
    return _setuptools_build_sdist(sdist_directory, config_settings)


# =============================================================================
# Optional PEP 660 Hooks (Editable Installs)
# =============================================================================

def get_requires_for_build_editable(config_settings=None):
    """Return build requirements for editable install."""
    return get_requires_for_build_wheel(config_settings)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    """
    Build an editable wheel, compiling ddbc_bindings first.

    This enables `pip install -e .` to automatically compile.
    """
    print("[build_backend] Starting editable install...")

    # Compile ddbc_bindings for editable installs too
    print("[build_backend] Compiling ddbc_bindings for editable install...")
    try:
        compile_ddbc(verbose=True)
        print("[build_backend] Compilation successful!")
    except FileNotFoundError:
        print("[build_backend] Build scripts not found, assuming pre-compiled binaries")
    except RuntimeError as e:
        print(f"[build_backend] Compilation failed: {e}")
        raise

    # Import here to avoid issues if not available
    from setuptools.build_meta import build_editable as _setuptools_build_editable
    return _setuptools_build_editable(wheel_directory, config_settings, metadata_directory)
