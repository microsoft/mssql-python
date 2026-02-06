"""
CLI entry point for build_ddbc.

Usage:
    python -m build_ddbc              # Compile ddbc_bindings
    python -m build_ddbc --arch arm64 # Specify architecture (Windows)
    python -m build_ddbc --coverage   # Enable coverage (Linux)
    python -m build_ddbc --help       # Show help
"""

import argparse
import sys

from . import __version__
from .compiler import compile_ddbc, get_platform_info


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="python -m build_ddbc",
        description="Compile ddbc_bindings native extension for mssql-python",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m build_ddbc              # Build for current platform
    python -m build_ddbc --arch arm64 # Build for ARM64 (Windows)
    python -m build_ddbc --coverage   # Build with coverage (Linux)
    python -m build_ddbc --quiet      # Build without output
        """,
    )

    parser.add_argument(
        "--arch", "-a",
        choices=["x64", "x86", "arm64", "x86_64", "aarch64", "universal2"],
        help="Target architecture (Windows: x64, x86, arm64)",
    )

    parser.add_argument(
        "--coverage", "-c",
        action="store_true",
        help="Enable coverage instrumentation (Linux only)",
    )

    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress build output",
    )

    parser.add_argument(
        "--version", "-V",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    args = parser.parse_args()

    # Show platform info
    if not args.quiet:
        arch, platform_tag = get_platform_info()
        print(f"[build_ddbc] Platform: {sys.platform}")
        print(f"[build_ddbc] Architecture: {arch}")
        print(f"[build_ddbc] Platform tag: {platform_tag}")
        print()

    try:
        compile_ddbc(
            arch=args.arch,
            coverage=args.coverage,
            verbose=not args.quiet,
        )
        return 0
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except RuntimeError as e:
        print(f"Build failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
