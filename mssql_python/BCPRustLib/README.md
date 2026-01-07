# BCPRust Library Binaries

This directory contains the compiled binaries from the mssql-tds Rust project.

## Contents

### Python Bindings
- **libmssql_core_tds.so** - Core TDS protocol Python extension (PyO3)
- **mssql_py_core-*.whl** - Python wheel package for easy installation

### JavaScript Bindings
- **libmssql_js.so** - Node.js native addon for TDS protocol

### CLI Tool
- **mssql-tds-cli** - Command-line interface for testing TDS connections

## Usage

### Installing Python Bindings

```bash
pip install --break-system-packages mssql_py_core-0.1.0-cp312-cp312-linux_x86_64.whl
```

Or for development:
```bash
python3 -c "import sys; sys.path.insert(0, '.'); import libmssql_core_tds"
```

### Testing the CLI

```bash
./mssql-tds-cli --help
```

## Source

Built from: `~/BCPRust/mssql-tds`

## Build Commands Used

```bash
# Main workspace
cd ~/BCPRust/mssql-tds
cargo build --release

# Python bindings
cd ~/BCPRust/mssql-tds/mssql-py-core
maturin build --release --skip-auditwheel
```

## File Sizes

- libmssql_core_tds.so: ~2.6 MB
- libmssql_js.so: ~3.3 MB
- mssql-tds-cli: ~2.0 MB
- Python wheel: ~978 KB

## Notes

- These are release builds with optimizations enabled
- Symbols are stripped for smaller file sizes
- Built for Linux x86_64 platform
- Python bindings are for CPython 3.12
