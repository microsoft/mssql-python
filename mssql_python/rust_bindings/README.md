# Rust Bindings for mssql-python

This directory contains Rust-based Python bindings using PyO3, providing an alternative/complementary implementation to the C++ pybind11 bindings.

## Prerequisites

- **Rust**: Install from [rustup.rs](https://rustup.rs/)
- **Python**: 3.8 or higher
- **Maturin**: Python build tool for Rust extensions

```bash
pip install maturin
```

## Building

### Development Build (Fast, with debug symbols)

```bash
cd mssql_python/rust_bindings
maturin develop
```

This installs the extension in your current Python environment for testing.

### Release Build (Optimized)

```bash
cd mssql_python/rust_bindings
maturin build --release
```

This creates a wheel file in `target/wheels/`.

### Using Build Scripts

**Linux/macOS:**
```bash
chmod +x build.sh
./build.sh
```

**Windows:**
```cmd
build.bat
```

## Testing the Rust Module

After building with `maturin develop`, you can test it:

```python
import mssql_python.mssql_rust_bindings as rust

# Check version
print(rust.rust_version())

# Test functions
result = rust.add_numbers(10, 20)
print(f"10 + 20 = {result}")

# Test connection string formatting
conn_str = rust.format_connection_string(
    "localhost",
    "mydb",
    "myuser"
)
print(f"Connection string: {conn_str}")

# Test RustConnection class
conn = rust.RustConnection("Server=localhost;Database=test")
print(conn.connect())
print(f"Is connected: {conn.is_connected()}")
conn.disconnect()
```

## Module Structure

- `Cargo.toml` - Rust package configuration
- `src/lib.rs` - Main Rust code with PyO3 bindings
- `build.sh` / `build.bat` - Build scripts for different platforms

## Features

The module currently provides:

- **RustConnection**: A sample connection class
- **add_numbers()**: Simple addition function
- **format_connection_string()**: Connection string builder
- **parse_connection_params()**: Parse connection strings into dict
- **rust_version()**: Get module version info

## Integration with C++ Bindings

This Rust module works alongside the existing C++ `ddbc_bindings`. Both can coexist:

- C++ bindings: `ddbc_bindings` (existing)
- Rust bindings: `mssql_rust_bindings` (new)

You can use either or both in your Python code.

## Performance Considerations

- Rust provides memory safety without garbage collection
- Similar performance to C++ for most operations
- Excellent for concurrent operations with async support
- Zero-cost abstractions

## Future Development

Areas for expansion:
- Implement more database operations
- Add async/await support
- Create benchmarks vs C++ implementation
- Gradually migrate functionality from C++ to Rust
