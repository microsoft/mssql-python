"""
Example usage of the Rust bindings for mssql-python.

This script demonstrates how to use the Rust-based PyO3 bindings
alongside the existing C++ bindings.
"""

try:
    import mssql_rust_bindings as rust
    print("✓ Rust bindings loaded successfully!")
    print(f"  Version: {rust.rust_version()}")
    print()
except ImportError as e:
    print(f"✗ Could not load Rust bindings: {e}")
    print("  Run 'maturin build --release' in mssql_python/rust_bindings/ to build them.")
    print("  Then install with: pip install target/wheels/*.whl")
    exit(1)


def test_basic_functions():
    """Test basic Rust functions."""
    print("Testing basic functions:")
    print("-" * 50)
    
    # Test addition
    result = rust.add_numbers(42, 58)
    print(f"add_numbers(42, 58) = {result}")
    assert result == 100, "Addition failed!"
    
    # Test connection string formatting
    conn_str = rust.format_connection_string(
        "localhost",
        "TestDB",
        "sa"
    )
    print(f"format_connection_string() = {conn_str}")
    
    # Test connection string parsing
    params = rust.parse_connection_params(conn_str)
    print(f"parse_connection_params() = {params}")
    
    print("✓ All basic functions passed!\n")


def test_rust_connection():
    """Test the RustConnection class."""
    print("Testing RustConnection class:")
    print("-" * 50)
    
    # Create a connection
    conn = rust.RustConnection("Server=localhost;Database=TestDB;User Id=sa")
    print(f"Created: {conn}")
    
    # Check initial state
    print(f"Is connected (initial): {conn.is_connected()}")
    assert not conn.is_connected(), "Should not be connected initially"
    
    # Connect
    message = conn.connect()
    print(f"Connect message: {message}")
    print(f"Is connected (after connect): {conn.is_connected()}")
    assert conn.is_connected(), "Should be connected after connect()"
    
    # Disconnect
    conn.disconnect()
    print(f"Is connected (after disconnect): {conn.is_connected()}")
    assert not conn.is_connected(), "Should not be connected after disconnect()"
    
    print("✓ RustConnection tests passed!\n")


def main():
    """Run all tests."""
    print("=" * 50)
    print("Rust Bindings Test Suite")
    print("=" * 50)
    print()
    
    test_basic_functions()
    test_rust_connection()
    
    print("=" * 50)
    print("✓ All tests passed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    main()
