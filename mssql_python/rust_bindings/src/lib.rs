use pyo3::prelude::*;
use pyo3::types::PyDict;

/// A sample Rust-based connection class
#[pyclass]
struct RustConnection {
    connection_string: String,
    is_connected: bool,
}

#[pymethods]
impl RustConnection {
    #[new]
    fn new(connection_string: String) -> Self {
        RustConnection {
            connection_string,
            is_connected: false,
        }
    }

    fn connect(&mut self) -> PyResult<String> {
        self.is_connected = true;
        Ok(format!("Connected to: {}", self.connection_string))
    }

    fn disconnect(&mut self) -> PyResult<()> {
        self.is_connected = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.is_connected
    }

    fn __repr__(&self) -> String {
        format!(
            "RustConnection(connection_string='{}', connected={})",
            self.connection_string, self.is_connected
        )
    }
}

/// Example function that adds two numbers
#[pyfunction]
fn add_numbers(a: i64, b: i64) -> PyResult<i64> {
    Ok(a + b)
}

/// Example function that demonstrates string manipulation
#[pyfunction]
fn format_connection_string(host: String, database: String, user: String) -> PyResult<String> {
    Ok(format!(
        "Server={};Database={};User Id={};",
        host, database, user
    ))
}

/// Example function to show Python dict interaction
#[pyfunction]
fn parse_connection_params(py: Python, connection_string: String) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new_bound(py);
    
    // Simple parsing example
    for part in connection_string.split(';') {
        if let Some((key, value)) = part.split_once('=') {
            dict.set_item(key.trim(), value.trim())?;
        }
    }
    
    Ok(dict.unbind())
}

/// Version information
#[pyfunction]
fn rust_version() -> PyResult<String> {
    Ok(format!("Rust bindings v{} (PyO3)", env!("CARGO_PKG_VERSION")))
}

/// Python module definition
#[pymodule]
fn mssql_rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustConnection>()?;
    m.add_function(wrap_pyfunction!(add_numbers, m)?)?;
    m.add_function(wrap_pyfunction!(format_connection_string, m)?)?;
    m.add_function(wrap_pyfunction!(parse_connection_params, m)?)?;
    m.add_function(wrap_pyfunction!(rust_version, m)?)?;
    
    // Add module-level constants
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("__author__", "Microsoft SQL Server Python Team")?;
    
    Ok(())
}
