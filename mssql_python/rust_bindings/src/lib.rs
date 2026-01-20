use pyo3::prelude::*;

// Import the bulk_copy module
mod bulk_copy;
use bulk_copy::BulkCopyWrapper;

/// Python module definition for mssql-python Rust bindings
#[pymodule]
fn mssql_rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Bulk copy wrapper class
    m.add_class::<BulkCopyWrapper>()?;
    
    Ok(())
}
