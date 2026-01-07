use pyo3::prelude::*;

/// BulkCopyWrapper - Wrapper around mssql_core_tds bulk copy API
/// 
/// This wrapper provides access to bulk copy operations using an existing
/// mssql_core_tds connection. Create it using from_connection() with a
/// DdbcConnection instance.
#[pyclass]
pub struct BulkCopyWrapper {
    connection: Py<PyAny>,
}

#[pymethods]
impl BulkCopyWrapper {
    /// Create BulkCopyWrapper from an existing mssql_core_tds.DdbcConnection
    /// 
    /// Args:
    ///     connection: An existing mssql_core_tds.DdbcConnection instance
    /// 
    /// Returns:
    ///     BulkCopyWrapper instance ready for bulk operations
    #[new]
    fn new(connection: PyObject) -> PyResult<Self> {
        Ok(BulkCopyWrapper { connection })
    }

    /// Perform bulk copy operation
    /// 
    /// Args:
    ///     table_name: Target table name for bulk copy
    ///     data: Data to copy (list of rows)
    /// 
    /// Returns:
    ///     Result from bulk_copy operation
    /// 
    /// Raises:
    ///     AttributeError: If bulk_copy method is not available on the connection
    ///     Exception: Any exception raised by the underlying bulk_copy implementation
    fn bulk_copy(
        &self,
        py: Python,
        table_name: String,
        data: PyObject,
    ) -> PyResult<PyObject> {
        let conn = self.connection.bind(py);
        
        // Check if bulk_copy method exists
        if !conn.hasattr("bulk_copy")? {
            return Err(pyo3::exceptions::PyAttributeError::new_err(
                "bulk_copy method not implemented in mssql_core_tds.DdbcConnection"
            ));
        }
        
        // Call bulk_copy and handle any exceptions
        match conn.call_method1("bulk_copy", (table_name.clone(), data)) {
            Ok(result) => Ok(result.into()),
            Err(e) => {
                // Re-raise the Python exception with additional context
                Err(pyo3::exceptions::PyRuntimeError::new_err(
                    format!("Bulk copy failed for table '{}': {}", table_name, e)
                ))
            }
        }
    }
}
