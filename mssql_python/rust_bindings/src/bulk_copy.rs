use pyo3::prelude::*;
use pyo3::types::PyDict;

/// BulkCopyWrapper - Wrapper around mssql_core_tds bulk copy API
/// 
/// This wrapper manages mssql_core_tds connections internally and provides
/// access to bulk copy operations.
#[pyclass]
pub struct BulkCopyWrapper {
    connection: Py<PyAny>,
}

#[pymethods]
impl BulkCopyWrapper {
    /// Create BulkCopyWrapper with connection parameters
    /// 
    /// Args:
    ///     params: Dictionary with connection parameters (server, database, user_name, password, etc.)
    /// 
    /// Returns:
    ///     BulkCopyWrapper instance ready for bulk operations
    /// 
    /// Raises:
    ///     ImportError: If mssql_core_tds module is not available
    ///     Exception: If connection creation fails
    #[new]
    fn new(py: Python, params: &Bound<'_, PyDict>) -> PyResult<Self> {
        // Import mssql_core_tds module
        let mssql_module = py.import_bound("mssql_core_tds")
            .map_err(|e| pyo3::exceptions::PyImportError::new_err(
                format!("Failed to import mssql_core_tds: {}", e)
            ))?;
        
        // Get DdbcConnection class
        let ddbc_conn_class = mssql_module.getattr("DdbcConnection")
            .map_err(|e| pyo3::exceptions::PyAttributeError::new_err(
                format!("Failed to get DdbcConnection class: {}", e)
            ))?;
        
        // Create connection instance
        let connection = ddbc_conn_class.call1((params,))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(
                format!("Failed to create DdbcConnection: {}", e)
            ))?;
        
        Ok(BulkCopyWrapper { 
            connection: connection.unbind()
        })
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
    
    /// Close the underlying connection
    /// 
    /// Raises:
    ///     Exception: If connection close fails
    fn close(&self, py: Python) -> PyResult<()> {
        let conn = self.connection.bind(py);
        conn.call_method0("close")?;
        Ok(())
    }
}
