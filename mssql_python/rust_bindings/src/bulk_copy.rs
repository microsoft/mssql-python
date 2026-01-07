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
    fn bulk_copy(
        &self,
        py: Python,
        table_name: String,
        data: PyObject,
    ) -> PyResult<PyObject> {
        let conn = self.connection.bind(py);
        let result = conn.call_method1("bulk_copy", (table_name, data))?;
        Ok(result.into())
    }

    fn __repr__(&self) -> String {
        "BulkCopyWrapper(mssql_core_tds)".to_string()
    }
}
