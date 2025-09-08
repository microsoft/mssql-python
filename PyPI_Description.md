# mssql-python

This is a new Python driver for Microsoft SQL Server currently in Public Preview phase.

## Public Preview Release

We are making progress - The Public Preview of our driver is now available! This marks a significant milestone in our development journey. While we saw a few early adopters of our public preview release, we are introducing the following functionalities to support your applications in a more robust and reliable manner.

### What's Included:

- Everything from previous releases
- **SUSE Linux Support:** Added full support for SUSE and openSUSE distributions alongside existing other Linux distros support, broadening enterprise Linux compatibility.
- **Context Manager Support:** Implemented Python `with` statement support for Connection and Cursor classes with automatic transaction management and resource cleanup.
- **Large Text Streaming:** Added Data At Execution (DAE) support for streaming large text parameters (`NVARCHAR(MAX)`, `VARCHAR(MAX)`), eliminating memory constraints for bulk text `execute()` operations.
  - `VARBINARY(MAX)` support to follow alongwith streaming support for fetch operations.
- **Enhanced Unicode Handling:** Improved emoji and international character support with robust UTF-16 encoding for reliable multilingual data processing.
- **PyODBC Compatibility:** Enhanced API compatibility with pyodbc including:
  - DB-API 2.0 exception classes: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
  - Context manager support with `with` statements for Connection and Cursor
  - Encoding configuration APIs: `setencoding()`, `getencoding()`, `setdecoding()`, `getdecoding()`
  - Cursor navigation APIs: `next()`, `__iter__()`, `scroll()`, `skip()`, `fetchval()`
  - Cursor attributes: `rownumber`, `messages`
  - Additional methods: `cursor.commit()`, `cursor.rollback()` `table()`

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python

If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.

### What's Next:

As we continue to develop and refine the driver, you can expect regular updates that will introduce new features, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver for the final release.

### Stay Tuned:

We appreciate your interest and support in this project. Stay tuned for more updates and enhancements as we work towards delivering a robust and fully-featured driver in coming months.
Thank you for being a part of our journey!