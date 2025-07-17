# mssql-python

A modern Python driver for Microsoft SQL Server – now in Public Preview!

## Public Preview Release

We’re excited to announce the Public Preview of mssql-python, marking a major milestone in our development journey. Building on the interest from our alpha release, this version delivers new features and improvements to support your applications with greater reliability and flexibility.

### What's Included:

- Everything from previous releases
- **Azure Active Directory Authentication:** New authentication module supporting Azure AD login options (ActiveDirectoryInteractive, ActiveDirectoryDeviceCode, ActiveDirectoryDefault) for secure and flexible cloud integration.
- **Batch Execution Performance:** Refactored `executemany` for efficient bulk operations and improved C++ bindings for performance.
- **Robust Logging System:** Overhauled logging with a singleton manager, sensitive data sanitization, and better exception handling.
- **Improved Row Representation:** Enhanced output and debugging via updated `Row` object string and representation methods.

For more details and documentation, visit the project on [GitHub](https://github.com/microsoft/mssql-python).

### What's Next:

We’re committed to continuous improvement. Expect regular updates with new features, optimizations, and bug fixes. Your feedback, contributions, and issue reports are invaluable in shaping the final release.

### Stay Tuned:

Thank you for your interest and support! Stay tuned for further enhancements as we work towards a robust, fully-featured Python driver for SQL Server.