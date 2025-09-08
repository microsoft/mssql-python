# mssql-python

This is a new Python driver for Microsoft SQL Server currently in Public Preview phase.

## Public Preview Release

We are making progress - The Public Preview of our driver is now available! This marks a significant milestone in our development journey. While we saw a few early adopters of our alpha release, we are introducing the following functionalities to support your applications in a more robust and reliable manner.

### What's Included:

- Everything from previous releases
- **SUSE Linux Support:** Added full support for SUSE and openSUSE distributions alongside existing Alpine Linux support, broadening enterprise Linux compatibility.
- **Context Manager Support:** Implemented Python `with` statement support for Connection and Cursor classes with automatic transaction management and resource cleanup.
- **Large Data Streaming:** Added Data At Execution (DAE) support for streaming large text and binary parameters, eliminating memory constraints for bulk operations.
- **Enhanced Unicode Handling:** Improved emoji and international character support with robust UTF-16 encoding for reliable multilingual data processing.
- **DB-API 2.0 Compliance:** Added standard exception classes and improved API consistency for seamless migration from other Python database drivers.

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python

### What's Next:

As we continue to develop and refine the driver, you can expect regular updates that will introduce new features, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver for the final release.

### Stay Tuned:

We appreciate your interest and support in this project. Stay tuned for more updates and enhancements as we work towards delivering a robust and fully-featured driver in coming months.
Thank you for being a part of our journey!