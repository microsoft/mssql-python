# mssql-python

This is a new Python driver for Microsoft SQL Server currently in Alpha phase.

## Public Preview Release

We are making progress - The Public Preview of our driver is now available! This marks a significant milestone in our development journey. While we saw a few early adopters of our alpha release, we are introducing the following functionalities to support your applications in a more robust and reliable manner.

### What's Included:

- Everything from previous releases
- **Azure Active Directory Authentication:** New authentication module supporting Azure AD login options (ActiveDirectoryInteractive, ActiveDirectoryDeviceCode, ActiveDirectoryDefault) for secure and flexible cloud integration.
- **Batch Execution Performance:** Refactored `executemany` for efficient bulk operations and improved C++ bindings for performance.
- **Robust Logging System:** Overhauled logging with a singleton manager, sensitive data sanitization, and better exception handling.
- **Improved Row Representation:** Enhanced output and debugging via updated `Row` object string and representation methods.

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python

### What's Next:

As we continue to develop and refine the driver, you can expect regular updates that will introduce new features, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver for the final release.

### Stay Tuned:

We appreciate your interest and support in this project. Stay tuned for more updates and enhancements as we work towards delivering a robust and fully-featured driver in coming months.
Thank you for being a part of our journey!