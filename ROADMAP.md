# Roadmap for Python Driver for SQL Server

We are thrilled to introduce Python driver for SQL Server (Public Preview) – a modern, high performant, and developer-friendly SDK designed to enhance your SQL Server database connectivity experience. This roadmap outlines the key structural improvements, new features and upcoming enhancements that will set our driver apart from existing solutions.

Why a New Driver?

Unlike existing Python SQL Server drivers, we are making substantial improvements to performance, maintainability, and usability by re-architecting the core internals. Our focus is on seamless integration between Python and C++, efficient memory management, better state handling, and advanced DBAPI enhancements.

Here’s what’s coming:

**1. Structural changes for abstraction of C++ and Python codebase**

We are undertaking significant structural changes to provide a clear abstraction between C++ code and Python. This will ensure better maintainability, improved performance, and a cleaner codebase. By leveraging existing pybind11 module, we aim to create a seamless integration between the two languages, allowing for efficient execution and easier debugging.

This will improve:
- Maintainability via simplified modular architecture
- Performance via optimized C++ code
- Debugging, traceability and seamless interaction between C++ and Python via with PyBind11 module integration

**2. Future DBAPI Enhancements**

In future releases, we plan to add several DBAPI enhancements, including:
- `Callproc()` : Support for calling stored procedures.   
- `setinputsize()` and `setoutputsize()`
- `Output` and `InputOutput` Parameters: Handling of output and input-output parameters in stored procedures.
- Optional DBAPIs: Additional optional DBAPI features to provide more flexibility and functionality for developers.

**3. Cross-Platform Support: Additional Linux Distributions** 

We are committed to providing cross-platform support for our Python driver. In the next few weeks, we will release support for additional Linux distributions viz Alpine, SUSE Linux & Oracle Linux.

**4. Bulk Copy (BCP)**

Bulk Copy API (BCP) support is coming soon to the Python Driver for SQL Server. It enables high-speed data ingestion and offers fine-grained control over batch operations, making it ideal for large-scale ETL workflows.

**5. Asynchronous Query Execution**

We are also working on adding support for asynchronous query execution. This feature will allow developers to execute queries without blocking the main thread, enabling more responsive and efficient applications. Asynchronous query execution will be particularly beneficial for applications that require high concurrency and low latency.
- No blocking of the main thread
- Faster parallel processing – ideal for high-concurrency applications
- Better integration with async frameworks like asyncio

We are dedicated to continuously improving the Python driver for SQL Server and welcome feedback from the community. Stay tuned for updates and new features as we work towards delivering a high-quality driver that meets your needs.
Join the Conversation!

We are building this for developers, with developers. Your feedback will shape the future of the driver.
- Follow our [Github Repo](https://github.com/microsoft/mssql-python)
- Join Discussions – Share your ideas and suggestions
- Try our alpha release – Help us refine and optimize the experience

Stay tuned for more updates, and lets build something amazing together. Watch this space for announcements and release timelines.
