# Debug Symbols Explained: What They Are and How PublishSymbols Works

## What Are Symbols? 🔍

**Symbols** (or **debug symbols**) are special files that contain debugging information for compiled binaries. They act as a "map" between your compiled machine code and your original source code.

### Platform-Specific Symbol Files

| Platform | Symbol File Type | Example |
|----------|-----------------|---------|
| Windows | `.pdb` (Program Database) | `mssql_python.pdb` |
| Linux | `.debug` or embedded in `.so` | `mssql_python.so.debug` |
| macOS | `.dSYM` bundle or embedded | `mssql_python.so.dSYM/` |

For **mssql-python**, we primarily care about Windows `.pdb` files since our C++ extensions compile to:
- Windows: `mssql_python.pyd` + `mssql_python.pdb`
- Linux: `mssql_python.so` (with embedded debug info)
- macOS: `mssql_python.so` (with embedded debug info or separate `.dSYM`)

## What's Inside a Symbol File? 📊

Symbol files contain:

### 1. **Source Code Mapping**
```
Machine Code Address    →    Source Location
==================           ================
0x00401000              →    connection.cpp, line 42
0x00401010              →    connection.cpp, line 43
0x00401234              →    cursor.cpp, line 156
```

### 2. **Function Names**
```
Address      →    Function Name
========          =============
0x00401000   →    Connection::execute()
0x00401234   →    Cursor::fetchall()
0x00405678   →    handle_sql_error()
```

### 3. **Variable Names and Types**
```
Memory Location    →    Variable Info
===============         =============
EBP-0x10          →    int rowCount
EBP-0x20          →    std::string queryText
EBP-0x30          →    Connection* conn
```

### 4. **Call Stack Information**
```
Who called whom and in what order
```

## Why Symbols Matter 🎯

### Without Symbols: ❌
```python
Traceback (most recent call last):
  File "app.py", line 10, in <module>
    cursor.execute("SELECT * FROM users")
RuntimeError: Access violation at address 0x00401234

Stack trace:
  mssql_python.pyd!0x00401234
  mssql_python.pyd!0x00405000
  python39.dll!0x1c002340
```
**Useless for debugging!** Just memory addresses.

### With Symbols: ✅
```python
Traceback (most recent call last):
  File "app.py", line 10, in <module>
    cursor.execute("SELECT * FROM users")
RuntimeError: Null pointer dereference in Connection::execute()
  at connection.cpp:42 (mssql-python v2.0.1)

Stack trace:
  mssql_python.pyd!Connection::execute() Line 42
    C:\mssql-python\mssql_python\connection.cpp
  mssql_python.pyd!Cursor::execute_query() Line 156
    C:\mssql-python\mssql_python\cursor.cpp
  python39.dll!PyObject_Call() Line 123
```
**Actionable!** You know exactly where the bug is.

## Real-World Use Cases 🌍

### 1. **Customer Crash Reports**
When a customer reports a crash, you get a **minidump** file:
```
crash_report_20251008_143022.dmp
```

With symbols published to a symbol server, you can:
```bash
# WinDbg automatically downloads symbols
windbg -z crash_report_20251008_143022.dmp

# Shows exact function and line number
0:000> !analyze -v
FAULTING_IP: 
mssql_python!Connection::execute+0x234
00007ff8`12340234 mov     rax,qword ptr [rcx]  ← Null pointer!

SOURCE_FILE:  C:\code\mssql_python\connection.cpp
SOURCE_LINE:  42
```

### 2. **Performance Profiling**
Profilers show function names instead of addresses:
```
Top Functions by CPU Time:
1. Cursor::fetchall()           45.3%  ← Can optimize this!
2. Connection::execute()        23.1%
3. ResultSet::parse_rows()      15.7%

vs without symbols:
1. 0x00401234                   45.3%  ← No idea what this is
2. 0x00405000                   23.1%
3. 0x00408888                   15.7%
```

### 3. **Memory Leak Detection**
Tools like Application Insights or Visual Studio can show:
```
Memory Leak Detected:
  Allocated in: Connection::open() at connection.cpp:78
  Never freed, leaked 1,024 KB
```

### 4. **Stack Traces in Production**
When exceptions occur in production:
```python
# Windows Error Reporting (WER) captures the crash
# Uploads minidump to your symbol server
# You get detailed crash reports with full call stacks
```

## How PublishSymbols@2 Task Works 🔧

### Current Configuration (build-release-package-pipeline.yml)

```yaml
- stage: PublishSymbols
  displayName: 'Publish Debug Symbols'
  dependsOn: Build
  condition: succeeded()
  jobs:
    - job: PublishSymbolsJob
      displayName: 'Publish Symbols to Symbol Server'
      pool:
        type: windows
      steps:
        - task: PublishSymbols@2
          displayName: 'Publish Symbols'
          inputs:
            SymbolsFolder: '$(ob_outputDirectory)/symbols'
            SearchPattern: '**/*.pdb'
            IndexSources: false
            PublishSymbols: true
            SymbolServerType: 'TeamServices'
            SymbolsArtifactName: 'mssql-python-symbols'
```

### What Each Parameter Does:

#### `SymbolsFolder: '$(ob_outputDirectory)/symbols'`
- **What**: Directory containing symbol files
- **For mssql-python**: Should point to where `.pdb` files are copied during Windows builds
- **Example**: `$(Build.ArtifactStagingDirectory)/symbols/`

#### `SearchPattern: '**/*.pdb'`
- **What**: Glob pattern to find symbol files
- **`**/*.pdb`**: Recursively find all `.pdb` files
- **Can use**: `**/*.pdb;**/*.debug` for multiple types

#### `IndexSources: false`
- **What**: Whether to index source code locations
- **`true`**: Embeds source file paths in symbols (good for internal debugging)
- **`false`**: No source paths (better for public releases)
- **Recommendation**: `false` for public releases, `true` for internal

#### `PublishSymbols: true`
- **What**: Actually upload to symbol server vs just indexing
- **`true`**: Upload to Azure Artifacts symbol server
- **`false`**: Index only, don't upload (ODBC does this)

#### `SymbolServerType: 'TeamServices'`
- **What**: Where to publish symbols
- **`TeamServices`**: Azure DevOps Artifacts (Azure Artifacts symbol server)
- **`FileShare`**: Network file share (legacy)
- **Recommendation**: Use `TeamServices` for Azure DevOps

#### `SymbolsArtifactName: 'mssql-python-symbols'`
- **What**: Name for the published symbols artifact
- **Used by**: Debugging tools to locate symbols

## Symbol Server Workflow 🔄

### 1. Build Phase
```
Windows Build Job
├── Compile C++ → mssql_python.pyd + mssql_python.pdb
├── Copy .pyd to $(ob_outputDirectory)/bindings/Windows/
└── Copy .pdb to $(ob_outputDirectory)/symbols/
```

### 2. Publish Phase
```
PublishSymbols Task
├── Scan $(ob_outputDirectory)/symbols/ for *.pdb
├── Extract symbol metadata (GUID, Age, Hash)
├── Upload to Azure Artifacts Symbol Server
└── Store with unique identifier
```

### 3. Debugging Phase
```
Customer's Machine
├── App crashes with mssql_python.pyd
├── WinDbg captures crash dump
├── WinDbg looks for mssql_python.pdb
│   └── Checks symbol path: SRV*C:\symbols*https://artifacts.dev.azure.com/...
├── Downloads matching .pdb from Azure Artifacts
└── Shows full stack trace with source lines
```

## Symbol Server URL Structure 📍

Azure Artifacts stores symbols like this:
```
https://artifacts.dev.azure.com/{organization}/_apis/symbol/symsrv/
  mssql_python.pdb/
    {GUID}{Age}/
      mssql_python.pdb

Example:
https://artifacts.dev.azure.com/microsoft/_apis/symbol/symsrv/
  mssql_python.pdb/
    A1B2C3D4E5F6012345678901234567891/
      mssql_python.pdb
```

The `{GUID}{Age}` uniquely identifies each build, so multiple versions coexist.

## How Debuggers Use Symbols 🛠️

### WinDbg Example
```bash
# Set symbol path to include Azure Artifacts
.sympath SRV*C:\symbols*https://artifacts.dev.azure.com/microsoft/_apis/symbol/symsrv

# Load crash dump
windbg -z crash.dmp

# WinDbg automatically:
# 1. Reads mssql_python.pyd metadata from the dump
# 2. Extracts GUID/Age identifier
# 3. Downloads matching .pdb from symbol server
# 4. Maps addresses to source code

# Now you can debug with full context
!analyze -v          # Automatic crash analysis with source lines
k                    # Stack trace with function names
lm                   # List modules with symbol status
```

### Visual Studio Example
```
Visual Studio Debugger
├── Options → Debugging → Symbols
├── Add symbol server: https://artifacts.dev.azure.com/...
├── Open crash dump: crash_report.dmp
├── Debug → Start Debugging
└── See full source code and variables!
```

### Python Traceback Enhancement
Some Python crash handlers (like `faulthandler` or Windows Error Reporting) can include native stack traces:
```python
import faulthandler
faulthandler.enable()

# If mssql_python crashes, you get:
Fatal Python error: Segmentation fault

Current thread 0x00001234 (most recent call first):
  File "app.py", line 10 in <module>
    cursor.execute("SELECT * FROM users")
  
Native stack trace (if symbols available):
  mssql_python.pyd!Connection::execute() at connection.cpp:42
  mssql_python.pyd!Cursor::execute_query() at cursor.cpp:156
```

## ODBC vs SQL Client Symbol Handling 📚

### ODBC Approach
```yaml
- task: PublishSymbols@2
  displayName: 'Index symbols'
  inputs:
    SymbolsFolder: $(Build.SourcesDirectory)\retail\Symbols.pri
    SearchPattern: '**/*.pdb'
    IndexSources: true          # ← Index source paths
    PublishSymbols: false       # ← Don't publish, just index
    DetailedLog: true
    SymbolServerType: FileShare

- task: CopyFiles@2
  displayName: 'Copy Private Symbols Files to Artifact'
  inputs:
    SourceFolder: '$(System.DefaultWorkingDirectory)\retail\Symbols.pri'
    Contents: '**\*.pdb'
    TargetFolder: '$(Build.ArtifactStagingDirectory)/Symbols.private'
```

**ODBC Strategy**:
- Index symbols but don't publish to symbol server
- Copy `.pdb` files to artifacts for manual distribution
- Keeps symbols private (not on public symbol server)

### SQL Client Approach
```yaml
symbolsFolder: '${{ variables.apiScanPdbPath }}'
publishSymbols: '${{ parameters.publishSymbols }}'
symbolsAzureSubscription: '$(SymbolsAzureSubscription)'
symbolsPublishProjectName: '$(SymbolsPublishProjectName)'
symbolsPublishServer: '$(SymbolsPublishServer)'
```

**SQL Client Strategy**:
- Publish to Azure Artifacts symbol server
- Use parameterized configuration
- Separate symbol publishing from build

## Best Practices for mssql-python 🎯

### 1. **Where to Generate Symbols**
```yaml
# Windows builds - compile with debug info
jobs/build-windows-job.yml:
  - script: |
      # CMake: Generate .pdb files
      cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ...
      # OR for Visual Studio
      msbuild /p:Configuration=Release /p:DebugSymbols=true
    
  - task: CopyFiles@2
    inputs:
      SourceFolder: '$(Build.SourcesDirectory)/build'
      Contents: '**/*.pdb'
      TargetFolder: '$(ob_outputDirectory)/symbols'
```

### 2. **What to Publish**
```yaml
# Publish symbols for all platforms
SymbolsFolder: '$(ob_outputDirectory)/symbols'
SearchPattern: |
  **/*.pdb           # Windows
  **/*.debug         # Linux (if separated)
  **/*.dSYM/**/*     # macOS (if separated)
```

### 3. **When to Publish**
```yaml
# Only publish symbols for Official builds
- ${{ if and(eq(parameters.oneBranchType, 'Official'), 
             eq(parameters.publishSymbols, true)) }}:
    - stage: PublishSymbols
```

### 4. **Symbol Retention**
- Azure Artifacts keeps symbols **indefinitely** by default
- Symbols are small (~1-10 MB per build)
- Keep symbols for all released versions
- Clean up symbols for internal/test builds after 90 days

### 5. **Public vs Private Symbols**

**Public Symbols** (Strip source info):
```yaml
IndexSources: false       # Don't embed source paths
PublishSymbols: true      # Upload to public symbol server
```

**Private Symbols** (Full debug info):
```yaml
IndexSources: true        # Embed source paths
PublishSymbols: false     # Keep in artifacts, don't publish
# Copy to internal symbol server or artifact storage
```

## Current mssql-python Configuration Analysis 🔬

### What We Have
```yaml
# build-release-package-pipeline.yml
- stage: PublishSymbols
  displayName: 'Publish Debug Symbols'
  dependsOn: Build
  condition: succeeded()
  jobs:
    - job: PublishSymbolsJob
      pool:
        type: windows    # ← Need Windows pool for symbol indexing
      steps:
        - task: PublishSymbols@2
          inputs:
            SymbolsFolder: '$(ob_outputDirectory)/symbols'
            SearchPattern: '**/*.pdb'
            IndexSources: false      # ← Good: No source paths
            PublishSymbols: true     # ← Will upload to Azure Artifacts
            SymbolServerType: 'TeamServices'
            SymbolsArtifactName: 'mssql-python-symbols'
```

### What's Missing
1. **Symbol Collection in Build Jobs**
   - Windows job needs to copy `.pdb` files to `$(ob_outputDirectory)/symbols`
   - Currently might only be copying binaries

2. **Build Configuration**
   - Need to ensure C++ compilation generates `.pdb` files
   - Use `/Zi` or `/Z7` compiler flag (Visual Studio)
   - Use `-g` compiler flag (GCC/Clang for Linux/macOS)

3. **Artifact Download**
   - PublishSymbols stage needs to download Build artifacts
   - Currently missing `DownloadPipelineArtifact@2` task

### Recommended Fix

```yaml
# In build-windows-job.yml - Add after building
- script: |
    # Ensure .pdb files are generated
    python setup.py build_ext --debug
    # OR if using CMake
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ...
  displayName: 'Build with debug symbols'

- task: CopyFiles@2
  displayName: 'Copy symbol files (.pdb)'
  inputs:
    SourceFolder: '$(Build.SourcesDirectory)/build'
    Contents: '**/*.pdb'
    TargetFolder: '$(ob_outputDirectory)/symbols/Windows'

# In build-release-package-pipeline.yml - Add before PublishSymbols task
- job: PublishSymbolsJob
  steps:
    - task: DownloadPipelineArtifact@2
      displayName: 'Download Build Artifacts'
      inputs:
        artifact: 'drop_Build_Build'
        path: '$(Pipeline.Workspace)/symbols'
    
    - task: PublishSymbols@2
      displayName: 'Publish Symbols'
      inputs:
        SymbolsFolder: '$(Pipeline.Workspace)/symbols'
        SearchPattern: '**/*.pdb'
        IndexSources: false
        PublishSymbols: true
        SymbolServerType: 'TeamServices'
        SymbolsArtifactName: 'mssql-python-symbols'
```

## How Customers Use Published Symbols 👥

### Scenario 1: Developer Debugging
```python
# Developer encounters crash during development
import mssql_python

conn = mssql_python.connect("...")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")  # ← Crashes here

# With Visual Studio debugger + symbols:
# 1. Exception breaks in debugger
# 2. Shows exact C++ line: connection.cpp:42
# 3. Shows variables: conn->state = DISCONNECTED
# 4. Developer can fix the root cause
```

### Scenario 2: Production Crash Analysis
```
1. Customer app crashes in production
2. Windows Error Reporting captures minidump
3. Minidump uploaded to internal crash analysis system
4. System uses symbol server to symbolicate crash
5. Engineers receive report with full stack trace
6. Fix deployed in next release
```

### Scenario 3: Performance Investigation
```
1. Customer reports: "Query execution is slow"
2. Ask customer to run profiler (py-spy, Windows Performance Recorder)
3. Profiler captures stack samples
4. With symbols: Shows 80% time in ResultSet::parse_large_rows()
5. Engineers optimize that specific function
```

## Symbol Security Considerations 🔒

### What Symbols Expose
✅ **Safe to expose**:
- Function names (already in import table)
- Stack frame layout
- Line numbers

⚠️ **Could expose**:
- Internal variable names (mild info leak)
- Source file paths (shows directory structure)
- Compiler/build settings

🔴 **Never in symbols**:
- Source code itself (not included)
- Secrets or credentials
- Customer data

### Recommendation for mssql-python
Since mssql-python is **open source**:
```yaml
IndexSources: false        # Don't embed source paths (privacy)
PublishSymbols: true       # Publish to Azure Artifacts (helps debugging)
# Make symbols publicly accessible to help customers debug issues
```

If symbols were for **closed-source** product:
```yaml
IndexSources: true         # Full debug info for internal use
PublishSymbols: false      # Keep private, don't expose internals
# Store in artifacts, share only with support team
```

## Testing Symbol Publishing 🧪

### Verify Symbols Are Generated
```bash
# Windows - check .pdb exists and has valid info
dumpbin /HEADERS mssql_python.pdb

# Linux - check debug info in .so
objdump -g mssql_python.so | head

# macOS - check debug info
dwarfdump mssql_python.so
```

### Verify Symbols Are Published
```bash
# Check Azure Artifacts symbol server
curl https://artifacts.dev.azure.com/microsoft/_apis/symbol/symsrv/mssql_python.pdb/index2.txt

# Should return list of available symbol GUIDs
```

### Verify Symbols Work
```bash
# WinDbg - load symbols and verify
.sympath SRV*C:\symbols*https://artifacts.dev.azure.com/...
.reload /f mssql_python.pyd
lm v m mssql_python    # Should show "PDB loaded successfully"
```

## Summary: Why You Need This ✨

For **mssql-python**, publishing symbols means:

1. **Better Customer Support**
   - Customers can send you meaningful crash dumps
   - You can debug production issues without reproducing locally

2. **Faster Bug Resolution**
   - Exact line numbers in stack traces
   - No guessing which function crashed

3. **Performance Optimization**
   - Profiling shows real function names
   - Identify bottlenecks accurately

4. **Professional Release Process**
   - Standard practice for native code libraries
   - Expected by enterprise customers

5. **Open Source Friendly**
   - Helps community contributors debug issues
   - Makes bug reports more actionable

**Bottom line**: Symbol publishing is essential for any library with native code (C/C++ extensions). It's the difference between "it crashed somewhere in mssql_python.pyd" and "it crashed in Connection::execute() at line 42 because conn was null".
