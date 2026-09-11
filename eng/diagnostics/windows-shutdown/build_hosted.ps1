param(
    [Parameter(Mandatory = $true)][string]$RepositoryRoot,
    [Parameter(Mandatory = $true)][string]$PrivateRoot,
    [Parameter(Mandatory = $true)][string]$PublishRoot
)
$ErrorActionPreference = 'Stop'
$control = Get-Content -LiteralPath "$PrivateRoot\control.json" -Raw | ConvertFrom-Json
if ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() -ge [long]$control.work_deadline_unix_ms - 300000) {
    throw 'Insufficient global work budget to build and verify native symbols.'
}
Set-Location -LiteralPath $RepositoryRoot
& python "$PSScriptRoot\hosted_shutdown.py" verify --repo $RepositoryRoot --private $PrivateRoot --out $PublishRoot
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$tag = & python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')"
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
$vs = & $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if (-not $vs) { throw 'VS2022 x64 C++ toolchain is required.' }
$vcvars = "$vs\VC\Auxiliary\Build\vcvars64.bat"
$bin = "$PrivateRoot\bin"
New-Item -ItemType Directory -Path $bin -Force | Out-Null
$buildLog = "$PrivateRoot\native-build.raw.log"
$source = "$RepositoryRoot\mssql_python\pybind"
$build = "$source\build\x64\py$tag"
Push-Location -LiteralPath $source
try {
    & $env:ComSpec /d /c "call `"$source\build.bat`" x64 > `"$buildLog`" 2>&1"
    $buildCode = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($buildCode -ne 0) { throw "Supported native build failed, exit $buildCode; raw build log is private." }
$commands = @(
    "call `"$vcvars`" >nul",
    "cmake -S `"$source`" -B `"$build`" `"-DCMAKE_MODULE_LINKER_FLAGS_RELEASE=/DEBUG /OPT:REF /OPT:ICF`"",
    "cmake --build `"$build`" --config Release",
    "copy /y `"$build\Release\ddbc_bindings.cp$tag-amd64.pyd`" `"$RepositoryRoot\mssql_python\ddbc_bindings.cp$tag-amd64.pyd`"",
    "copy /y `"$build\Release\ddbc_bindings.cp$tag-amd64.pdb`" `"$RepositoryRoot\mssql_python\ddbc_bindings.cp$tag-amd64.pdb`"",
    "cl /nologo /std:c++17 /EHsc /MD /O2 /Zi `"$PSScriptRoot\native_probe.cpp`" /Fo`"$bin\native_probe.obj`" /Fe`"$bin\native_probe.exe`" /link /DEBUG /OPT:REF /OPT:ICF /PDB:`"$bin\native_probe.pdb`" dbghelp.lib version.lib"
)
$batch = @('@echo off')
foreach ($command in $commands) {
    $batch += $command
    $batch += 'if errorlevel 1 exit /b 1'
}
$batch += 'exit /b 0'
$batch | Set-Content -LiteralPath "$PrivateRoot\relink-private.cmd" -Encoding ASCII
& $env:ComSpec /d /c "call `"$PrivateRoot\relink-private.cmd`" >> `"$buildLog`" 2>&1"
if ($LASTEXITCODE -ne 0) { throw "Native build failed, exit $LASTEXITCODE; raw build log is private." }
$pyd = "$RepositoryRoot\mssql_python\ddbc_bindings.cp$tag-amd64.pyd"
& $env:ComSpec /d /c "`"$bin\native_probe.exe`" --verify-pdb `"$pyd`" > `"$PrivateRoot\pdb-verify.raw.log`" 2>&1"
if ($LASTEXITCODE -ne 0) { throw 'Native PYD/PDB matching and SqlHandle::free symbol verification failed.' }
@{
    python_tag = $tag
    compiler = [regex]::Match((Get-Content -LiteralPath $buildLog -Raw), 'The CXX compiler identification is ([^\r\n]+)').Groups[1].Value
    approved_manifest_sha256 = (Get-FileHash -LiteralPath "$PSScriptRoot\approved-sources.json").Hash.ToLowerInvariant()
    source_lf_tree_sha256 = (Get-Content -LiteralPath "$PSScriptRoot\approved-sources.json" -Raw | ConvertFrom-Json).lf_tree_sha256
    pyd_sha256 = (Get-FileHash -LiteralPath $pyd).Hash.ToLowerInvariant()
    pdb_sha256 = (Get-FileHash -LiteralPath ($pyd -replace '\.pyd$', '.pdb')).Hash.ToLowerInvariant()
    collector_sha256 = (Get-FileHash -LiteralPath "$bin\native_probe.exe").Hash.ToLowerInvariant()
    effective_private_link_flags = '/DEBUG /OPT:REF /OPT:ICF'
    shipping_CMake_changed = $false
    pdb_verification = (Get-Content -LiteralPath "$PrivateRoot\pdb-verify.raw.log" -Raw).Trim()
} | ConvertTo-Json | Set-Content -LiteralPath "$PublishRoot\build.json" -Encoding UTF8
Write-Host 'Native extension, effective private PDBs, and text-only collector built and symbol-verified.'
