param(
    [Parameter(Mandatory = $true)][ValidateSet('SQL2022', 'SQL2025')][string]$SqlVersion,
    [Parameter(Mandatory = $true)][string]$PrivateRoot,
    [Parameter(Mandatory = $true)][string]$PublishRoot
)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
# Azure Pipelines exports agent.isselfhosted as "0"/"1", not Boolean strings.
if ($env:TF_BUILD -ne 'True' -or $env:AGENT_OS -ne 'Windows_NT' -or $env:AGENT_ISSELFHOSTED -cne '0') {
    throw 'SQL installation requires a Microsoft-hosted Windows Azure Pipelines agent.'
}
$control = Get-Content -LiteralPath "$PrivateRoot\control.json" -Raw | ConvertFrom-Json
function Remaining-Milliseconds {
    $left = [long]$control.work_deadline_unix_ms - [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    if ($left -le 10000) { throw 'Global diagnostic deadline reached.' }
    return [int][Math]::Min($left - 10000, 1200000)
}
function Run-Installer([string]$File, [string[]]$Arguments, [string]$Label) {
    $remaining = Remaining-Milliseconds
    $process = Start-Process -FilePath $File -ArgumentList $Arguments -PassThru -NoNewWindow
    try {
        # Cache the owned handle before WaitForExit; PS5.1 can otherwise lose ExitCode.
        $ownedHandle = $process.Handle
        if ($ownedHandle -eq [IntPtr]::Zero) { throw "$Label has no owned process handle." }
        if (-not $process.WaitForExit($remaining)) {
            if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
            if (-not $process.WaitForExit(5000)) {
                throw "$Label timed out and process cleanup did not complete."
            }
            throw "$Label exceeded its bounded installation budget."
        }
        $exitCode = $process.ExitCode
        if ($null -eq $exitCode) { throw "$Label did not provide an exit code." }
        if ($exitCode -notin @(0, 3010)) {
            throw "$Label failed with exit code $exitCode. Raw installer logs remain private."
        }
        Write-Host "$Label completed with exit code $exitCode"
    } finally {
        $process.Dispose()
    }
}

$bytes = New-Object byte[] 32
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
$password = ([BitConverter]::ToString($bytes).Replace('-', '').ToLowerInvariant()) + 'Aa1!'
@{ password = $password } | ConvertTo-Json | Set-Content -LiteralPath "$PrivateRoot\sql-secret.json" -Encoding UTF8
$url = if ($SqlVersion -eq 'SQL2022') {
    'https://download.microsoft.com/download/e5d37105-aa68-4488-8ed5-b579e3809ea1/SQL2022-SSEI-Expr.exe'
} else {
    'https://go.microsoft.com/fwlink/p/?linkid=2216019&clcid=0x409&culture=en-us&country=us'
}
$bootstrap = "$PrivateRoot\$SqlVersion-SSEI-Expr.exe"
$media = "$PrivateRoot\media"
$setup = "$PrivateRoot\setup"
New-Item -ItemType Directory -Path $media, $setup -Force | Out-Null
$downloadSeconds = [int][Math]::Min(300, (Remaining-Milliseconds) / 1000)
Invoke-WebRequest -Uri $url -OutFile $bootstrap -UseBasicParsing -TimeoutSec $downloadSeconds
Run-Installer $bootstrap @('/Action=Download', "/MediaPath=`"$media`"", '/MediaType=Core', '/Quiet') 'SQL media download'
$archive = @(Get-ChildItem -LiteralPath $media -Filter SQLEXPR_x64_ENU.exe -Recurse)
if ($archive.Count -ne 1) { throw 'Expected exactly one downloaded Express core installer.' }
Run-Installer $archive[0].FullName @("/x:`"$setup`"", '/u') 'SQL media extraction'
Run-Installer "$setup\setup.exe" @(
    '/Q', '/ACTION=Install', '/FEATURES=SQLEngine', '/INSTANCENAME=MSSQLSERVER',
    '/SQLSVCACCOUNT="NT AUTHORITY\SYSTEM"', '/SQLSYSADMINACCOUNTS="BUILTIN\Administrators"',
    '/TCPENABLED=1', '/SECURITYMODE=SQL', "/SAPWD=$password", '/IACCEPTSQLSERVERLICENSETERMS'
) 'SQL engine installation'

Add-Type -AssemblyName System.Data
$builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
$builder['Data Source'] = 'localhost'
$builder['Initial Catalog'] = 'master'
$builder['User ID'] = 'sa'
$builder['Password'] = $password
$builder['Encrypt'] = $true
$builder['TrustServerCertificate'] = $true
$builder['Connect Timeout'] = 3
$connection = New-Object System.Data.SqlClient.SqlConnection($builder.ConnectionString)
$connected = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    [void](Remaining-Milliseconds)
    try {
        $connection.Open()
        $connected = $true
        break
    } catch [System.Data.SqlClient.SqlException] {
        Start-Sleep -Seconds 2
    }
}
if (-not $connected) { throw 'SQL did not become ready within the bounded connection attempts.' }
try {
    $command = $connection.CreateCommand()
    $command.CommandTimeout = 30
    $command.CommandText = "SELECT CONVERT(int, SERVERPROPERTY('ProductMajorVersion'))"
    $major = $command.ExecuteScalar()
    $expected = if ($SqlVersion -eq 'SQL2022') { 16 } else { 17 }
    if ($major -ne $expected) { throw 'Installed SQL engine major version does not match requested leg.' }
    $command.CommandText = "CREATE DATABASE TestDB; CREATE LOGIN testuser WITH PASSWORD = '$password';"
    [void]$command.ExecuteNonQuery()
    $command.CommandText = 'USE TestDB; CREATE USER testuser FOR LOGIN testuser; ALTER ROLE db_owner ADD MEMBER testuser;'
    [void]$command.ExecuteNonQuery()
} finally {
    $connection.Dispose()
}
@{ sqlVersion = $SqlVersion; major = $major; database = 'TestDB'; authentication = 'SQL';
   password_source = 'per-job cryptographic random secret, private file only';
   bootstrap_sha256 = (Get-FileHash -LiteralPath $bootstrap).Hash.ToLowerInvariant() } |
    ConvertTo-Json | Set-Content -LiteralPath "$PublishRoot\setup.json" -Encoding UTF8
Write-Host 'Requested SQL engine and job-local test database are ready.'
