param([Parameter(Mandatory)][ValidateSet('2022', '2025')][string]$Year)
$ErrorActionPreference = 'Stop'

function Invoke-Installer([string]$FilePath, [string[]]$ArgumentList) {
    $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -PassThru -Wait
    if ($process.ExitCode -ne 0) { throw "SQL installer exited with code $($process.ExitCode)" }
}

if ($Year -eq '2022') {
    $major = 16
    $url = 'https://download.microsoft.com/download/e5d37105-aa68-4488-8ed5-b579e3809ea1/SQL2022-SSEI-Expr.exe'
} else {
    $major = 17
    $url = 'https://go.microsoft.com/fwlink/p/?linkid=2216019&clcid=0x409&culture=en-us&country=us'
}

# A prior attempt may have installed the engine before reporting failure. Check
# the actual instance/version and login, never consider a service alone ready.
if (Get-Service MSSQLSERVER -ErrorAction SilentlyContinue) {
    $env:SQLCMDPASSWORD = $env:DB_PASSWORD
    sqlcmd -S "lpc:." -U sa -C -b -l 5 -Q "IF CAST(SERVERPROPERTY('ProductMajorVersion') AS int) <> $major OR SERVERPROPERTY('EngineEdition') <> 4 THROW 50000, 'Unexpected SQL instance', 1;"
    if ($LASTEXITCODE -eq 0) { exit 0 }
}

# Do not reuse partial media or a partially extracted setup.exe on a retry.
$media = Join-Path $env:AGENT_TEMPDIRECTORY ([guid]::NewGuid().ToString())
$null = New-Item -Path $media -ItemType Directory
$bootstrapper = Join-Path $media "SQL${Year}-SSEI-Expr.exe"
$extract = Join-Path $media 'extracted'
try {
    Write-Host "Downloading SQL Server $Year Express..."
    # Download SQL Server $Year Express installer
    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -Uri $url -OutFile $bootstrapper

    Write-Host "Installing SQL Server $Year Express..."
    # Install SQL Server $Year Express with basic features
    Invoke-Installer -FilePath $bootstrapper -ArgumentList "/Action=Download","/MediaPath=`"$media`"","/MediaType=Core","/Quiet"

    # Find the downloaded setup file
    $setupFile = Get-ChildItem -Path $media -Filter "SQLEXPR_x64_ENU.exe" -Recurse | Select-Object -First 1

    if ($setupFile) {
      Write-Host "Extracting SQL Server setup files..."
      Invoke-Installer -FilePath $setupFile.FullName -ArgumentList "/x:`"$extract`"","/u"

      Write-Host "Running SQL Server setup..."
      $configPath = Join-Path $env:TEMP ("sql${Year}-setup-{0}.ini" -f [guid]::NewGuid())
      $configLines = @(
        '[OPTIONS]'
        'QUIET="True"'
        'ACTION="Install"'
        'FEATURES=SQLEngine'
        'INSTANCENAME=MSSQLSERVER'
        'SQLSVCACCOUNT="NT AUTHORITY\SYSTEM"'
        'SQLSYSADMINACCOUNTS="BUILTIN\Administrators"'
        'TCPENABLED="1"'
        'SECURITYMODE="SQL"'
        "SAPWD=`"$env:DB_PASSWORD`""
        'IACCEPTSQLSERVERLICENSETERMS="True"'
      )
      [System.IO.File]::Create($configPath).Dispose()
      try {
        $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        icacls $configPath /inheritance:r /grant:r "${identity}:(F)" "SYSTEM:(R)" | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Failed to secure SQL Server setup configuration" }
        Set-Content -Path $configPath -Value $configLines -Encoding ASCII
        Invoke-Installer -FilePath "$extract\setup.exe" -ArgumentList "/ConfigurationFile=`"$configPath`""
      } finally {
        Remove-Item $configPath -Force -ErrorAction Stop
      }
    } else {
      throw "Failed to download SQL Server setup file"
    }

    Write-Host "SQL Server $Year installation completed"
} finally {
    Remove-Item -LiteralPath $media -Recurse -Force
}
