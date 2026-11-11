<#
    start-proxy.ps1 -- launch the AgentRouter Codex compatibility proxy.

    Every path is derived from this script's own location, so the repository
    can be cloned anywhere by any user.

    Does five things, in order:
      0. Refuses to start a second instance on an already-healthy port.
      1. Loads .env if one is present (values are never printed).
      2. Activates .venv if one is present, else falls back to system Python.
      3. Verifies fastapi / uvicorn / httpx are importable.
      4. Verifies a credential exists WITHOUT printing it.
      5. Starts uvicorn bound to 127.0.0.1 (loopback only).

    Usage:
      .\start-proxy.ps1                  # interactive, Ctrl+C to stop
      .\start-proxy.ps1 -Port 8788       # default or custom port
      .\start-proxy.ps1 -Verbose_        # extra diagnostics
      .\start-proxy.ps1 -Service         # supervised, rotating log file
#>

[CmdletBinding()]
param(
    [int]$Port = 8788,
    [switch]$Verbose_,
    # Set by the scheduled task: log to a rotating file instead of the console.
    [switch]$Service,
    # Hard ceiling per log file. One previous generation is kept as .1, so
    # total on-disk log usage is bounded at roughly 2 x this value.
    [int]$MaxLogKB = 1024
)

$ErrorActionPreference = 'Stop'

# Script-directory anchored. Works from any clone location and any username.
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# ---------------------------------------------------------------------------
# Logging. Console when run interactively, rotating file under -Service.
#
# Nothing written here can contain credentials, prompts, or model output: the
# credential check below prints a length only, and the proxy's own log emits
# structural diagnostics exclusively.
# ---------------------------------------------------------------------------
$script:LogPath   = $null
$script:MaxBytes  = $MaxLogKB * 1KB
$script:LineCount = 0

if ($Service) {
    $logDir = Join-Path $root 'logs'
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
    $script:LogPath = Join-Path $logDir 'proxy.log'
}

function Invoke-LogRotation {
    if (-not $script:LogPath) { return }
    if (-not (Test-Path $script:LogPath)) { return }
    if ((Get-Item $script:LogPath).Length -le $script:MaxBytes) { return }
    $old = "$($script:LogPath).1"
    if (Test-Path $old) { Remove-Item $old -Force -ErrorAction SilentlyContinue }
    Move-Item $script:LogPath $old -Force -ErrorAction SilentlyContinue
}

function Write-Line {
    param([string]$Text, [string]$Color = 'Gray')
    if (-not $script:LogPath) { Write-Host $Text -ForegroundColor $Color; return }
    if (($script:LineCount % 50) -eq 0) { Invoke-LogRotation }
    $script:LineCount++
    $stamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    try { Add-Content -LiteralPath $script:LogPath -Value "$stamp $Text" -Encoding utf8 -ErrorAction Stop } catch { }
}

function Write-Step { param([string]$m) Write-Line "[start] $m" 'Cyan' }
function Write-Ok   { param([string]$m) Write-Line "[ ok  ] $m" 'Green' }
function Write-Warn { param([string]$m) Write-Line "[warn ] $m" 'Yellow' }
function Write-Err  { param([string]$m) Write-Line "[fail ] $m" 'Red' }

# ---------------------------------------------------------------------------
# 0. Fast single-instance guard.
# ---------------------------------------------------------------------------
function Test-OurProxy {
    param([int]$OnPort)
    try {
        $probe = Invoke-RestMethod "http://127.0.0.1:$OnPort/_health" -TimeoutSec 5 -ErrorAction Stop
        return ($probe.status -eq 'ok' -and $probe.listen -eq "127.0.0.1:$OnPort")
    } catch { return $false }
}

function Get-PortOwners {
    param([int]$OnPort)
    $conns = $null
    try { $conns = Get-NetTCPConnection -LocalPort $OnPort -State Listen -ErrorAction SilentlyContinue } catch { }
    if (-not $conns) { return @() }
    return @($conns | Select-Object -ExpandProperty OwningProcess -Unique)
}

function Show-PortConflict {
    param([int]$OnPort, [int[]]$OwnerPids)
    $names = @()
    foreach ($procId in $OwnerPids) {
        $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
        if ($proc) { $names += "$($proc.ProcessName) (PID $procId)" } else { $names += "PID $procId" }
    }
    Write-Err "Port $OnPort is occupied by an unrelated process: $($names -join ', ')"
    Write-Err 'It did not answer /_health as this proxy. Leaving it running and untouched.'
    Write-Err "Free the port yourself, or start the proxy elsewhere with -Port <number>."
}

$owners = Get-PortOwners -OnPort $Port
if ($owners.Count -gt 0) {
    if (Test-OurProxy -OnPort $Port) {
        Write-Ok "proxy already healthy on 127.0.0.1:$Port (PID $($owners -join ', ')) -- nothing to do"
        exit 0
    }
    Show-PortConflict -OnPort $Port -OwnerPids $owners
    exit 2
}

# ---------------------------------------------------------------------------
# 1. Optional .env (never required, never printed)
# ---------------------------------------------------------------------------
$envFile = Join-Path $root '.env'
if (Test-Path $envFile) {
    $loaded = 0
    foreach ($line in (Get-Content -LiteralPath $envFile -ErrorAction SilentlyContinue)) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) { continue }
        $split = $trimmed.IndexOf('=')
        if ($split -lt 1) { continue }
        $name  = $trimmed.Substring(0, $split).Trim()
        $value = $trimmed.Substring($split + 1).Trim().Trim('"').Trim("'")
        if (-not $name -or -not $value) { continue }
        Set-Item -Path "Env:$name" -Value $value
        $loaded++
    }
    Write-Ok ".env loaded ($loaded variable(s), values not displayed)"
}

# ---------------------------------------------------------------------------
# 2. Locate Python
# ---------------------------------------------------------------------------
Write-Step 'Locating Python interpreter'

$venvPython = Join-Path $root '.venv\Scripts\python.exe'
$activate   = Join-Path $root '.venv\Scripts\Activate.ps1'

if (Test-Path $venvPython) {
    if (Test-Path $activate) {
        try { . $activate; Write-Ok 'venv activated' }
        catch { Write-Warn 'venv activation script failed; using its python.exe directly' }
    }
    $python = $venvPython
} else {
    Write-Warn 'no .venv found -- falling back to system Python'
    $python = (Get-Command python -ErrorAction SilentlyContinue).Source
    if (-not $python) { $python = (Get-Command py -ErrorAction SilentlyContinue).Source }
    if (-not $python) {
        Write-Err 'No Python interpreter found on PATH. Install Python 3.10+ and re-run.'
        exit 1
    }
}

$pyVersion = & $python --version 2>&1
Write-Ok "interpreter: $pyVersion"

# ---------------------------------------------------------------------------
# 3. Verify dependencies
# ---------------------------------------------------------------------------
Write-Step 'Verifying dependencies (fastapi, uvicorn, httpx)'

$checkScript = @'
import importlib, sys
missing = []
for mod in ("fastapi", "uvicorn", "httpx"):
    try:
        m = importlib.import_module(mod)
        print(f"  {mod:<8} {getattr(m, '__version__', 'unknown')}")
    except ImportError:
        missing.append(mod)
if missing:
    print("MISSING:" + ",".join(missing))
    sys.exit(3)
'@

$ErrorActionPreference = 'Continue'
$depOutput = $checkScript | & $python - 2>&1
$depExit = $LASTEXITCODE
$depOutput | ForEach-Object { Write-Line ([string]$_) }

if ($depExit -ne 0) {
    Write-Warn 'Dependencies missing -- installing from requirements.txt'
    & $python -m pip install -r (Join-Path $root 'requirements.txt') 2>&1 |
        ForEach-Object { Write-Line ([string]$_) }
    if ($LASTEXITCODE -ne 0) {
        Write-Err 'pip install failed. Install manually: python -m pip install -r requirements.txt'
        exit 1
    }
    $checkScript | & $python - | ForEach-Object { Write-Line ([string]$_) }
    if ($LASTEXITCODE -ne 0) { Write-Err 'Dependencies still unavailable after install.'; exit 1 }
}
Write-Ok 'all dependencies present'

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# 4. Check for a credential -- never print its value
# ---------------------------------------------------------------------------
Write-Step 'Checking for a credential in the environment'

$keyVar = $null
$keyLen = 0
foreach ($name in @('CODEX_GATEWAY_API_KEY', 'AGENTROUTER_API_KEY', 'OPENAI_API_KEY')) {
    $val = [Environment]::GetEnvironmentVariable($name)
    if ([string]::IsNullOrWhiteSpace($val)) {
        $val = [Environment]::GetEnvironmentVariable($name, 'User')
        if (-not [string]::IsNullOrWhiteSpace($val)) {
            Set-Item -Path "Env:$name" -Value $val
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($val)) { $keyVar = $name; $keyLen = $val.Length; break }
}

if ($keyVar) {
    Write-Ok "credential found in `$env:$keyVar (length $keyLen, value not displayed)"
} else {
    Write-Warn 'No CODEX_GATEWAY_API_KEY / AGENTROUTER_API_KEY set.'
    Write-Warn 'Codex will forward whatever token is set in config.toml or send its own.'
}

# ---------------------------------------------------------------------------
# 5. Start uvicorn on loopback only
# ---------------------------------------------------------------------------
if ($Verbose_) { $env:PROXY_VERBOSE = '1' }
$env:PYTHONIOENCODING = 'utf-8:replace'

try { [Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false } catch { }

$env:PROXY_PORT = "$Port"

$upstream = $env:AGENTROUTER_UPSTREAM
if ([string]::IsNullOrWhiteSpace($upstream)) { $upstream = 'https://agentrouter.org' }

Write-Line ''
Write-Line '  AgentRouter Codex Responses Compatibility Proxy' 'White'
Write-Line "  upstream : $upstream"  'DarkGray'
Write-Line "  listen   : http://127.0.0.1:$Port (loopback only)" 'DarkGray'
Write-Line "  stats    : http://127.0.0.1:$Port/_stats" 'DarkGray'
Write-Line "  health   : http://127.0.0.1:$Port/_health" 'DarkGray'
if ($Service) { Write-Line "  log      : $($script:LogPath) (rotates at $MaxLogKB KB)" 'DarkGray' }
else          { Write-Line '  stop     : Ctrl+C' 'DarkGray' }
Write-Line ''

function Stop-OwnUvicorn {
    $childName = Split-Path -Leaf $python
    $kids = @()
    try {
        $kids = @(Get-CimInstance Win32_Process -Filter "ParentProcessId = $PID" -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -eq $childName })
    } catch { }
    foreach ($kid in $kids) {
        Write-Warn "stopping leftover uvicorn (PID $($kid.ProcessId))"
        Stop-Process -Id $kid.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-Uvicorn {
    param([int]$OnPort)
    $savedPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $script:LastUvicornExit = $null
    try {
        & $python -m uvicorn proxy:app --host 127.0.0.1 --port $OnPort --timeout-keep-alive 600 --log-level warning --no-access-log 2>&1 |
            ForEach-Object { Write-Line ([string]$_) }
        $script:LastUvicornExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $savedPreference
        Stop-OwnUvicorn
    }
}

if ($Service) {
    $backoff = 5
    while ($true) {
        $runStart = [System.Diagnostics.Stopwatch]::StartNew()
        Write-Step 'Supervisor: starting proxy under supervision...'
        Invoke-Uvicorn -OnPort $Port
        $runStart.Stop()
        $ranForSec = $runStart.Elapsed.TotalSeconds

        if ($ranForSec -ge 30) { $backoff = 5 }

        Write-Warn "Proxy exited (code: $script:LastUvicornExit) after $([math]::Round($ranForSec, 1))s. Restarting in ${backoff}s..."
        Start-Sleep -Seconds $backoff
        $backoff = [math]::Min($backoff * 2, 60)
    }
} else {
    try {
        Invoke-Uvicorn -OnPort $Port
    } finally {
        Write-Ok 'Proxy stopped.'
    }
}
