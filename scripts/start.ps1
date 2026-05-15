[CmdletBinding()]
param(
    # Optional python executable override.
    [string]$PythonExe = "python",
    # Virtual environment directory under project root.
    [string]$VenvDir = ".venv",
    # Optional host override for server binding.
    [string]$BindHost = "",
    # Optional port override; 0 means auto-resolve.
    [int]$Port = 0,
    # Only run checks, do not start server.
    [switch]$NoStart
)

# Stop on all errors for predictable startup behavior.
$ErrorActionPreference = "Stop"

# Switch to project root to keep relative paths stable.
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $projectRoot

function Write-Step {
    param([string]$Message)
    # Unified startup log prefix.
    Write-Host "[start] $Message" -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# Python detection
# ---------------------------------------------------------------------------
function Get-PythonPath {
    param([string]$exe)
    $found = $null
    try {
        # Ask interpreter to print its executable path.
        $out = & $exe -c "import sys; print(sys.executable)" 2>$null
        if ($LASTEXITCODE -eq 0 -and $out) {
            $found = $out.Trim()
        }
    } catch {
        # Ignore and continue fallback.
    }
    return $found
}

# Prefer CLI value, then python/python3/py fallbacks.
$bootPython = ""
if (-not [string]::IsNullOrWhiteSpace($PythonExe) -and $PythonExe -ne "python") {
    $bootPython = Get-Command $PythonExe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source
}
if ([string]::IsNullOrWhiteSpace($bootPython)) {
    foreach ($cand in @("python", "python3", "py")) {
        $p = Get-Command $cand -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1
        if ($p) {
            $bootPython = $p
            break
        }
    }
}
if ([string]::IsNullOrWhiteSpace($bootPython)) {
    Write-Host "[start] ERROR: Python 3.9+ was not found in PATH." -ForegroundColor Red
    exit 1
}
Write-Step "Bootstrap interpreter: $bootPython"

# ---------------------------------------------------------------------------
# Virtual environment detection
# ---------------------------------------------------------------------------
$venvPython = Join-Path (Join-Path $projectRoot $VenvDir) "Scripts\python.exe"
if (Test-Path $venvPython) {
    $pythonCmd = $venvPython
    Write-Step "Using venv python: $venvPython"
} else {
    $pythonCmd = $bootPython
    Write-Step "Using system python: $pythonCmd"
}

# 启动入口已内置依赖检查，这里提前提示便于用户理解首启耗时。
Write-Step "Dependency bootstrap is enabled. Missing packages will be installed automatically."

# ---------------------------------------------------------------------------
# Port detection and auto-increment
# ---------------------------------------------------------------------------
function Test-PortInUse {
    param([int]$Port)

    $tcp = $null
    try {
        # Use TCP connect to test if the port is listening.
        $tcp = New-Object System.Net.Sockets.TcpClient
        $result = $tcp.BeginConnect("127.0.0.1", $Port, $null, $null)
        $handle = $result.AsyncWaitHandle
        $waited = $false
        if ($null -ne $handle) {
            $waited = $handle.WaitOne(1000)
        }
        if ($waited) {
            $tcp.EndConnect($result) | Out-Null
            return $true
        }
        return $false
    } catch {
        return $false
    } finally {
        # Always release socket resources.
        if ($tcp) {
            $tcp.Close()
            $tcp.Dispose()
        }
    }
}

# Priority: CLI port > env SERVER_PORT > config.json > 8000.
$defaultPort = 8000
if ($Port -gt 0) {
    $defaultPort = $Port
} else {
    $envPortRaw = ""
    try {
        $envPortRaw = [string]$env:SERVER_PORT
    } catch {
        $envPortRaw = ""
    }

    $envPortTrimmed = $envPortRaw.Trim()
    if ($envPortTrimmed.Length -gt 0) {
        try {
            $defaultPort = [int]$envPortTrimmed
        } catch {
            # Invalid env value; continue with config fallback.
            $envPortTrimmed = ""
        }
    }

    if ($envPortTrimmed.Length -eq 0) {
        $cfgPath = Join-Path $projectRoot "config.json"
        if (Test-Path $cfgPath) {
            try {
                $cfgText = Get-Content -LiteralPath $cfgPath -Raw -Encoding UTF8
                $cfg = $cfgText | ConvertFrom-Json
                $sysSection = $cfg.system
                if ($null -ne $sysSection -and $null -ne $sysSection.server_port) {
                    $defaultPort = [int]$sysSection.server_port
                }
            } catch {
                # Keep default 8000 on any config read failure.
            }
        }
    }
}

# Increment until a free port is found.
$actualPort = $defaultPort
while (Test-PortInUse -Port $actualPort) {
    Write-Step "Port $actualPort is in use, trying $($actualPort + 1)"
    $actualPort++
}

if ($actualPort -ne $defaultPort) {
    Write-Step "Port changed from $defaultPort to $actualPort (was occupied)"
}

# Expose final port for server.py.
$env:SERVER_PORT = "$actualPort"
Write-Step "SERVER_PORT=$actualPort"

# ---------------------------------------------------------------------------
# Optional host override
# ---------------------------------------------------------------------------
if ($BindHost) {
    # Set host only when explicitly provided.
    $env:SERVER_HOST = $BindHost
    Write-Step "SERVER_HOST=$BindHost"
}

# ---------------------------------------------------------------------------
# No-start mode
# ---------------------------------------------------------------------------
if ($NoStart) {
    Write-Step "NoStart enabled"
    exit 0
}

# ---------------------------------------------------------------------------
# Start server, wait port ready, then open browser
# ---------------------------------------------------------------------------
Write-Step "Starting server.py ..."

# Run server in background and redirect logs.
$serverArgs = @{
    FilePath               = $pythonCmd
    ArgumentList           = @(Join-Path $projectRoot "server.py")
    NoNewWindow            = $true
    RedirectStandardOutput = Join-Path $projectRoot "server-start.log"
    RedirectStandardError  = Join-Path $projectRoot "server-start-error.log"
    PassThru               = $true
}
$serverProc = Start-Process @serverArgs

# Poll until port is listening, up to 30 seconds.
$pollInterval = 500
$timeout = 30000
$elapsed = 0
$portOpened = $false
while ($elapsed -lt $timeout) {
    Start-Sleep -Milliseconds $pollInterval
    $elapsed += $pollInterval

    $tcp = $null
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $result = $tcp.BeginConnect("127.0.0.1", $actualPort, $null, $null)
        $handle = $null
        try {
            $handle = $result.AsyncWaitHandle
        } catch {
            $handle = $null
        }

        if ($null -ne $handle -and $handle.WaitOne(1000)) {
            $tcp.EndConnect($result) | Out-Null
            $portOpened = $true
            break
        }
    } catch {
        # Ignore while server is still warming up.
    } finally {
        if ($tcp) {
            $tcp.Close()
            $tcp.Dispose()
        }
    }
}

if (-not $portOpened) {
    Write-Host "[start] WARNING: Server did not respond within 30s. Check server-start.log" -ForegroundColor Red
    exit 1
}

Write-Step "Server is running on port $actualPort"
Write-Step "Opening dashboard in browser..."

# Build access URL and try opening default browser.
$accessUrl = "http://localhost:$actualPort"
try {
    Start-Process $accessUrl
} catch {
    Write-Host "[start] Could not open browser automatically. Please visit $accessUrl" -ForegroundColor Yellow
}

Write-Step "Dashboard available at $accessUrl"

# Block until server exits and forward exit code.
$serverProc.WaitForExit() | Out-Null
exit $serverProc.ExitCode
