param(
    [ValidateSet("run", "stop", "status")]
    [string]$Mode = "run"
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

$pythonPath = Join-Path $projectRoot ".venv\Scripts\python.exe"
$botPath = Join-Path $projectRoot "bot.py"
$pidPath = Join-Path $projectRoot "kithwave.pid"

function Test-ProcessAlive {
    param([int]$PidValue)
    try {
        $p = Get-Process -Id $PidValue -ErrorAction Stop
        return ($null -ne $p)
    } catch {
        return $false
    }
}

function Get-ProjectPythonPids {
    $ids = @()
    Get-Process -Name python, pythonw -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            if ($_.Path -and ([string]::Equals($_.Path, $pythonPath, [System.StringComparison]::OrdinalIgnoreCase))) {
                $ids += [int]$_.Id
            }
        } catch {
            # Ignore processes where path is inaccessible.
        }
    }
    return $ids
}

function Get-RunningPid {
    if (-not (Test-Path $pidPath)) {
        $projectPids = Get-ProjectPythonPids
        if ($projectPids.Count -gt 0) {
            return [int]$projectPids[0]
        }
        return $null
    }

    $raw = (Get-Content $pidPath -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($null -eq $raw) {
        Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
        return $null
    }

    $rawPid = $raw.Trim()
    if ($rawPid -notmatch "^\d+$") {
        Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
        return $null
    }

    $candidate = [int]$rawPid
    if (Test-ProcessAlive -PidValue $candidate) {
        return $candidate
    }

    Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
    $projectPids = Get-ProjectPythonPids
    if ($projectPids.Count -gt 0) {
        return [int]$projectPids[0]
    }
    return $null
}

function Stop-Bot {
    param([Nullable[int]]$PidValue)

    $stopped = 0
    $primaryPid = $null

    if ($null -ne $PidValue) {
        $primaryPid = [int]$PidValue
        if (Test-ProcessAlive -PidValue $primaryPid) {
            Write-Host "[KithWave] Stopping PID $primaryPid..."
            Stop-Process -Id $primaryPid -Force -ErrorAction SilentlyContinue
            $stopped++
        }
    }

    $extraPids = Get-ProjectPythonPids
    foreach ($extra in $extraPids) {
        if ($null -ne $primaryPid -and $extra -eq $primaryPid) {
            continue
        }
        Write-Host "[KithWave] Stopping extra PID $extra..."
        Stop-Process -Id $extra -Force -ErrorAction SilentlyContinue
        $stopped++
    }

    Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
    if ($stopped -eq 0) {
        Write-Host "[KithWave] Bot is not running."
    } else {
        Write-Host "[KithWave] Stopped."
    }
}

if ($Mode -eq "status") {
    $projectPids = Get-ProjectPythonPids
    if ($projectPids.Count -eq 0) {
        Write-Host "[KithWave] Status: stopped."
    } else {
        Write-Host ("[KithWave] Status: running (PID {0})." -f ($projectPids -join ", "))
    }
    exit 0
}

if ($Mode -eq "stop") {
    Stop-Bot -PidValue (Get-RunningPid)
    exit 0
}

if (-not (Test-Path $pythonPath)) {
    Write-Host "[KithWave] Missing $pythonPath"
    Write-Host "Create venv and install deps first."
    [void][System.Console]::ReadKey($true)
    exit 1
}

$runningPid = Get-RunningPid
if ($null -eq $runningPid) {
    $proc = Start-Process -FilePath $pythonPath -ArgumentList $botPath -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru
    $runningPid = $proc.Id
    Set-Content -Path $pidPath -Value $runningPid -Encoding ASCII
    Write-Host "[KithWave] Started in background (PID $runningPid)."
} else {
    if (-not (Test-Path $pidPath)) {
        Set-Content -Path $pidPath -Value $runningPid -Encoding ASCII
    }
    Write-Host "[KithWave] Already running (PID $runningPid)."
}

Write-Host ""
Write-Host "[KithWave] Close this window to keep it running."
Write-Host "[KithWave] Type 'stop' + Enter to stop it."
Write-Host "[KithWave] You can also press Ctrl+C."
Write-Host ""

$script:stopRequested = $false
$handler = [ConsoleCancelEventHandler]{
    param($sender, $eventArgs)
    $eventArgs.Cancel = $true
    $script:stopRequested = $true
}

[Console]::add_CancelKeyPress($handler)
try {
    while (-not $script:stopRequested) {
        $cmd = Read-Host "kithwave"
        if ($null -eq $cmd) {
            continue
        }

        $normalized = $cmd.Trim().ToLowerInvariant()
        if ($normalized -eq "stop" -or $normalized -eq "s" -or $normalized -eq "exit") {
            $script:stopRequested = $true
            continue
        }
        if ($normalized -eq "status") {
            $statusPids = Get-ProjectPythonPids
            if ($statusPids.Count -eq 0) {
                Write-Host "[KithWave] Status: stopped."
            } else {
                Write-Host ("[KithWave] Status: running (PID {0})." -f ($statusPids -join ", "))
            }
            continue
        }
        if ($normalized -eq "") {
            continue
        }

        Write-Host "[KithWave] Commands: status, stop"
    }
}
finally {
    [Console]::remove_CancelKeyPress($handler)
}

Stop-Bot -PidValue $runningPid
