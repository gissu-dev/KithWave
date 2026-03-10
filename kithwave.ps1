param(
    [ValidateSet("run", "start", "stop", "status", "restart")]
    [string]$Mode = "start"
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

$pythonPath = Join-Path $projectRoot ".venv\Scripts\python.exe"
$botPath = Join-Path $projectRoot "bot.py"
$pidPath = Join-Path $projectRoot "kithwave.pid"
$resolvedBotPath = $botPath
try {
    $resolvedBotPath = (Resolve-Path $botPath -ErrorAction Stop).Path
} catch {
    # Keep original path when resolve fails.
}

function Test-ProcessAlive {
    param([int]$PidValue)
    try {
        $p = Get-Process -Id $PidValue -ErrorAction Stop
        return ($null -ne $p)
    } catch {
        return $false
    }
}

function Read-PidFile {
    if (-not (Test-Path $pidPath)) {
        return $null
    }

    $raw = Get-Content $pidPath -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $raw) {
        Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
        return $null
    }

    $value = $raw.Trim()
    if ($value -notmatch "^\d+$") {
        Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
        return $null
    }

    return [int]$value
}

function Write-PidFile {
    param([int]$PidValue)
    Set-Content -Path $pidPath -Value $PidValue -Encoding ASCII
}

function Remove-PidFile {
    Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
}

function Get-ProjectPythonPids {
    $ids = @()

    try {
        $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" -ErrorAction Stop
        foreach ($proc in $procs) {
            $pid = [int]$proc.ProcessId
            $exePath = [string]$proc.ExecutablePath
            $cmdLine = [string]$proc.CommandLine

            if ($exePath -and (-not [string]::Equals($exePath, $pythonPath, [System.StringComparison]::OrdinalIgnoreCase))) {
                continue
            }

            $matchesCmd = $false
            if ($cmdLine) {
                if ($resolvedBotPath -and $cmdLine.IndexOf($resolvedBotPath, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                    $matchesCmd = $true
                } elseif ($cmdLine -match "(^|[\s`"'])bot\.py($|[\s`"'])") {
                    $matchesCmd = $true
                }
            }

            if ($matchesCmd) {
                $ids += $pid
            }
        }
    } catch {
        # Fallback below.
    }

    if ($ids.Count -eq 0) {
        Get-Process -Name python, pythonw -ErrorAction SilentlyContinue | ForEach-Object {
            try {
                if ($_.Path -and [string]::Equals($_.Path, $pythonPath, [System.StringComparison]::OrdinalIgnoreCase)) {
                    $ids += [int]$_.Id
                }
            } catch {
                # Ignore processes where metadata is inaccessible.
            }
        }
    }

    return @($ids | Sort-Object -Unique)
}

function Get-RunningPid {
    param([switch]$AllowScan)

    $pidFromFile = Read-PidFile
    if ($null -ne $pidFromFile) {
        if (Test-ProcessAlive -PidValue $pidFromFile) {
            return $pidFromFile
        }
        Remove-PidFile
    }

    if (-not $AllowScan) {
        return $null
    }

    $candidates = Get-ProjectPythonPids
    if ($candidates.Count -gt 0) {
        $candidate = [int]$candidates[0]
        Write-PidFile -PidValue $candidate
        return $candidate
    }

    return $null
}

function Show-Status {
    $pidFromFile = Read-PidFile
    if ($null -ne $pidFromFile -and (Test-ProcessAlive -PidValue $pidFromFile)) {
        Write-Host ("[KithWave] Status: running (PID {0})." -f $pidFromFile)
        return
    }

    $runningPid = Get-RunningPid -AllowScan
    if ($null -eq $runningPid) {
        Write-Host "[KithWave] Status: stopped."
        return
    }

    $projectPids = Get-ProjectPythonPids
    if ($projectPids.Count -eq 0) {
        $projectPids = @($runningPid)
    } elseif ($projectPids -notcontains $runningPid) {
        $projectPids = @($runningPid) + @($projectPids)
    }
    $projectPids = @($projectPids | Sort-Object -Unique)
    Write-Host ("[KithWave] Status: running (PID {0})." -f ($projectPids -join ", "))
}

function Start-Bot {
    if (-not (Test-Path $pythonPath)) {
        throw "[KithWave] Missing $pythonPath`nCreate venv and install deps first."
    }

    $runningPid = Get-RunningPid -AllowScan
    if ($null -ne $runningPid) {
        Write-Host "[KithWave] Already running (PID $runningPid)."
        return [int]$runningPid
    }

    $proc = Start-Process -FilePath $pythonPath -ArgumentList $botPath -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru
    Write-PidFile -PidValue ([int]$proc.Id)
    Write-Host "[KithWave] Started in background (PID $($proc.Id))."
    return [int]$proc.Id
}

function Stop-Bot {
    param([Nullable[int]]$PidValue = $null)

    $targetPid = $PidValue
    if ($null -eq $targetPid) {
        $targetPid = Get-RunningPid
    }

    if ($null -eq $targetPid) {
        $scanPids = Get-ProjectPythonPids
        if ($scanPids.Count -gt 0) {
            $targetPid = [int]$scanPids[0]
        }
    }

    if ($null -eq $targetPid) {
        Remove-PidFile
        Write-Host "[KithWave] Bot is not running."
        return $true
    }

    $target = [int]$targetPid
    Write-Host "[KithWave] Stopping PID $target..."

    $stopped = $false
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            Stop-Process -Id $target -Force -ErrorAction Stop
        } catch {
            # Continue retries below.
        }

        Start-Sleep -Milliseconds (120 * $attempt)
        if (-not (Test-ProcessAlive -PidValue $target)) {
            $stopped = $true
            break
        }
    }

    $extraStopped = 0
    if (-not $stopped) {
        foreach ($extra in Get-ProjectPythonPids) {
            if ($extra -eq $target) {
                continue
            }
            try {
                Write-Host "[KithWave] Stopping extra PID $extra..."
                Stop-Process -Id $extra -Force -ErrorAction Stop
                Start-Sleep -Milliseconds 120
                if (-not (Test-ProcessAlive -PidValue $extra)) {
                    $extraStopped++
                }
            } catch {
                Write-Host "[KithWave] Could not stop extra PID ${extra}: $($_.Exception.Message)"
            }
        }
    }

    if ($stopped -or (-not (Test-ProcessAlive -PidValue $target))) {
        Remove-PidFile
        if ($extraStopped -gt 0) {
            Write-Host "[KithWave] Stopped. (Also stopped $extraStopped extra process(es).)"
        } else {
            Write-Host "[KithWave] Stopped."
        }
        return $true
    }

    Write-Host "[KithWave] Could not stop PID $target. Try closing any locked Python process or run terminal as admin."
    return $false
}

function Restart-Bot {
    [void](Stop-Bot -PidValue (Get-RunningPid))
    Start-Sleep -Milliseconds 250
    [void](Start-Bot)
}

if ($Mode -eq "status") {
    Show-Status
    exit 0
}

if ($Mode -eq "stop") {
    [void](Stop-Bot -PidValue (Get-RunningPid))
    exit 0
}

if ($Mode -eq "start") {
    try {
        [void](Start-Bot)
        exit 0
    } catch {
        Write-Host $_.Exception.Message
        exit 1
    }
}

if ($Mode -eq "restart") {
    try {
        Restart-Bot
        exit 0
    } catch {
        Write-Host $_.Exception.Message
        exit 1
    }
}

try {
    $runningPid = Start-Bot
} catch {
    Write-Host $_.Exception.Message
    exit 1
}

Write-Host ""
Write-Host "[KithWave] Interactive control mode."
Write-Host "[KithWave] Type: status, restart, stop, exit"
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
        if ($normalized -eq "") {
            continue
        }
        if ($normalized -eq "status") {
            Show-Status
            continue
        }
        if ($normalized -eq "restart" -or $normalized -eq "r") {
            Restart-Bot
            continue
        }
        if ($normalized -eq "stop" -or $normalized -eq "s") {
            $script:stopRequested = $true
            continue
        }
        if ($normalized -eq "exit" -or $normalized -eq "quit" -or $normalized -eq "q") {
            $script:stopRequested = $true
            continue
        }

        Write-Host "[KithWave] Commands: status, restart, stop, exit"
    }
}
finally {
    [Console]::remove_CancelKeyPress($handler)
}

[void](Stop-Bot -PidValue (Get-RunningPid))
