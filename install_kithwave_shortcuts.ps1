$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$programsDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs"
$legacyDir = Join-Path $programsDir "KithWave"
$roamingRoot = Split-Path -Parent $programsDir
$appDataRoot = Split-Path -Parent $roamingRoot
$profileRoot = Split-Path -Parent $appDataRoot
$defaultRoot = Join-Path $profileRoot "KithWave"
$defaultBatch = Join-Path $defaultRoot "kithwave.bat"
$scriptBatch = Join-Path $projectRoot "kithwave.bat"
$batchPath = if (Test-Path $defaultBatch) { $defaultBatch } else { $scriptBatch }
$batchRoot = Split-Path -Parent $batchPath

$wsh = New-Object -ComObject WScript.Shell

foreach ($legacyName in @("KithWave.lnk", "KithWave Stop.lnk", "KithWave Control.lnk", "KithWave Start.lnk")) {
    $legacyInPrograms = Join-Path $programsDir $legacyName
    if (Test-Path $legacyInPrograms) {
        try {
            Remove-Item $legacyInPrograms -Force -ErrorAction Stop
        } catch {
            Write-Host "Skipping locked legacy shortcut: $legacyInPrograms"
        }
    }

    $legacyInFolder = Join-Path $legacyDir $legacyName
    if (Test-Path $legacyInFolder) {
        try {
            Remove-Item $legacyInFolder -Force -ErrorAction Stop
        } catch {
            Write-Host "Skipping locked legacy shortcut: $legacyInFolder"
        }
    }
}

if (Test-Path $legacyDir) {
    try {
        Remove-Item $legacyDir -Force -Recurse -ErrorAction Stop
    } catch {
        Write-Host "Skipping legacy folder cleanup: $legacyDir"
    }
}

$mainLnkPath = Join-Path $programsDir "KithWave.lnk"
$mainShortcut = $wsh.CreateShortcut($mainLnkPath)
$mainShortcut.TargetPath = "$env:SystemRoot\System32\cmd.exe"
$mainShortcut.Arguments = "/c kithwave.bat menu"
$mainShortcut.WorkingDirectory = $batchRoot
$mainShortcut.IconLocation = "$batchPath,0"
$mainShortcut.Save()

$stopLnkPath = Join-Path $programsDir "KithWave Stop.lnk"
$stopShortcut = $wsh.CreateShortcut($stopLnkPath)
$stopShortcut.TargetPath = "$env:SystemRoot\System32\cmd.exe"
$stopShortcut.Arguments = "/c kithwave.bat stop"
$stopShortcut.WorkingDirectory = $batchRoot
$stopShortcut.IconLocation = "$batchPath,0"
$stopShortcut.Save()

Write-Host "Installed Start Menu shortcuts:"
Write-Host $mainLnkPath
Write-Host $stopLnkPath
