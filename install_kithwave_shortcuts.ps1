$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$programsDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs"
$legacyDir = Join-Path $programsDir "KithWave"

$wsh = New-Object -ComObject WScript.Shell

foreach ($legacyName in @("KithWave Control.lnk", "KithWave Start.lnk", "KithWave Stop.lnk")) {
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

$lnkPath = Join-Path $programsDir "KithWave.lnk"
$shortcut = $wsh.CreateShortcut($lnkPath)
$shortcut.TargetPath = "$env:SystemRoot\System32\cmd.exe"
$shortcut.Arguments = "/c kithwave.bat"
$shortcut.WorkingDirectory = $projectRoot
$shortcut.IconLocation = "$env:SystemRoot\System32\shell32.dll,220"
$shortcut.Save()

Write-Host "Installed Start Menu shortcut:"
Write-Host $lnkPath
