$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$venvDir = Join-Path $PSScriptRoot '.venv'
$pythonExe = Join-Path $venvDir 'Scripts/python.exe'

if (-Not (Test-Path $pythonExe)) {
  Write-Host "Creating virtualenv at $venvDir" -ForegroundColor Cyan
  python -m venv $venvDir
}

Write-Host "Installing project into venv..." -ForegroundColor Cyan
& $pythonExe -m pip install --upgrade pip
& $pythonExe -m pip install -e $projectRoot
Write-Host "Done." -ForegroundColor Green
