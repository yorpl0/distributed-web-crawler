$ErrorActionPreference = "Stop"
$projectRoot = $PSScriptRoot
$venv = Join-Path $projectRoot ".venv"
$python = Join-Path $venv "Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python)) {
  python -m venv $venv
}

& $python -m pip install --upgrade pip
& $python -m pip install -e "$projectRoot[test]"
& $python -c "import aiohttp, protego, redis; print('Python crawler dependencies installed')"
