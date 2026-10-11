# Orchestrates all reliability experiments from the host (OCS CPU count must change between runs).
# ASCII only: Windows PowerShell 5.1 reads BOM-less scripts as ANSI.
# Run: powershell -ExecutionPolicy Bypass -File scripts\run_reliability.ps1
Set-Location (Join-Path $PSScriptRoot "..")
$R = "results\reliability"
New-Item -ItemType Directory -Force $R | Out-Null
Remove-Item "$R\DONE" -ErrorAction SilentlyContinue

function Wait-Ocs {
  for ($i = 0; $i -lt 30; $i++) {
    docker compose exec -T compute python -c "from ocsdemo.connector import OcsConnector; print(OcsConnector().config())" 2>$null
    if ($LASTEXITCODE -eq 0) { return }
    Start-Sleep 2
  }
  throw "OCS not responding"
}

function Stage([string]$name, [string[]]$extra) {
  Write-Output "=== $name $extra ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/reliability.py $name @extra
  if ($LASTEXITCODE -ne 0) { Write-Output "!!! stage $name failed (exit $LASTEXITCODE)" }
}

$env:OCS_CPUS = "2"
docker compose up -d --force-recreate ocs compute
Wait-Ocs
foreach ($s in @("calib", "e7", "media", "ablation", "bandwidth", "scale")) { Stage $s @() }
foreach ($n in @("1", "4", "2")) {
  $env:OCS_CPUS = $n
  docker compose up -d --force-recreate ocs
  Wait-Ocs
  Stage "cpus" @("--label", $n)
}
$env:OCS_CPUS = "2"
New-Item -ItemType File "$R\DONE" | Out-Null
Start-Sleep 20
docker compose exec -T compute python scripts/stats.py | Out-File -Encoding utf8 "$R\stats_output.txt"
Write-Output "=== ALL DONE ($(Get-Date -Format HH:mm:ss))"
