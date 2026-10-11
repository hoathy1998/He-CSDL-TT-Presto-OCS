# Measurement chain (lane A): scale 4-64 files -> MinIO Select baseline -> real Presto, then statistics.
# Strictly sequential. Each stage starts ONLY the containers it needs (others stopped) so idle services
# cannot steal memory/CPU, plus the power monitor and the "machine is quiet" guard. ASCII only.
param([string]$Stages = "scale,minio,presto")
$run = $Stages.Split(",")
Set-Location (Join-Path $PSScriptRoot "..")
$R = "results\reliability"
Remove-Item "$R\DONE" -ErrorAction SilentlyContinue
$mon = Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File scripts\power_monitor.ps1" -PassThru -WindowStyle Hidden
$thm = Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File scripts\thermal_monitor.ps1" -PassThru -WindowStyle Hidden
$env:OCS_CPUS = "2"

function Wait-Quiet {
  for ($i = 0; $i -lt 720; $i++) {
    $busy = docker ps --format "{{.Names}} {{.Image}}" | Select-String -Pattern "prep|maven|builder"
    $building = Get-CimInstance Win32_Process -Filter "Name='docker.exe'" | Where-Object { $_.CommandLine -match " build |compose .*build| pull " }
    if (-not $busy -and -not $building) { break }
    Start-Sleep 5
  }
  Write-Output "running containers: $((docker ps --format '{{.Names}}') -join ', ')"
}
function Only([string[]]$profiles, [string[]]$keep) {
  docker compose --profile multi --profile minio --profile presto stop ocs2 ocs3 minio minio-tc presto 2>$null | Out-Null
  docker compose up -d ocs compute 2>$null | Out-Null
  if ($keep.Count -gt 0) {
    $pargs = @(); foreach ($p in $profiles) { $pargs += @("--profile", $p) }
    docker compose @pargs up -d @keep 2>$null | Out-Null
  }
}

if ($run -contains "scale") {
  Only @() @()
  Wait-Quiet
  Write-Output "=== scale ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/reliability.py scale
}
if ($run -contains "scale256") {
  Only @() @()
  Wait-Quiet
  Write-Output "=== scale256 ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/reliability.py scale256 --reps 10
}
if ($run -contains "minio") {
  Only @("minio") @("minio", "minio-tc")
  Start-Sleep 10
  Wait-Quiet
  Write-Output "=== minio ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/minio_baseline.py bench --reps 4
}
if ($run -contains "presto") {
  Only @("presto") @("presto")
  for ($i = 0; $i -lt 60; $i++) {
    docker compose exec -T compute python -c "import sys; sys.path.insert(0,'/app/scripts'); import presto_bench as p; p.presto('SELECT 1')" 2>$null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep 3
  }
  Wait-Quiet
  Write-Output "=== presto ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/presto_bench.py bench --reps 10
  Write-Output "=== presto_q3 ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/presto_bench.py bench --set q3 --reps 10
  Write-Output "=== presto_q4 ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/presto_bench.py bench --set q4 --reps 10
}
if ($run -contains "presto256") {
  Only @("presto") @("presto")
  for ($i = 0; $i -lt 60; $i++) {
    docker compose exec -T compute python -c "import sys; sys.path.insert(0,'/app/scripts'); import presto_bench as p; p.presto('SELECT 1')" 2>$null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep 3
  }
  Wait-Quiet
  Write-Output "=== presto256 ($(Get-Date -Format HH:mm:ss))"
  docker compose exec -T compute python scripts/presto_bench.py bench --set l256 --reps 10
}

New-Item -ItemType File "$R\DONE" | Out-Null
$mon.WaitForExit(60000) | Out-Null
docker compose exec -T compute python scripts/stats.py | Out-File -Encoding utf8 "$R\stats_output.txt"
Write-Output "=== ALL DONE ($(Get-Date -Format HH:mm:ss))"
