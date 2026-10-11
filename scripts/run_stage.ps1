# Run one reliability stage with the power monitor, then recompute statistics.
# ASCII only (Windows PowerShell 5.1). Usage: powershell -File scripts\run_stage.ps1 -Stage engine
param([Parameter(Mandatory = $true)][string]$Stage, [string]$Label = "")
Set-Location (Join-Path $PSScriptRoot "..")
$R = "results\reliability"
Remove-Item "$R\DONE" -ErrorAction SilentlyContinue
$mon = Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File scripts\power_monitor.ps1" -PassThru -WindowStyle Hidden
$env:OCS_CPUS = "2"
docker compose up -d ocs compute
# Guard: never measure while preparation work (prep/upload/maven build/image build) is running,
# so that background activity cannot bias the timings. Wait until the machine is quiet.
for ($i = 0; $i -lt 720; $i++) {
  $busy = docker ps --format "{{.Names}} {{.Image}}" | Select-String -Pattern "prep|maven|builder|minio_baseline"
  $building = Get-CimInstance Win32_Process -Filter "Name='docker.exe'" | Where-Object { $_.CommandLine -match " build |compose .*build| pull " }
  if (-not $busy -and -not $building) { break }
  if ($i -eq 0) { Write-Output "waiting for background preparation to finish: $busy" }
  Start-Sleep 5
}
Write-Output "running containers at start: $((docker ps --format '{{.Names}}') -join ', ')"
Write-Output "=== $Stage ($(Get-Date -Format HH:mm:ss))"
if ($Label) { docker compose exec -T compute python scripts/reliability.py $Stage --label $Label }
else { docker compose exec -T compute python scripts/reliability.py $Stage }
if ($LASTEXITCODE -ne 0) { Write-Output "!!! stage $Stage failed (exit $LASTEXITCODE)" }
New-Item -ItemType File "$R\DONE" | Out-Null
$mon.WaitForExit(60000) | Out-Null
docker compose exec -T compute python scripts/stats.py | Out-File -Encoding utf8 "$R\stats_output.txt"
Write-Output "=== ALL DONE ($(Get-Date -Format HH:mm:ss))"
