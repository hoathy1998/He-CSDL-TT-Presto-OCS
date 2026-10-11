# Re-run stage e7 (n=10, randomized interleaving) with the power monitor, then recompute statistics.
# ASCII only (Windows PowerShell 5.1).
Set-Location (Join-Path $PSScriptRoot "..")
$R = "results\reliability"
Remove-Item "$R\DONE" -ErrorAction SilentlyContinue
$mon = Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File scripts\power_monitor.ps1" -PassThru -WindowStyle Hidden
$env:OCS_CPUS = "2"
docker compose up -d ocs compute
Write-Output "=== e7 ($(Get-Date -Format HH:mm:ss))"
docker compose exec -T compute python scripts/reliability.py e7
if ($LASTEXITCODE -ne 0) { Write-Output "!!! stage e7 failed (exit $LASTEXITCODE)" }
New-Item -ItemType File "$R\DONE" | Out-Null
$mon.WaitForExit(60000) | Out-Null
docker compose exec -T compute python scripts/stats.py | Out-File -Encoding utf8 "$R\stats_output.txt"
Write-Output "=== ALL DONE ($(Get-Date -Format HH:mm:ss))"
