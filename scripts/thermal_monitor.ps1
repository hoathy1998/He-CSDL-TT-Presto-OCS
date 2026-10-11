# Logs CPU "% Processor Performance" (actual vs base frequency; drops when thermally throttled) every 15 s
# to results/reliability/cpu_perf_log.csv until results/reliability/DONE exists. ASCII only, no admin needed.
param([string]$Dir = (Join-Path $PSScriptRoot "..\results\reliability"))
$log = Join-Path $Dir "cpu_perf_log.csv"
$done = Join-Path $Dir "DONE"
if (-not (Test-Path $log)) { "epoch,perf_pct,utility_pct" | Out-File -Encoding ascii $log }
while (-not (Test-Path $done)) {
  $c = Get-Counter -Counter "\Processor Information(_Total)\% Processor Performance", "\Processor Information(_Total)\% Processor Utility" -MaxSamples 1
  $v = $c.CounterSamples | % { [math]::Round($_.CookedValue, 1) }
  $epoch = [math]::Round(([DateTimeOffset](Get-Date)).ToUnixTimeMilliseconds() / 1000, 1)
  "$epoch,$($v[0]),$($v[1])" | Out-File -Append -Encoding ascii $log
  Start-Sleep -Seconds 15
}
