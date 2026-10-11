# Giám sát nguồn điện của host trong lúc chạy thí nghiệm (Windows PowerShell 5.1).
#  - Mỗi 15s ghi: epoch UTC, đang cắm sạc (1/0), % pin -> results/reliability/power_log.csv
#  - Yêu cầu Windows không ngủ (SetThreadExecutionState), KHÔNG đổi cài đặt nguồn
#  - Khi có file results/reliability/DONE: xuất sự kiện Modern Standby (506/507) -> standby.json rồi thoát
param([string]$Dir = (Join-Path $PSScriptRoot "..\results\reliability"))
Add-Type -Namespace W -Name P -MemberDefinition '[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint f);'
New-Item -ItemType Directory -Force $Dir | Out-Null
$log = Join-Path $Dir "power_log.csv"
$done = Join-Path $Dir "DONE"
$start = Get-Date
if (-not (Test-Path $log)) { "epoch,on_ac,charge" | Out-File -Encoding ascii $log }
else {  # log already exists (monitor restarted): export standby events since the first logged sample
  $first = (Get-Content $log | Select-Object -Skip 1 -First 1) -split ","
  if ($first[0]) { $start = [DateTimeOffset]::FromUnixTimeSeconds([long][double]$first[0]).LocalDateTime }
}
while (-not (Test-Path $done)) {
  [W.P]::SetThreadExecutionState([uint32]"0x80000001") | Out-Null      # ES_CONTINUOUS | ES_SYSTEM_REQUIRED
  $b = Get-CimInstance Win32_Battery
  $ac = if ($b.BatteryStatus -eq 2 -or $b.BatteryStatus -ge 6) { 1 } else { 0 }
  $epoch = [math]::Round(([DateTimeOffset](Get-Date)).ToUnixTimeMilliseconds() / 1000, 1)
  "$epoch,$ac,$($b.EstimatedChargeRemaining)" | Out-File -Append -Encoding ascii $log
  Start-Sleep -Seconds 15
}
[W.P]::SetThreadExecutionState([uint32]"0x80000000") | Out-Null
$ev = Get-WinEvent -FilterHashtable @{LogName='System'; StartTime=$start; Id=506,507} -ErrorAction SilentlyContinue |
      Where-Object { $_.ProviderName -eq 'Microsoft-Windows-Kernel-Power' } | Sort-Object TimeCreated
$pairs = @(); $enter = $null
foreach ($e in $ev) {
  $t = ([DateTimeOffset]$e.TimeCreated).ToUnixTimeSeconds()
  if ($e.Id -eq 506) { $enter = $t } elseif ($enter) { $pairs += @{start=$enter; end=$t}; $enter = $null }
}
ConvertTo-Json -InputObject @($pairs) | Out-File -Encoding ascii (Join-Path $Dir "standby.json")
"power monitor: xong, $($pairs.Count) lần standby"
