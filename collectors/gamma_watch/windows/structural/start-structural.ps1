$feedRoot=Split-Path -Parent $MyInvocation.MyCommand.Path
$running=Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'powershell.exe' -and $_.CommandLine -match 'run-structural.ps1' }
if($running){Write-Output 'already-running';exit}
Start-Process powershell.exe -WindowStyle Hidden -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File', (Join-Path $feedRoot 'run-structural.ps1')) -RedirectStandardOutput (Join-Path $feedRoot 'collector.log') -RedirectStandardError (Join-Path $feedRoot 'error.log')
Write-Output 'started'
