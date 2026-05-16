<#
.SYNOPSIS
Read-only preflight for Windows ANIK/ANIK-PC access to the home Mac over Tailscale.

.DESCRIPTION
This script checks local Windows prerequisites and network reachability for the
Windows-PC-to-home-Mac path. It does not start services, edit firewall rules,
enable RDP, authorize Tailscale machines, or write SSH keys.

Exit code 0 means no FAIL checks were found. WARN checks still need review.
Exit code 1 means one or more required checks failed.
#>

[CmdletBinding()]
param(
    [string]$MacTailscaleIp = "100.120.20.120",
    [string]$MacSshUser = "anikdang",
    [string]$ExpectedWindowsTailscaleIp = "100.94.80.45",
    [int]$TimeoutSeconds = 5,
    [switch]$SkipSshProbe,
    [switch]$ExpectRdpOpen,
    [string]$JsonReportPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-Check {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][ValidateSet("PASS", "WARN", "FAIL", "INFO")][string]$Status,
        [Parameter(Mandatory = $true)][string]$Detail,
        [string]$Remediation = ""
    )

    [pscustomobject]@{
        Name = $Name
        Status = $Status
        Detail = $Detail
        Remediation = $Remediation
    }
}

function Test-TailnetIp {
    param([Parameter(Mandatory = $true)][string]$IpAddress)

    $octets = $IpAddress -split "\."
    if ($octets.Count -ne 4) {
        return $false
    }

    try {
        $first = [int]$octets[0]
        $second = [int]$octets[1]
    }
    catch {
        return $false
    }

    return ($first -eq 100 -and $second -ge 64 -and $second -le 127)
}

function Resolve-CommandPath {
    param(
        [Parameter(Mandatory = $true)][string]$CommandName,
        [string[]]$FallbackPaths = @()
    )

    $command = Get-Command $CommandName -ErrorAction SilentlyContinue
    if ($null -ne $command) {
        return $command.Source
    }

    foreach ($path in $FallbackPaths) {
        if (Test-Path -LiteralPath $path) {
            return $path
        }
    }

    return $null
}

function Invoke-CapturedCommand {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList
    )

    $output = & $FilePath @ArgumentList 2>&1
    [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        Output = ($output | ForEach-Object { $_.ToString() }) -join "`n"
    }
}

function Test-TcpPort {
    param(
        [Parameter(Mandatory = $true)][string]$HostName,
        [Parameter(Mandatory = $true)][int]$Port,
        [int]$TimeoutMilliseconds = 5000
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $async = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne($TimeoutMilliseconds, $false)) {
            return $false
        }
        $client.EndConnect($async)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
}

function Get-TailscaleStatus {
    param([Parameter(Mandatory = $true)][string]$TailscalePath)

    $status = Invoke-CapturedCommand -FilePath $TailscalePath -ArgumentList @("status", "--json")
    if ($status.ExitCode -ne 0) {
        return [pscustomobject]@{
            Ok = $false
            Error = $status.Output.Trim()
            Data = $null
        }
    }

    try {
        return [pscustomobject]@{
            Ok = $true
            Error = ""
            Data = ($status.Output | ConvertFrom-Json)
        }
    }
    catch {
        return [pscustomobject]@{
            Ok = $false
            Error = $_.Exception.Message
            Data = $null
        }
    }
}

$checks = [System.Collections.Generic.List[object]]::new()
$startedAt = Get-Date -Format "o"
$macTarget = "$MacSshUser@$MacTailscaleIp"
$timeoutMs = [Math]::Max(1, $TimeoutSeconds) * 1000

$checks.Add((New-Check `
    -Name "Mac Tailscale IP format" `
    -Status $(if (Test-TailnetIp -IpAddress $MacTailscaleIp) { "PASS" } else { "FAIL" }) `
    -Detail "Mac target is $MacTailscaleIp." `
    -Remediation "Use the Mac mini Tailscale IP from the current tailnet inventory, not a LAN or public IP."))

$tailscalePath = Resolve-CommandPath `
    -CommandName "tailscale.exe" `
    -FallbackPaths @("C:\Program Files\Tailscale\tailscale.exe", "C:\Program Files (x86)\Tailscale\tailscale.exe")

if ($null -eq $tailscalePath) {
    $checks.Add((New-Check `
        -Name "Tailscale CLI" `
        -Status "FAIL" `
        -Detail "tailscale.exe was not found in PATH or the default install locations." `
        -Remediation "Install Tailscale and sign in manually; do not run automated auth from this preflight."))
}
else {
    $checks.Add((New-Check `
        -Name "Tailscale CLI" `
        -Status "PASS" `
        -Detail "Found $tailscalePath."))

    $tailscaleStatus = Get-TailscaleStatus -TailscalePath $tailscalePath
    if (-not $tailscaleStatus.Ok) {
        $checks.Add((New-Check `
            -Name "Tailscale status" `
            -Status "FAIL" `
            -Detail "tailscale status --json failed: $($tailscaleStatus.Error)" `
            -Remediation "Open the Tailscale UI, sign in, and make sure this device is authorized in the admin console."))
    }
    else {
        $self = $tailscaleStatus.Data.Self
        $tailscaleIps = @()
        if ($null -ne $self -and $null -ne $self.TailscaleIPs) {
            $tailscaleIps = @($self.TailscaleIPs)
        }
        $ipDetail = if ($tailscaleIps.Count -gt 0) { $tailscaleIps -join ", " } else { "none reported" }
        $online = if ($null -ne $self -and $self.PSObject.Properties.Name -contains "Online") { [bool]$self.Online } else { $true }

        $checks.Add((New-Check `
            -Name "Tailscale local status" `
            -Status $(if ($online -and $tailscaleIps.Count -gt 0) { "PASS" } else { "FAIL" }) `
            -Detail "Local Tailscale IPs: $ipDetail." `
            -Remediation "If no tailnet IP is listed, sign in and approve the Windows machine in the Tailscale admin console."))

        if ($ExpectedWindowsTailscaleIp -ne "") {
            $hasExpectedIp = $tailscaleIps -contains $ExpectedWindowsTailscaleIp
            $checks.Add((New-Check `
                -Name "Expected Windows tailnet IP" `
                -Status $(if ($hasExpectedIp) { "PASS" } else { "WARN" }) `
                -Detail "Expected $ExpectedWindowsTailscaleIp; current IPs: $ipDetail." `
                -Remediation "If this is a replacement machine or reauth changed the IP, update the runbook and coordinator notes before registering a worker."))
        }
    }
}

$sshPath = Resolve-CommandPath -CommandName "ssh.exe" -FallbackPaths @("C:\Windows\System32\OpenSSH\ssh.exe")
if ($null -eq $sshPath) {
    $checks.Add((New-Check `
        -Name "OpenSSH client" `
        -Status "FAIL" `
        -Detail "ssh.exe was not found." `
        -Remediation "Install the Windows OpenSSH Client optional feature before using SSH to the Mac mini."))
}
else {
    $checks.Add((New-Check `
        -Name "OpenSSH client" `
        -Status "PASS" `
        -Detail "Found $sshPath."))
}

$pingOk = Test-Connection -ComputerName $MacTailscaleIp -Count 1 -Quiet -ErrorAction SilentlyContinue
$checks.Add((New-Check `
    -Name "Mac ICMP over Tailscale" `
    -Status $(if ($pingOk) { "PASS" } else { "WARN" }) `
    -Detail $(if ($pingOk) { "Ping to $MacTailscaleIp succeeded." } else { "Ping to $MacTailscaleIp failed or ICMP is blocked." }) `
    -Remediation "A failed ping is not fatal if TCP checks pass; otherwise verify both devices are online in Tailscale."))

$sshPortOpen = Test-TcpPort -HostName $MacTailscaleIp -Port 22 -TimeoutMilliseconds $timeoutMs
$checks.Add((New-Check `
    -Name "Mac SSH TCP 22" `
    -Status $(if ($sshPortOpen) { "PASS" } else { "FAIL" }) `
    -Detail $(if ($sshPortOpen) { "TCP 22 is reachable at $MacTailscaleIp." } else { "TCP 22 is not reachable at $MacTailscaleIp." }) `
    -Remediation "On the Mac mini, enable Remote Login for $MacSshUser and confirm Tailscale is connected."))

if ($sshPortOpen -and $null -ne $sshPath -and -not $SkipSshProbe) {
    $sshProbe = Invoke-CapturedCommand `
        -FilePath $sshPath `
        -ArgumentList @(
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=$TimeoutSeconds",
            "-o", "StrictHostKeyChecking=accept-new",
            $macTarget,
            "hostname"
        )
    $sshOutput = $sshProbe.Output.Trim()
    $checks.Add((New-Check `
        -Name "Mac SSH key probe" `
        -Status $(if ($sshProbe.ExitCode -eq 0) { "PASS" } else { "WARN" }) `
        -Detail $(if ($sshProbe.ExitCode -eq 0) { "Non-interactive SSH to $macTarget returned: $sshOutput" } else { "SSH port is open, but non-interactive auth failed: $sshOutput" }) `
        -Remediation "Create or reuse a Windows user SSH key and manually add its public key to the Mac mini authorized_keys after verifying the key fingerprint."))
}
elseif ($SkipSshProbe) {
    $checks.Add((New-Check `
        -Name "Mac SSH key probe" `
        -Status "INFO" `
        -Detail "Skipped by -SkipSshProbe."))
}

$mstscPath = Resolve-CommandPath -CommandName "mstsc.exe" -FallbackPaths @("C:\Windows\System32\mstsc.exe")
$checks.Add((New-Check `
    -Name "Windows RDP client" `
    -Status $(if ($null -ne $mstscPath) { "PASS" } else { "WARN" }) `
    -Detail $(if ($null -ne $mstscPath) { "Found $mstscPath." } else { "mstsc.exe was not found." }) `
    -Remediation "RDP is only a client-side fallback here; Win 11 Home does not provide an inbound RDP server. Use RustDesk for full GUI control."))

$rdpPortOpen = Test-TcpPort -HostName $MacTailscaleIp -Port 3389 -TimeoutMilliseconds $timeoutMs
if ($ExpectRdpOpen) {
    $checks.Add((New-Check `
        -Name "Mac RDP TCP 3389" `
        -Status $(if ($rdpPortOpen) { "PASS" } else { "FAIL" }) `
        -Detail $(if ($rdpPortOpen) { "TCP 3389 is reachable at $MacTailscaleIp." } else { "TCP 3389 is not reachable at $MacTailscaleIp." }) `
        -Remediation "Only expect this to pass if a deliberate RDP gateway/server exists on the Mac side."))
}
else {
    $checks.Add((New-Check `
        -Name "Mac RDP TCP 3389" `
        -Status "INFO" `
        -Detail $(if ($rdpPortOpen) { "TCP 3389 is open at $MacTailscaleIp; confirm this is intentional." } else { "TCP 3389 is closed at $MacTailscaleIp, which is expected for the Mac mini path." }) `
        -Remediation "Do not enable RDP just to satisfy this preflight; use RustDesk or Mac Screen Sharing for GUI access."))
}

$finishedAt = Get-Date -Format "o"
$failCount = @($checks | Where-Object { $_.Status -eq "FAIL" }).Count
$warnCount = @($checks | Where-Object { $_.Status -eq "WARN" }).Count

$report = [pscustomobject]@{
    StartedAt = $startedAt
    FinishedAt = $finishedAt
    MacTarget = $macTarget
    MacTailscaleIp = $MacTailscaleIp
    ExpectedWindowsTailscaleIp = $ExpectedWindowsTailscaleIp
    FailCount = $failCount
    WarnCount = $warnCount
    Checks = $checks
}

foreach ($check in $checks) {
    $prefix = "[{0}]" -f $check.Status
    Write-Host "$prefix $($check.Name): $($check.Detail)"
    if ($check.Remediation -ne "") {
        Write-Host "      Next: $($check.Remediation)"
    }
}

Write-Host ""
Write-Host "Summary: $failCount fail, $warnCount warn."

if ($JsonReportPath -ne "") {
    $parent = Split-Path -Parent $JsonReportPath
    if ($parent -ne "" -and -not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    $report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $JsonReportPath -Encoding UTF8
    Write-Host "Wrote JSON report to $JsonReportPath"
}

if ($failCount -gt 0) {
    exit 1
}

exit 0
