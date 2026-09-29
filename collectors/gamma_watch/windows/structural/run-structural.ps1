$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$feedRoot=Split-Path -Parent $MyInvocation.MyCommand.Path
$pia='C:\Windows\assembly\GAC_MSIL\Microsoft.Office.Interop.Excel\15.0.0.0__71e9bce111e9429c\Microsoft.Office.Interop.Excel.dll'
Add-Type -Path $pia
Add-Type -Path (Join-Path $feedRoot 'structural-collector.cs') -ReferencedAssemblies @($pia,'System.Windows.Forms','System.Web.Extensions')
[StructuralCollector]::Run($feedRoot)
