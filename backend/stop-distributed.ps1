$ErrorActionPreference = "SilentlyContinue"

$ports = 8080, 8081, 8082, 8083
$connections = Get-NetTCPConnection -LocalPort $ports -State Listen
$processIds = $connections | Select-Object -ExpandProperty OwningProcess -Unique

foreach ($processId in $processIds) {
  if ($processId) {
    Stop-Process -Id $processId -Force
    Write-Host "Stopped process $processId"
  }
}

if (-not $processIds) {
  Write-Host "No distributed EOS services are listening on ports 8080-8083."
}
