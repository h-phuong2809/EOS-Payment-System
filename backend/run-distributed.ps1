$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

$services = @(
  @{ Name = "NODE_A"; Profile = "node-a"; Log = "node-a.out.log" },
  @{ Name = "NODE_B"; Profile = "node-b"; Log = "node-b.out.log" },
  @{ Name = "NODE_C"; Profile = "node-c"; Log = "node-c.out.log" },
  @{ Name = "GATEWAY"; Profile = "gateway"; Log = "gateway.out.log" }
)

foreach ($service in $services) {
  $logPath = Join-Path $root $service.Log
  $arguments = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-Command",
    "cd '$root'; `$env:SPRING_PROFILES_ACTIVE='$($service.Profile)'; mvn spring-boot:run *> '$logPath'"
  )
  Start-Process -FilePath "powershell.exe" -ArgumentList $arguments -WindowStyle Hidden
  Write-Host "Started $($service.Name) with profile $($service.Profile). Log: $logPath"
  Start-Sleep -Seconds 5
}

Write-Host "Gateway: http://localhost:8080"
Write-Host "Nodes:   http://localhost:8081, http://localhost:8082, http://localhost:8083"
