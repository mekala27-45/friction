# Check the live API and the site from your own machine, a client separate from the build.
# Usage: .\deploy\verify.ps1 -Api https://<your-api>.vercel.app
param([Parameter(Mandatory = $true)][string]$Api)
$health = Invoke-RestMethod -Uri "$Api/health" -Method Get
Write-Output ("health: " + $health.status + ", models: " + (($health.models.PSObject.Properties | ForEach-Object { $_.Name + "=" + $_.Value.model }) -join ", "))
$site = Invoke-WebRequest -Uri "https://mekala27-45.github.io/friction/" -UseBasicParsing
Write-Output ("site: HTTP " + $site.StatusCode)
