# Probe the hosts friction depends on, from your machine. Prints HTTP status only, never a secret.
foreach ($u in @("https://github.com", "https://console.neon.tech", "https://vercel.com", "https://mekala27-45.github.io/friction/")) {
  try { $r = Invoke-WebRequest -Uri $u -UseBasicParsing -Method Get -TimeoutSec 20; Write-Output "$u $($r.StatusCode)" }
  catch { Write-Output "$u unreachable: $($_.Exception.Message)" }
}
