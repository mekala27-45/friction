# Fetch the TabFormer archive on your machine if the build machine cannot reach git LFS, and check
# its digest against data/sources.lock.json before using it.
$url = "https://media.githubusercontent.com/media/IBM/TabFormer/main/data/credit_card/transactions.tgz"
Invoke-WebRequest -Uri $url -OutFile transactions.tgz
$expected = (Get-Content data/sources.lock.json | ConvertFrom-Json).tabformer.sha256
$actual = (Get-FileHash transactions.tgz -Algorithm SHA256).Hash.ToLower()
if ($actual -ne $expected) { throw "digest $actual does not match the lock $expected" }
Write-Output "transactions.tgz matches the lock"
