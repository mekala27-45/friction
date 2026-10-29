# Tag the release after every commit is pushed and the rederive shows no drift.
param([string]$Tag = "v0.1.0")
git fetch origin --tags
git tag -a $Tag -m "friction $Tag"
git push origin $Tag
