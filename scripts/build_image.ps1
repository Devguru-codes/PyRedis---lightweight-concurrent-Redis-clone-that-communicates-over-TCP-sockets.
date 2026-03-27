param(
    [string]$ImageName = "pyredis",
    [string]$Tag = "latest"
)

$fullName = "$ImageName`:$Tag"
Write-Host "Building Docker image $fullName"
docker build -t $fullName .
