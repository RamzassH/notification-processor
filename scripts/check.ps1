$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack.ps1"
Push-Location "$PSScriptRoot/.."
try {
    $unformatted = Invoke-Tool gofmt -l cmd internal
    if ($unformatted) { throw "Run go fmt ./... for: $($unformatted -join ', ')" }
    Invoke-Tool go vet ./...

    $env:COMPOSE_PROJECT_NAME = 'notification-check-' + [guid]::NewGuid().ToString('N')
    $env:POSTGRES_PORT = if ($env:TEST_POSTGRES_PORT) { $env:TEST_POSTGRES_PORT } else { '15433' }
    $env:KAFKA_PORT = if ($env:TEST_KAFKA_PORT) { $env:TEST_KAFKA_PORT } else { '19094' }
    $env:DATABASE_URL = "postgres://notifications:notifications_dev@127.0.0.1:$($env:POSTGRES_PORT)/notifications?sslmode=disable"
    $env:KAFKA_BROKER = "localhost:$($env:KAFKA_PORT)"
    try {
        $settings = Start-Stack
        $env:TEST_DATABASE_URL = $settings.DATABASE_URL
        $env:TEST_KAFKA_BROKER = $settings.KAFKA_BROKER
        $env:TEST_KAFKA_TOPIC = $settings.KAFKA_TOPIC
        $env:TEST_KAFKA_GROUP_ID = $settings.KAFKA_GROUP_ID
        $flags = @('-p', '1', '-count=1', '-v')
        if ($IsLinux) { $flags += '-race' }
        Invoke-Tool go test @flags ./...
    } finally {
        Invoke-Tool docker compose --env-file (Get-EnvFile) down --volumes
    }
} finally {
    Pop-Location
}
