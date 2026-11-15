$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack.ps1"
Push-Location "$PSScriptRoot/.."
try {
    $settings = Start-Stack
    $env:DATABASE_URL = $settings.DATABASE_URL
    $env:KAFKA_BROKER = $settings.KAFKA_BROKER
    $env:KAFKA_TOPIC = $settings.KAFKA_TOPIC
    $env:KAFKA_GROUP_ID = $settings.KAFKA_GROUP_ID
    & go run ./cmd/notification-processor
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
