$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack.ps1"
Push-Location "$PSScriptRoot/.."
try {
    $file = Get-EnvFile
    $settings = Get-StackSettings $file
    Get-Content -LiteralPath demo/events.jsonl | ForEach-Object { "demo:$_" } |
        docker compose --env-file $file exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic $settings.KAFKA_TOPIC --property parse.key=true --property key.separator=:
    if ($LASTEXITCODE -ne 0) { throw 'Kafka producer failed' }

    $filter = "event_id IN ('90000000-0000-4000-8000-000000000001', '90000000-0000-4000-8000-000000000002', '90000000-0000-4000-8000-000000000003', '90000000-0000-4000-8000-000000000004', '90000000-0000-4000-8000-000000000005', '90000000-0000-4000-8000-000000000006', '90000000-0000-4000-8000-000000000007')"
    $deadline = [DateTime]::UtcNow.AddSeconds(45)
    do {
        $count = Invoke-Tool docker compose --env-file $file exec -T postgres psql -XAt -U notifications -d notifications -c "SELECT count(*) FROM notifications WHERE $filter AND status IN ('sent','failed')"
        if ($count -eq '11') { break }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    Invoke-Tool docker compose --env-file $file exec -T postgres psql -X -U notifications -d notifications -c "SELECT user_id, event_type, channel, status, attempts FROM notifications WHERE $filter ORDER BY event_id, channel"
    if ($count -ne '11') { throw 'Expected eleven completed notifications. Check the service log.' }
} finally {
    Pop-Location
}
