function Invoke-Tool {
    param([string]$Program)
    & $Program @args
    if ($LASTEXITCODE -ne 0) { throw "$Program exited with code $LASTEXITCODE" }
}

function Get-EnvFile {
    if (Test-Path -LiteralPath .env) { return '.env' }
    return '.env.example'
}

function Get-StackSettings($file) {
    $settings = @{}
    $lines = Invoke-Tool docker compose --env-file .env.example --env-file $file config --environment
    foreach ($line in $lines) {
        if ($line -match '^(DATABASE_URL|KAFKA_BROKER|KAFKA_TOPIC|KAFKA_GROUP_ID)=(.+)$') {
            $settings[$Matches[1]] = $Matches[2]
        }
    }
    if (!$settings.DATABASE_URL -or !$settings.KAFKA_BROKER -or !$settings.KAFKA_TOPIC -or !$settings.KAFKA_GROUP_ID) {
        throw 'Set DATABASE_URL, KAFKA_BROKER, KAFKA_TOPIC and KAFKA_GROUP_ID in .env (see .env.example)'
    }
    return $settings
}

function Start-Stack {
    $file = Get-EnvFile
    Invoke-Tool docker compose --env-file $file up -d --wait --wait-timeout 120 postgres kafka | Out-Host

    $settings = Get-StackSettings $file
    Invoke-Tool go run github.com/jackc/tern/v2@v2.4.3 migrate --migrations migrations --conn-string $settings.DATABASE_URL | Out-Host
    Invoke-Tool docker compose --env-file $file exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic $settings.KAFKA_TOPIC --partitions 3 --replication-factor 1 | Out-Host
    return $settings
}
