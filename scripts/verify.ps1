# One-command end-to-end verification of the local pipeline.
# Usage:  powershell -ExecutionPolicy Bypass -File scripts/verify.ps1
# Prints a PASS/FAIL table and exits 1 if any check fails. Nothing is modified.

$ErrorActionPreference = "SilentlyContinue"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$results = @()

function Add-Result {
    param([string]$Check, [bool]$Ok, [string]$Detail)
    $script:results += [pscustomobject]@{
        Check  = $Check
        Result = $(if ($Ok) { "PASS" } else { "FAIL" })
        Detail = $Detail
    }
}

# 1. All nine long-running services up (seven with a passing healthcheck).
$svc = @(docker compose ps --format "{{.Service}}|{{.Status}}" 2>&1 | ForEach-Object { "$_" })
$expected = @("airflow-db", "airflow-scheduler", "airflow-webserver", "dashboard",
              "kafka", "lake", "producer", "spark", "warehouse")
$noHealth = @("producer", "spark")
$badSvc = @()
foreach ($name in $expected) {
    $line = $svc | Where-Object { $_ -like "$name|*" } | Select-Object -First 1
    if (-not $line) { $badSvc += "$name missing"; continue }
    $status = $line.Substring($line.IndexOf("|") + 1)
    if ($noHealth -contains $name) {
        if ($status -notmatch "^Up") { $badSvc += "$name=$status" }
    } elseif ($status -notmatch "\(healthy\)") {
        $badSvc += "$name=$status"
    }
}
Add-Result "services up/healthy" ($badSvc.Count -eq 0) `
    $(if ($badSvc.Count -eq 0) { "9/9 up (7 healthy + producer/spark)" } else { $badSvc -join "; " })

# 2. Kafka has messages: end offset of crypto.prices > 0.
$kafka = @(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh `
    --bootstrap-server kafka:9092 --topic crypto.prices 2>&1 | ForEach-Object { "$_" })
$kafkaCode = $LASTEXITCODE
$offset = $null
foreach ($line in $kafka) {
    if ($line -match "crypto\.prices:\d+:(\d+)") { $offset = [int64]$Matches[1] }
}
Add-Result "kafka has messages" ($kafkaCode -eq 0 -and $null -ne $offset -and $offset -gt 0) `
    "end offset=$offset (exit=$kafkaCode)"

# 3. Lake has raw tick objects (data partitions, not just _spark_metadata).
$lake = @(docker compose run --rm --no-deps -v "${repoRoot}/scripts:/app/scripts" `
    producer python scripts/lake_ls.py raw/ticks/event_date= 5 2>&1 | ForEach-Object { "$_" })
$lakeCode = $LASTEXITCODE
$keys = @($lake | Where-Object { $_ -match "s3://\S+/raw/ticks/event_date=" })
Add-Result "lake raw/ticks objects" ($lakeCode -eq 0 -and $keys.Count -gt 0) `
    "$($keys.Count) event_date partition object(s) listed (exit=$lakeCode)"

# 4. Warehouse rows exist and the newest ingest is at most 5 minutes old.
$sql = "select count(*) || '|' || coalesce(to_char(max(ingested_at),'YYYY-MM-DD HH24:MI:SS'),'none') || '|' || coalesce(round(abs(extract(epoch from (now()-max(ingested_at)))))::text,'na') from raw.price_windows;"
$wh = @(docker compose exec -T warehouse psql -U warehouse -d warehouse -tAc $sql 2>&1 | ForEach-Object { "$_" })
$whCode = $LASTEXITCODE
$row = $wh | Where-Object { $_ -match "^\s*\d+\|" } | Select-Object -First 1
if ($row) {
    $parts = $row.Trim().Split("|")
    $cnt = [int64]$parts[0]
    $newest = $parts[1].Trim()
    if ($parts[2] -match "^\d+$") { $age = [int]$parts[2] } else { $age = 999999 }
} else {
    $cnt = 0; $newest = "none"; $age = 999999
}
Add-Result "raw.price_windows fresh" ($whCode -eq 0 -and $cnt -gt 0 -and $age -le 300) `
    "rows=$cnt max_ingested_at=$newest age_sec=$age (exit=$whCode)"

# 5. dbt data tests all pass.
$dbt = @(docker compose exec -T -w /opt/airflow/dbt airflow-scheduler `
    /home/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/dbt 2>&1 | `
    ForEach-Object { "$_" -replace "\x1b\[[0-9;]*m", "" })
$dbtCode = $LASTEXITCODE
$summary = @($dbt | Where-Object { $_ -match "Done\. PASS=" } | Select-Object -Last 1)
$dbtOk = ($dbtCode -eq 0 -and $summary.Count -gt 0 -and $summary[0] -notmatch "ERROR=[1-9]")
Add-Result "dbt test passes" $dbtOk `
    $(if ($summary.Count) { $summary[0].Trim() } else { "no dbt summary (exit=$dbtCode)" })

# 6. Streaming analytics DAG is registered and unpaused.
$dags = @(docker compose exec -T airflow-webserver airflow dags list 2>&1 | ForEach-Object { "$_" })
$dagsCode = $LASTEXITCODE
$dagLine = $dags | Where-Object { $_ -match "^\s*streaming_analytics\s*\|" } | Select-Object -First 1
Add-Result "DAG exists & unpaused" ($dagsCode -eq 0 -and $null -ne $dagLine -and $dagLine -match "\bFalse\b") `
    $(if ($dagLine) { $dagLine.Trim() } else { "DAG not found (exit=$dagsCode)" })

# 7. Streamlit dashboard answers on port 8501 and its recent logs have no import/traceback errors.
$dashCode = 0
try {
    $resp = Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing -TimeoutSec 10
    $dashCode = [int]$resp.StatusCode
} catch { $dashCode = 0 }
$dashLogs = @(docker compose logs --tail 50 dashboard 2>&1 | ForEach-Object { "$_" })
$logErrors = @($dashLogs | Where-Object { $_ -match "Traceback|ModuleNotFoundError" })
$dashOk = ($dashCode -eq 200 -and $logErrors.Count -eq 0)
$logDetail = if ($logErrors.Count -eq 0) { "logs clean (tail 50)" } else { "logs contain: " + ($logErrors -join "; ") }
Add-Result "dashboard HTTP 200" $dashOk "HTTP $dashCode on port 8501; $logDetail"

# 8. pipeline_metrics view reports ingestion lag within the 5-minute threshold.
$lagSql = "select coalesce(round(ingestion_lag_seconds::numeric)::text,'na') || '|' || coalesce(to_char(latest_ingested_at,'YYYY-MM-DD HH24:MI:SS'),'none') from marts.pipeline_metrics;"
$metrics = @(docker compose exec -T warehouse psql -U warehouse -d warehouse -tAc $lagSql 2>&1 | ForEach-Object { "$_" })
$metricsCode = $LASTEXITCODE
$mrow = $metrics | Where-Object { $_ -match "\|" } | Select-Object -First 1
if ($mrow) {
    $mparts = $mrow.Trim().Split("|")
    if ($mparts[0] -match "^\d+$") { $lagSec = [int]$mparts[0] } else { $lagSec = 999999 }
    $mLatest = $mparts[1].Trim()
} else {
    $lagSec = 999999; $mLatest = "none"
}
Add-Result "pipeline_metrics lag <= 300s" ($metricsCode -eq 0 -and $lagSec -le 300) `
    "latest_ingested_at=$mLatest lag_sec=$lagSec limit=300 (exit=$metricsCode)"

$results | Format-Table -AutoSize -Wrap
$failed = @($results | Where-Object { $_.Result -eq "FAIL" })
Write-Output "SUMMARY: $($results.Count - $failed.Count)/$($results.Count) checks passed"
if ($failed.Count -gt 0) { exit 1 }
exit 0
