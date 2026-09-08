param(
  [string]$Redis = "redis://localhost:6379/0",
  [int]$Pages = 5000,
  [int[]]$WorkerCounts = @(1, 2, 4, 8),
  [int]$Concurrency = 4,
  [int]$Trials = 3
)

$ErrorActionPreference = "Stop"
$projectRoot = $PSScriptRoot
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$dockerExe = (Get-Command docker.exe -ErrorAction SilentlyContinue).Source
if (-not $dockerExe) {
  $dockerExe = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin\docker.exe"
}
if (-not (Test-Path -LiteralPath $python)) { throw "Run ./setup.ps1 first." }
if (-not (Test-Path -LiteralPath $dockerExe)) { throw "Start Docker Desktop first." }

function Redis-Command([string[]]$RedisArgs) {
  return & $dockerExe exec faultcrawler-redis redis-cli --raw @RedisArgs
}

function Reset-Frontier([string]$Namespace) {
  & $python -m distributed_crawler --redis $Redis --namespace $Namespace --reset --max-pages 0 --idle-timeout 0.001 *> $null
  if ($LASTEXITCODE -ne 0) { throw "Could not reset $Namespace" }
}

function Percentile([int[]]$Values, [double]$P) {
  if ($Values.Count -eq 0) { return 0 }
  $sorted = @($Values | Sort-Object)
  return $sorted[[math]::Max(0, [math]::Ceiling($P * $sorted.Count) - 1)]
}

function Run-Trial([int]$Trial, [int]$Workers, [string]$Mode, [int]$Target, [double]$Delay) {
  $namespace = "pycrawler:limit:${Mode}:$Workers"
  Reset-Frontier $namespace
  $processes = @()
  $peakMemory = 0L
  $sampleTick = 0
  $started = Get-Date
  try {
    for ($index = 0; $index -lt $Workers; $index++) {
      $arguments = @(
        "-m", "distributed_crawler", "--redis", $Redis, "--namespace", $namespace,
        "--worker-id", "$Mode-$index", "--concurrency", $Concurrency,
        "--max-pages", $Target, "--max-depth", ($Pages + 100),
        "--lease", "30", "--delay", $Delay
      )
      if ($index -eq 0) { $arguments += @("--seed", "http://127.0.0.1:8088/?n=0") }
      $processes += Start-Process -FilePath $python -ArgumentList $arguments -PassThru -WindowStyle Hidden
    }

    $deadline = (Get-Date).AddMinutes(3)
    do {
      $running = 0
      foreach ($process in $processes) {
        if (-not $process.HasExited) { $running++ }
      }
      if ($sampleTick % 10 -eq 0) {
        # On Windows a venv python.exe is a small launcher for the base Python
        # process. Count both, excluding the fixture and pre-existing processes.
        $memory = 0L
        foreach ($sample in @(Get-Process python -ErrorAction SilentlyContinue)) {
          if ($script:baselinePythonPIDs -notcontains $sample.Id) {
            $memory += $sample.WorkingSet64
          }
        }
        if ($memory -gt $peakMemory) { $peakMemory = $memory }
      }
      $sampleTick++
      if ($running -gt 0) { Start-Sleep -Milliseconds 100 }
    } while ($running -gt 0 -and (Get-Date) -lt $deadline)
    if ($running -gt 0) { throw "$Mode/$Workers timed out" }

    $elapsed = ((Get-Date) - $started).TotalSeconds
    $fetched = [int](Redis-Command @("HLEN", "${namespace}:pages"))
    $processing = [int](Redis-Command @("HLEN", "${namespace}:processing"))
    $leases = [int](Redis-Command @("ZCARD", "${namespace}:leases"))
    $owners = [int](Redis-Command @("HLEN", "${namespace}:owners"))
    if ($fetched -lt $Target) { throw "$Mode/$Workers stopped at $fetched/$Target" }
    if ($processing -ne 0 -or $leases -ne 0 -or $owners -ne 0) {
      throw "$Mode/$Workers leaked processing=$processing leases=$leases owners=$owners"
    }

    $latencies = @()
    $successes = 0
    foreach ($value in @(Redis-Command @("HVALS", "${namespace}:pages"))) {
      $parts = $value -split '\|'
      if ($parts.Count -ne 3) { continue }
      if ([int]$parts[0] -ge 200 -and [int]$parts[0] -lt 300) { $successes++ }
      $latencies += [int]$parts[2]
    }
    if ($successes -ne $fetched) { throw "$Mode/$Workers returned non-2xx fixture pages" }

    return [pscustomobject]@{
      mode = $Mode
      trial = $Trial
      processes = $Workers
      async_tasks = $Workers * $Concurrency
      delay_ms = [math]::Round($Delay * 1000)
      fetched = $fetched
      seconds = [math]::Round($elapsed, 3)
      pages_per_second = [math]::Round($fetched / $elapsed, 2)
      p50_fetch_ms = Percentile $latencies 0.50
      p95_fetch_ms = Percentile $latencies 0.95
      p99_fetch_ms = Percentile $latencies 0.99
      peak_crawler_mb = [math]::Round($peakMemory / 1MB, 2)
      http_2xx_percent = 100
      processing_after_run = $processing
      leases_after_run = $leases
      owners_after_run = $owners
    }
  } finally {
    $processes | Where-Object { -not $_.HasExited } | Stop-Process -Force
  }
}

& $dockerExe exec faultcrawler-redis redis-cli PING | Out-Null
$fixture = Start-Process -FilePath $python -ArgumentList @(
  "-m", "distributed_crawler.fixture", "--pages", ($Pages + 100),
  "--fanout", "32", "--latency-ms", "0"
) -PassThru -WindowStyle Hidden
Start-Sleep -Milliseconds 700
$script:baselinePythonPIDs = @(Get-Process python -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)

try {
  $results = @()
  for ($trial = 1; $trial -le $Trials; $trial++) {
    foreach ($workers in $WorkerCounts) {
      $results += Run-Trial $trial $workers "saturation" $Pages 0
    }
    $results += Run-Trial $trial 8 "politeness" 400 0.025
  }
  $results | Export-Csv -NoTypeInformation (Join-Path $projectRoot "limit-benchmark.csv")

  $summary = foreach ($group in ($results | Group-Object mode, processes)) {
    $rates = @($group.Group.pages_per_second | ForEach-Object { [double]$_ } | Sort-Object)
    $mean = ($rates | Measure-Object -Average).Average
    $sumSquares = 0.0
    foreach ($rate in $rates) { $sumSquares += [math]::Pow($rate - $mean, 2) }
    $sample = $group.Group[0]
    [pscustomobject]@{
      mode = $sample.mode
      processes = $sample.processes
      async_tasks = $sample.async_tasks
      trials = $rates.Count
      median_pages_per_second = [math]::Round($rates[[math]::Floor($rates.Count / 2)], 2)
      mean_pages_per_second = [math]::Round($mean, 2)
      stddev_pages_per_second = [math]::Round([math]::Sqrt($sumSquares / [math]::Max(1, $rates.Count - 1)), 2)
      min_pages_per_second = $rates[0]
      max_pages_per_second = $rates[-1]
      mean_peak_crawler_mb = [math]::Round(($group.Group.peak_crawler_mb | Measure-Object -Average).Average, 2)
      max_p95_fetch_ms = ($group.Group.p95_fetch_ms | Measure-Object -Maximum).Maximum
      http_2xx_percent = 100
      processing_after_run = 0
    }
  }
  $summary | Export-Csv -NoTypeInformation (Join-Path $projectRoot "limit-summary.csv")
  $summary | Format-Table
} finally {
  if ($fixture -and -not $fixture.HasExited) { Stop-Process -Id $fixture.Id -Force }
}

