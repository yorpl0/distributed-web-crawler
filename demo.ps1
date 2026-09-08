param(
  [string]$Redis = "redis://localhost:6379/0",
  [int]$Pages = 1000
)

$ErrorActionPreference = "Stop"
$projectRoot = $PSScriptRoot
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$dockerExe = (Get-Command docker.exe -ErrorAction SilentlyContinue).Source
if (-not $dockerExe) {
  $dockerExe = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin\docker.exe"
}
if (-not (Test-Path -LiteralPath $python)) {
  throw "Python environment missing. Run ./setup.ps1 first."
}
if (-not (Test-Path -LiteralPath $dockerExe)) {
  throw "Docker CLI not found; start Docker Desktop before running this demo."
}

function Reset-Frontier([string]$Namespace) {
  & $python -m distributed_crawler --redis $Redis --namespace $Namespace --reset --max-pages 0 --idle-timeout 0.001 *> $null
  if ($LASTEXITCODE -ne 0) { throw "Could not reset namespace $Namespace" }
}

function Redis-Scalar([string[]]$RedisArgs) {
  return & $dockerExe exec faultcrawler-redis redis-cli --raw @RedisArgs
}

& $dockerExe exec faultcrawler-redis redis-cli PING | Out-Null
$fixture = Start-Process -FilePath $python -ArgumentList @(
  "-m", "distributed_crawler.fixture", "--pages", ($Pages + 50),
  "--fanout", "16", "--latency-ms", "20"
) -PassThru -WindowStyle Hidden
Start-Sleep -Milliseconds 700

try {
  "workers,pages,seconds,pages_per_second" | Set-Content (Join-Path $projectRoot "benchmark.csv")
  foreach ($workerCount in @(1, 2, 4)) {
    $namespace = "pycrawler:bench:$workerCount"
    Reset-Frontier $namespace
    $processes = @()
    $started = Get-Date
    for ($index = 0; $index -lt $workerCount; $index++) {
      $arguments = @(
        "-m", "distributed_crawler", "--redis", $Redis, "--namespace", $namespace,
        "--worker-id", "bench-$index", "--concurrency", "2", "--max-pages", $Pages,
        "--max-depth", $Pages, "--lease", "30", "--delay", "0"
      )
      if ($index -eq 0) { $arguments += @("--seed", "http://127.0.0.1:8088/?n=0") }
      $processes += Start-Process -FilePath $python -ArgumentList $arguments -PassThru -WindowStyle Hidden
    }

    $deadline = (Get-Date).AddSeconds(90)
    do {
      Start-Sleep -Milliseconds 200
      $fetched = [int](Redis-Scalar @("HLEN", "${namespace}:pages"))
    } while ($fetched -lt $Pages -and (Get-Date) -lt $deadline)
    if ($fetched -lt $Pages) { throw "Scaling run timed out at $fetched/$Pages pages" }
    $elapsed = ((Get-Date) - $started).TotalSeconds
    $rate = [math]::Round($fetched / $elapsed, 2)
    "$workerCount,$fetched,$([math]::Round($elapsed, 3)),$rate" | Add-Content (Join-Path $projectRoot "benchmark.csv")
    $processes | Where-Object { -not $_.HasExited } | Stop-Process -Force
  }

  $recoveryNamespace = "pycrawler:recovery"
  Reset-Frontier $recoveryNamespace
  $first = Start-Process -FilePath $python -ArgumentList @(
    "-m", "distributed_crawler", "--redis", $Redis, "--namespace", $recoveryNamespace,
    "--worker-id", "doomed", "--concurrency", "1", "--max-pages", "1",
    "--max-depth", "0", "--lease", "3", "--delay", "0",
    "--seed", "http://127.0.0.1:8088/slow"
  ) -PassThru -WindowStyle Hidden

  $claimDeadline = (Get-Date).AddSeconds(5)
  do {
    Start-Sleep -Milliseconds 100
    $processing = [int](Redis-Scalar @("HLEN", "${recoveryNamespace}:processing"))
  } while ($processing -lt 1 -and (Get-Date) -lt $claimDeadline)
  if ($processing -lt 1) { throw "Recovery worker never claimed its task" }
  Stop-Process -Id $first.Id -Force

  $survivor = Start-Process -FilePath $python -ArgumentList @(
    "-m", "distributed_crawler", "--redis", $Redis, "--namespace", $recoveryNamespace,
    "--worker-id", "survivor", "--concurrency", "1", "--max-pages", "1",
    "--max-depth", "0", "--lease", "3", "--delay", "0"
  ) -PassThru -WindowStyle Hidden
  $deadline = (Get-Date).AddSeconds(12)
  do {
    Start-Sleep -Milliseconds 200
    $recoveryDone = [int](Redis-Scalar @("HLEN", "${recoveryNamespace}:done"))
  } while ($recoveryDone -lt 1 -and (Get-Date) -lt $deadline)
  if (-not $survivor.HasExited) { Stop-Process -Id $survivor.Id -Force }
  $payload = Redis-Scalar @("HVALS", "${recoveryNamespace}:done")
  $attempts = ($payload | ConvertFrom-Json).attempts
  if ($recoveryDone -ne 1 -or $attempts -lt 1) {
    throw "Recovery proof failed: done=$recoveryDone attempts=$attempts"
  }
  "recovered=true`nattempts=$attempts`nkilled_worker=doomed`ncompleting_worker=survivor" | Set-Content (Join-Path $projectRoot "recovery.txt")

  Get-Content (Join-Path $projectRoot "benchmark.csv")
  Get-Content (Join-Path $projectRoot "recovery.txt")
} finally {
  if ($fixture -and -not $fixture.HasExited) { Stop-Process -Id $fixture.Id -Force }
}

