# Python local limit-test results

This is a synthetic single-machine ceiling, not a public-internet throughput
claim. The recorded test used Python 3.13.1, Docker Redis 7.4, a local
zero-latency HTML fixture, 5,000 fetched pages per run, 32 links per page, and
four async crawl tasks per process. Each configuration ran three times. Timings
include Python process startup; RAM includes both the Windows `.venv` launcher
and its real CPython child process.

The saved run predates the atomic shared page-cap fix. Its raw rows therefore
show small overshoots from requests already in flight. The current implementation
checks completed plus in-flight work inside the atomic Redis claim script and has
a dedicated integration test. Regenerate the CSVs with `./limit-test.ps1` when
comparing the current code on another machine.

## Saturation point

| Processes | Async tasks | Median pages/s | Range | Speedup | Mean peak RAM | Worst p95 fetch |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4 | 404.98 | 267.52–406.05 | 1.00x | 56.84 MB | 4 ms |
| 2 | 8 | 611.50 | 426.35–612.52 | 1.51x | 113.80 MB | 4 ms |
| 4 | 16 | **757.78** | 634.50–767.69 | **1.87x** | 227.25 MB | 5 ms |
| 8 | 32 | 720.01 | 707.05–731.88 | 1.78x | 453.88 MB | 8 ms |

The useful local operating range is two to four Python processes. Four reached
the highest median throughput. Eight processes reduced throughput by about 5%
while doubling memory, showing that Redis coordination, duplicate checks, and
process scheduling had overtaken useful network concurrency.

The first trial at one, two, and four processes was slower than the following
two, consistent with cold imports, filesystem cache warm-up, and normal desktop
load. The median is therefore the headline statistic; raw timings and standard
deviations remain in the CSV files rather than being hidden.

RAM scaled almost perfectly linearly at approximately 57 MB per process on this
Windows/Python 3.13 environment.

## Politeness behavior

Eight processes and 32 async tasks were run against one host with the configured
25 ms global minimum delay. Throughput was extremely stable at a median of
**29.18 pages/s** (range 29.15–29.29). The theoretical request-start ceiling was
40/s, but robots requests, Redis round trips, event-loop scheduling, and process
startup added overhead. The important correctness property held: extra workers
did not bypass the single shared host limit.

## Correctness

- Nine unit and real-Redis integration tests passed on the verified source run.
- 100% of fetched fixture pages returned 2xx in every measured run.
- Every run ended with zero processing tasks, leases, and owners.
- The recovery demo killed a worker holding a live task; a survivor completed the
  requeued task with `attempts=1`.
- The historical 400-page politeness run finished at 431 because up to 32
  requests were already in flight. The current atomic page cap prevents this.

Reproduce with:

```powershell
./limit-test.ps1
```

Detailed trials are in `limit-benchmark.csv`; aggregated statistics are in
`limit-summary.csv`.

