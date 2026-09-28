# criu-bench

Offload one running query from a busy NebulaStream worker to a CRIU-restored shadow and record it there with Undo live-record. The protocol is in [PLAN.md](../../PLAN.md).

Run everything from the repository root through direnv; each shell starts without the flake environment:

```bash
direnv exec . uv sync --locked
direnv exec . python scripts/criu-bench/bench.py check
direnv exec . python scripts/criu-bench/bench.py report [--reps 3] [--delay 15] [--run-id ID] [--keep-artifacts]
```

| File | Role |
| --- | --- |
| `bench.py` | CLI and trial orchestration: namespaces, workers, CRIU, trigger, metrics, cleanup |
| `runner.py` | Parses the `.test` files, writes nes-cli topologies, submits, stops, and checks queries |
| `analysis.py` | Loads `metrics.json` into tables and draws the plots |
| `report.qmd` | Executable notebook: runs the trials, analyzes, plots, cleans up |
| `namespace_init.py` | PID 1 of each worker namespace; reaps children |

Results go to `build/criu-bench/RUN_ID/`. `live-record` must be on `PATH` (or `UDB_BINARY_PATH` must name it); `criu` comes from the flake.
