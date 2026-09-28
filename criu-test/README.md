# Rootless CRIU benchmark

First load the repository environment with direnv or `nix develop .`, then run `uv sync --locked`. The [benchmark README](../scripts/criu-bench/README.md) gives the one-command Quarto workflow, render-only command, and manual harness options. See [PLAN.md](../PLAN.md) for the workload and acceptance criteria. It uses `unshare` and `nsenter`; Docker and sudo are not part of the active workflow.

Run `preflight` before any full trial. It records `criu check` and tests a real dump and restore in the same rootless namespace model. Its JSON and logs stay under ignored `build/criu-bench/preflight-*`; compact text evidence is in [evidence/rootless-preflight](evidence/rootless-preflight). On this host, the check warns about `sem_next_id` but the actual smoke test passes.

The previous Docker-based feasibility work remains on the `criu-benchmark-bk` branch, including the retired `namespace.sh`, `run-stage.sh`, and trial outputs. Local verbose historical logs are in ignored `build/criu-bench/historical-evidence/`.
