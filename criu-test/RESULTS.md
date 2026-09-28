# Rootless CRIU result

On 2026-09-27, CRIU 4.1.1 `check --unprivileged` reported that `/proc/sys/kernel/sem_next_id` was inaccessible in the root-mapped user namespace. An actual rootless smoke test nevertheless dumped and restored a session-leading process successfully. The harness records the check as a diagnostic and uses the dump/restore result as its preflight gate. [Compact evidence](evidence/rootless-preflight) includes both CRIU logs and the namespace capability mask.

The Nix CRIU executable is a wrapper script without file capabilities, and the host user lacks `CAP_SETFCAP`. The root-mapped namespace has effective `CAP_CHECKPOINT_RESTORE`; adding host file capabilities is unavailable here and is not needed for the successful smoke test.

The first rootless worker pilot also passed: `Nexmark_with_varsized:01` was active at the checkpoint, and original and shadow each produced one exact expected checksum record. The dump took 1.32 s and produced 1,150,907,963 bytes; restore took 0.94 s. [Pilot metrics and verification](evidence/pilot) are preserved. No rootless 32-query result is claimed yet. The prior Docker trial matrix remains on `criu-benchmark-bk` at `017bec2392`.
