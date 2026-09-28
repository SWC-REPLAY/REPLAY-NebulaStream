"""Reap processes in an isolated PID namespace until the trial ends."""

from __future__ import annotations

import os
import signal
from types import FrameType


def reap_children(_signal_number: int, _frame: FrameType | None) -> None:
    while True:
        try:
            child_pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if child_pid == 0:
            return


def main() -> None:
    signal.signal(signal.SIGCHLD, reap_children)
    print("READY", flush=True)
    while True:
        signal.pause()


if __name__ == "__main__":
    main()
