#!/usr/bin/env python3
import subprocess
import socket
import time
import os

WORKER_BIN = "cmake-build-debug/nes-single-node-worker/nes-single-node-worker"
REPL_BIN   = "cmake-build-debug/nes-frontend/apps/nes-repl"
UDB_REC_BIN = "live-record"
AIO_PRELOAD = "/home/vanhoa/Documents/research/Undo-Suite-x86-9.2.2/tools/libundo_aio_preload_x64.so"

DEMO_CSV = "demo-input.csv"
DEMO_SQL = "demo.sql"

SQL = """\
CREATE LOGICAL SOURCE demo(value UINT64);
CREATE PHYSICAL SOURCE FOR demo TYPE File SET('./demo-input.csv' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER);
CREATE SINK result(demo.value UINT64) TYPE File SET('./demo-output.csv' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.OUTPUT_FORMAT);
SELECT value FROM demo INTO result;
"""

def build():
    print("[*] Building targets...")
    subprocess.run([
        "cmake", "--build", "cmake-build-debug", "-j",
        "--target", "nes-single-node-worker", "nes-repl"
    ], check=True)

def start_worker(exec_compiled : bool = False):
    """Launch the worker standalone (without recording)."""
    print("[*] Starting nes-single-node-worker...")
    env = os.environ.copy()
    env["LD_PRELOAD"] = AIO_PRELOAD
    if (exec_compiled):
        EXEC_MODE = "COMPILER"
    else:
        EXEC_MODE = "INTERPRETER"
    proc = subprocess.Popen(
            [WORKER_BIN, f"--worker.default_query_execution.execution_mode={EXEC_MODE}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    return proc

def start_worker_recorded():
    """Launch the worker under live-record from the very beginning."""
    print("[*] Starting nes-single-node-worker under live-record...")
    env = os.environ.copy()
    env["LD_PRELOAD"] = AIO_PRELOAD
    proc = subprocess.Popen(
        [UDB_REC_BIN, "--verbose", "--recording-file", "recording.undo", WORKER_BIN],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    return proc, None  # no separate recorder proc needed

def attach_recorder(pid):
    """Attach live-record to an already running worker."""
    print(f"[*] Attaching live-record to PID {pid}...")
    proc = subprocess.Popen(
        [UDB_REC_BIN, "--verbose", "--recording-file", "recording.undo", "--pid", str(pid)],
    )
    time.sleep(2)  # wait for live-record to attach
    print("[*] live-record attached.")
    return proc

def wait_for_worker(proc, port=8080, timeout=30):
    """Poll until the worker is listening on port or timeout."""
    print(f"[*] Waiting for worker to be ready on port {port}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("Worker exited unexpectedly")
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                print(f"[*] Worker is ready! PID: {proc.pid}")
                return proc.pid
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"Worker did not become ready within {timeout}s")

def prepare_inputs():
    print("[*] Writing demo-input.csv and demo.sql...")
    with open(DEMO_CSV, "w") as f:
        for i in range(1, 100000):
            f.write(f"{i}\n")
    with open(DEMO_SQL, "w") as f:
        f.write(SQL)

def run_repl():
    print("[*] Submitting query via nes-repl...")
    with open(DEMO_SQL, "r") as sql_file:
        result = subprocess.run(
            [REPL_BIN, "-s", "localhost:8080"],
            stdin=sql_file,
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        print(f"[!] nes-repl stderr:\n{result.stderr}")
    else:
        print("[*] Query submitted successfully.")
        print(result.stdout)

def shutdown(recorder_proc, worker_proc):
    print("\n[*] Stopping worker (live-record will save recording automatically)...")
    worker_proc.terminate()
    worker_proc.wait()
    if recorder_proc is not None:
        recorder_proc.wait()

def main():
    build()
    prepare_inputs()

    # Choose one:
    # Option A: record from the very beginning (more snapshots, captures startup)
    # worker_proc, recorder_proc = start_worker_recorded()
    # pid = wait_for_worker(worker_proc, timeout=30)

    # Option B: start worker normally, then attach recorder after it's ready
    worker_proc = start_worker(exec_compiled=True)
    pid = wait_for_worker(worker_proc, timeout=30)
    recorder_proc = attach_recorder(pid)

    run_repl()

    print("[*] Query done. Worker still running under live-record.")
    print("    Press Ctrl+C to save recording and stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(recorder_proc, worker_proc)

if __name__ == "__main__":
    main()
