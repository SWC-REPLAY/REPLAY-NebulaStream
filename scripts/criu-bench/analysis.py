"""Load one run's trial metrics into tidy tables and draw its plots."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

GIB = 1024**3


def load_trials(run_dir: Path) -> pd.DataFrame:
    """One row per trial with every scalar metric; nested per-query values go to load_queries."""
    rows: list[dict[str, Any]] = []
    for metrics_file in sorted((run_dir / "trials").glob("*/metrics.json")):
        metrics: dict[str, Any] = json.loads(metrics_file.read_text(encoding="utf-8"))
        rows.append({key: value for key, value in metrics.items() if not isinstance(value, (dict, list))})
    return pd.DataFrame(rows)


def load_queries(run_dir: Path) -> pd.DataFrame:
    """Completion time after submission per query, trial and role (original worker or shadow)."""
    rows: list[dict[str, Any]] = []
    for metrics_file in sorted((run_dir / "trials").glob("*/metrics.json")):
        metrics: dict[str, Any] = json.loads(metrics_file.read_text(encoding="utf-8"))
        if not metrics.get("success"):
            continue
        for query, seconds in metrics.get("per_query_seconds", {}).items():
            rows.append({"trial": metrics["trial"], "kind": metrics["kind"], "query": query, "role": "original", "seconds": seconds})
        if metrics["kind"] == "offload":
            rows.append({
                "trial": metrics["trial"], "kind": "offload", "query": metrics["offload_key"],
                "role": "shadow", "seconds": metrics["offload_query_seconds"],
            })
    return pd.DataFrame(rows, columns=["trial", "kind", "query", "role", "seconds"])


def summarize(trials: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Median, min and max per trial kind over successful trials."""
    if trials.empty:
        return pd.DataFrame()
    successful = trials[trials["success"].eq(True)]
    present = [column for column in columns if column in successful]
    return successful.groupby("kind")[present].agg(["median", "min", "max"]).T


def export_tables(run_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    trials, queries = load_trials(run_dir), load_queries(run_dir)
    trials.to_csv(run_dir / "trials.csv", index=False)
    queries.to_csv(run_dir / "queries.csv", index=False)
    return trials, queries


def successful_offloads(trials: pd.DataFrame) -> pd.DataFrame:
    if trials.empty or "kind" not in trials:
        return trials
    return trials[trials["kind"].eq("offload") & trials["success"].eq(True)].set_index("trial")


def placeholder(message: str) -> Figure:
    """Keeps the report rendering when a run has no successful trial of the plotted kind."""
    figure, axis = plt.subplots(figsize=(7, 1.5))
    axis.axis("off")
    axis.text(0.5, 0.5, message, ha="center", va="center")
    return figure


def plot_offload_query(trials: pd.DataFrame) -> Figure:
    """The selected query end to end: baseline on a busy worker versus offloaded and recorded."""
    successful = trials[trials["success"].eq(True)] if "success" in trials else trials
    if successful.empty or "offload_query_seconds" not in successful:
        return placeholder("No successful trial measured the selected query.")
    figure, axis = plt.subplots(figsize=(7, 4))
    successful.boxplot(column="offload_query_seconds", by="kind", ax=axis)
    figure.suptitle("")
    axis.set_title("Selected query: submission to checksum")
    axis.set_xlabel("")
    axis.set_ylabel("Seconds")
    return figure


def plot_offload_phases(trials: pd.DataFrame) -> Figure:
    """Where the offload time goes, per trial."""
    offloads = successful_offloads(trials)
    if offloads.empty:
        return placeholder("No successful offload trial.")
    # Stopping the other queries overlaps the attach and the recorded run, so it is not a stacked phase.
    phases = ["dump_seconds", "restore_seconds", "attach_seconds", "shadow_after_attach_seconds"]
    figure, axis = plt.subplots(figsize=(8, 4))
    offloads[phases].rename(columns=lambda name: name.removesuffix("_seconds").replace("_", " ")).plot.barh(stacked=True, ax=axis)
    axis.set_xlabel("Seconds")
    axis.set_title("Offload phases")
    return figure


def plot_cohort(queries: pd.DataFrame) -> Figure:
    """Per-query completion on the original worker, with and without the checkpoint."""
    original = queries[queries["role"].eq("original")]
    if original.empty:
        return placeholder("No per-query completion times.")
    figure, axis = plt.subplots(figsize=(14, 5))
    original.groupby(["query", "kind"])["seconds"].median().unstack().plot.bar(ax=axis, width=0.85)
    axis.set_ylabel("Median seconds after submission")
    axis.set_xlabel("")
    axis.set_title("Original worker: the other 31 queries")
    axis.tick_params(axis="x", rotation=70)
    return figure


def plot_costs(trials: pd.DataFrame) -> Figure:
    """Storage and memory the offload needs."""
    offloads = successful_offloads(trials)
    if offloads.empty:
        return placeholder("No successful offload trial.")
    costs = pd.DataFrame({
        "CRIU image": offloads["image_bytes"] / GIB,
        "UDB trace": offloads["recording_bytes"] / GIB,
        "shadow peak RSS": offloads["shadow_peak_rss_bytes"] / GIB,
    })
    figure, axis = plt.subplots(figsize=(8, 4))
    costs.plot.bar(ax=axis)
    axis.set_ylabel("GiB")
    axis.set_title("Offload storage and memory")
    axis.tick_params(axis="x", rotation=0)
    return figure
