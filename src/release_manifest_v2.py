from __future__ import annotations

import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .common_v2 import ensure_dirs, get_paths


ARTIFACTS = {
    "dashboard": lambda p: p.outputs_dashboard / "grid-electrification-command-center.html",
    "validation_summary": lambda p: p.outputs_reports / "validation_summary.json",
    "validation_report": lambda p: p.outputs_reports / "validation_report.md",
    "scoring_table": lambda p: p.data_processed / "intervention_scoring_table.csv",
    "ranking_final": lambda p: p.data_processed / "intervention_ranking_final.csv",
    "scenario_summary": lambda p: p.data_processed / "scenario_summary_v2.csv",
}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_meta(path: Path) -> dict:
    if not path.exists():
        return {
            "exists": False,
            "size_bytes": 0,
            "sha256": None,
            "updated_utc": None,
        }
    stat = path.stat()
    return {
        "exists": True,
        "size_bytes": int(stat.st_size),
        "sha256": _sha256(path),
        "updated_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


def build_release_manifest_v2() -> dict:
    paths = ensure_dirs(get_paths())

    artifacts = {
        key: {
            "path": str(resolver(paths)),
            **_file_meta(resolver(paths)),
        }
        for key, resolver in ARTIFACTS.items()
    }

    summary_path = paths.outputs_reports / "validation_summary.json"
    if summary_path.exists():
        validation_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    else:
        validation_summary = {}

    scoring_path = paths.data_processed / "intervention_scoring_table.csv"
    if scoring_path.exists():
        scoring = pd.read_csv(scoring_path, usecols=["zona_id", "investment_priority_score"])
        top_zone = (
            scoring.sort_values("investment_priority_score", ascending=False)
            .head(1)["zona_id"]
            .iloc[0]
            if not scoring.empty
            else None
        )
        n_scoring_rows = int(len(scoring))
    else:
        top_zone = None
        n_scoring_rows = 0

    manifest = {
        "manifest_version": "1.0",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "release_readiness": validation_summary.get("release_readiness", {}),
        "validation_status": validation_summary.get("overall_status", "N/A"),
        "confidence_level": validation_summary.get("confidence_level", "N/A"),
        "row_counts": validation_summary.get("row_counts", {}),
        "top_zone_by_priority": top_zone,
        "n_scoring_rows": n_scoring_rows,
        "artifacts": artifacts,
    }

    out = paths.outputs_reports / "release_manifest.json"
    out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    payload = build_release_manifest_v2()
    print(payload["validation_status"])
