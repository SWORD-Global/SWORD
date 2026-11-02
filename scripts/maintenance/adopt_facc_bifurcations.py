#!/usr/bin/env python3
"""Materialize and select the validated FACC bifurcation working product.

This never opens a SWORD database or overwrites a published export. It selects
the frozen 26 October global run as the working FACC authority, preserving its
unsplit parent totals and SWORD width rules. Historical candidates stay sealed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
VARIANT = ROOT / "output/facc_detection/variants/rule0021_conservative_forks_fixed_total_20261026"
PARENT = ROOT / "output/facc_detection/variants/rule0021_credit_caps_ghost_candidate_20261021/runs/global_candidate"
RUN = VARIANT / "runs/global_candidate"
TARGET = ROOT / "data/derived/facc/bifurcation_20261026"
AUTHORITY = ROOT / "config/facc_adopted.json"
RECORD = ROOT / "output/facc_detection/adoptions/bifurcation_20261026"
REGIONS = ("AF", "AS", "EU", "NA", "OC", "SA")
RENAMES = {"facc_head_km2": "facc_head", "facc_tail_km2": "facc", "facc_total_head_km2": "facc_total_head", "facc_total_tail_km2": "facc_total"}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def bit_equal(a, b) -> bool:
    a, b = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    return np.array_equal(a.view(np.uint64), b.view(np.uint64))


def verify_sources() -> dict:
    seal = json.loads((VARIANT / "SHA256SUMS.json").read_text())
    for name, digest in seal.items():
        if sha(VARIANT / name) != digest:
            raise ValueError(f"Frozen candidate file changed: {name}")
    for name, digest in json.loads((VARIANT / "baseline_source_sha256.json").read_text()).items():
        if sha(Path(name)) != digest:
            raise ValueError(f"Frozen baseline source changed: {name}")
    summary = json.loads((RUN / "evaluation/SUMMARY.json").read_text())
    if summary["basins"] != 60 or summary["reaches"] != 248673:
        raise ValueError("Unexpected global population")
    if not summary["hydrobasins"]["candidate_parent_predictions_bit_equal"]:
        raise ValueError("Full-area reference predictions changed")
    nodes = json.loads((RUN / "nodes/PARENT_EQUALITY.json").read_text())
    if nodes["nodes"] != 11112454 or not nodes["all_facc_total_values_bit_identical"]:
        raise ValueError("Whole node area contract failed")
    for row in nodes["regions"]:
        if sha(PARENT / "nodes" / f"{row['region']}_nodes.parquet") != row["parent_nodes_sha256"]:
            raise ValueError(f"Parent node authority changed: {row['region']}")
    return {"candidate_sealed_files": len(seal), "candidate_seal_sha256": sha(VARIANT / "SHA256SUMS.json"), "summary": summary}


def verify_product(folder: Path, manifest: dict) -> None:
    reaches, nodes = 0, 0
    for info in manifest["files"]:
        path = folder / info["name"]
        if sha(path) != info["sha256"]:
            raise ValueError(f"Adopted product hash mismatch: {path}")
        rows = pq.ParquetFile(path).metadata.num_rows
        if rows != info["rows"]:
            raise ValueError(f"Adopted product count mismatch: {path}")
        if info["entity"] == "reaches":
            reaches += rows
        else:
            nodes += rows
    if (reaches, nodes) != (248673, 11112454):
        raise ValueError(f"Unexpected adopted population: {(reaches, nodes)}")


def adopt() -> None:
    checked = verify_sources()
    if TARGET.exists() or AUTHORITY.exists():
        raise FileExistsError("Working FACC authority already exists; use --verify")
    RECORD.mkdir(parents=True, exist_ok=True)
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    stage = TARGET.with_name(TARGET.name + ".staging")
    stage.mkdir(exist_ok=False)
    files, equality = [], []
    try:
        for region in REGIONS:
            raw = pd.read_parquet(RUN / f"{region}_reaches.parquet").sort_values("reach_id").reset_index(drop=True)
            parent = pd.read_parquet(PARENT / f"{region}_reaches.parquet").sort_values("reach_id").reset_index(drop=True)
            if not np.array_equal(raw.reach_id, parent.reach_id):
                raise ValueError(f"Parent reach identity mismatch: {region}")
            for col in ["facc_total_head_km2", "facc_total_tail_km2"]:
                if not bit_equal(raw[col], parent[col]):
                    raise ValueError(f"Full reach area changed: {region}/{col}")
            selected = raw[["reach_id", "region", *RENAMES, "facc_source", "cls"]].rename(columns=RENAMES)
            name = f"sword_{region}_facc_reaches.parquet"
            path = stage / name
            selected.to_parquet(path, index=False, compression="zstd")
            reread = pd.read_parquet(path)
            for old, new in RENAMES.items():
                if not bit_equal(raw[old], reread[new]):
                    raise ValueError(f"Serialization changed values: {region}/{new}")
            files.append({"name": name, "entity": "reaches", "region": region, "rows": len(raw), "sha256": sha(path), "input": relative(RUN / f"{region}_reaches.parquet")})
            source = RUN / "nodes" / f"{region}_nodes.parquet"
            name = f"sword_{region}_facc_nodes.parquet"
            shutil.copyfile(source, stage / name)
            if sha(source) != sha(stage / name):
                raise ValueError(f"Node copy differs: {region}")
            files.append({"name": name, "entity": "nodes", "region": region, "rows": pq.ParquetFile(source).metadata.num_rows, "sha256": sha(source), "input": relative(source)})
            equality.append({"region": region, "whole_reach_endpoints_parent_bit_equal": True, "node_values_identical_to_verified_frozen_run": True})
            print(region, len(raw), files[-1]["rows"], flush=True)
        metrics = checked["summary"]["fork_diagnostics"]
        manifest = {
            "schema_version": 1, "id": "facc_bifurcation_20261026", "status": "adopted_working_product",
            "adopted_on": "2026-10-26", "authority": "Explicit user adoption of validated conservative bifurcation fix",
            "generator": relative(Path(__file__).resolve()), "source_run": relative(RUN),
            "source_model": relative(RUN / "FROZEN_MODEL.json"), "source_seal_sha256": checked["candidate_seal_sha256"],
            "whole_area_authority": relative(PARENT), "whole_area_policy": "facc_total is unsplit and bit-identical to the frozen 21 October parent at all reach endpoints and nodes",
            "width_policy": "Existing SWORD characteristic widths; no SWOT substitution or width changes",
            "topology_policy": "Existing frozen topology, no direction edits; AS edge 43308000201 -> 43309000191 remains flagged for later review",
            "change": "Every allocated side share debits the main edge; source-specific conservative transport and parent rules are retained",
            "semantics": {"facc": "channel-partition area, km2", "facc_total": "unsplit whole-river area, km2", "reach_value": "tail", "facc_head": "upstream head endpoint", "node_values": "unchanged frozen interpolation in original node dist_out"},
            "population": {"basins": 60, "reaches": 248673, "nodes": 11112454},
            "verification": equality,
            "forks": {"count": metrics["forks"], "parent_overbudget_gt1km2": metrics["parent_overbudget_forks_gt1km2"], "adopted_overbudget_gt1km2": metrics["candidate_overbudget_forks_gt1km2"], "max_adopted_closure_error_km2": metrics["max_candidate_fork_closure_error_km2"]},
            "hydrobasins": {"metric": "facc_total", "n": 6262, "within20": 5312, "median_absolute_relative_error_pct": .9609995061176282, "change_from_parent": "none"},
            "followup_registry": "output/facc_detection/adoptions/bifurcation_20261026/FOLLOWUPS.json",
            "deployment_scope": "Local working product and analysis authority; published exports and databases untouched",
            "files": files,
        }
        verify_product(stage, manifest)
        (stage / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
        stage.rename(TARGET)
        authority = {
            "schema_version": 1, "id": manifest["id"], "status": manifest["status"], "adopted_on": manifest["adopted_on"],
            "manifest": relative(TARGET / "MANIFEST.json"), "manifest_sha256": sha(TARGET / "MANIFEST.json"),
            "source_run": relative(RUN), "whole_area_authority": relative(PARENT),
            "facc_total_changed": False, "SWORD_widths_changed": False, "topology_changed": False,
            "direction_followup": [43308000201, 43309000191],
            "deployment_scope": manifest["deployment_scope"],
        }
        temporary = AUTHORITY.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(authority, indent=2) + "\n")
        temporary.replace(AUTHORITY)
        report = {"adopted": True, "authority": relative(AUTHORITY), "manifest": authority["manifest"], "files_verified": len(files), "candidate_files_verified": checked["candidate_sealed_files"], "all_reach_and_node_totals_unchanged": True, "widths_and_topology_unchanged": True, "database_access": False, "published_exports_replaced": False}
        (RECORD / "ADOPTION_VERIFICATION.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    except BaseException:
        if stage.exists():
            shutil.rmtree(stage)
        raise


def verify() -> None:
    authority = json.loads(AUTHORITY.read_text())
    manifest_path = ROOT / authority["manifest"]
    if sha(manifest_path) != authority["manifest_sha256"]:
        raise ValueError("Authority manifest changed")
    manifest = json.loads(manifest_path.read_text())
    verify_product(manifest_path.parent, manifest)
    print(json.dumps({"adopted_authority_verified": True, "files": len(manifest["files"]), "population": manifest["population"], "facc_total_changed": authority["facc_total_changed"]}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="Verify the selected working product without changing it")
    args = parser.parse_args()
    verify() if args.verify else adopt()
