"""Create a Zenodo new-version draft for SWORD v17c and upload the bundle.

This script NEVER publishes and NEVER modifies the existing v17b record.
It creates a new-version draft off record 15299138, sets the draft metadata,
and uploads the six format ZIPs plus both SHA256SUMS manifests to the draft's
bucket. Publishing is left to a human in the Zenodo web UI.

The API token is read from the ZENODO_TOKEN environment variable and is never
logged. Generate one at https://zenodo.org/account/settings/applications/tokens/new/
with scopes: deposit:write, deposit:actions. Then, in your shell:

    export ZENODO_TOKEN=...        # or put it in a gitignored file and `source` it

Usage:
    uv run --with requests python scripts/export/upload_zenodo_v17c.py            # dry run (read-only)
    uv run --with requests python scripts/export/upload_zenodo_v17c.py --execute  # create draft + upload

After --execute completes it prints the draft URL. Open it, review metadata,
authors, and files, then click Publish yourself.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

API = "https://zenodo.org/api"
LATEST_RECORD_ID = 15299138  # v17b — newversion must target the latest version's id
REPO = Path(__file__).resolve().parents[2]
STAGING = REPO / "data/exports/v17c_beta/zenodo_0.0.12"
CREATORS_JSON = REPO / "scripts/export/zenodo_creators_v17c.json"
DESCRIPTION_HTML = REPO / "scripts/export/zenodo_description_v17c.html"

TITLE = "SWOT River Database (SWORD)"
VERSION_LABEL = "v17c"
KEYWORDS = ["SWORD", "SWOT", "Rivers", "Hydrology", "Hydrography", "River Networks", "Global"]

UPLOAD_FILES = [
    "SWORD_v17c_0.0.12_netcdf.zip",
    "SWORD_v17c_0.0.12_gpkg.zip",
    "SWORD_v17c_0.0.12_shp.zip",
    "SWORD_v17c_0.0.12_parquet.zip",
    "SWORD_v17c_0.0.12_duckdb.zip",
    "SWORD_v17c_0.0.12_global.zip",
    "SHA256SUMS_0.0.12.txt",
    "SHA256SUMS_0.0.12_files.txt",
]


def die(msg: str) -> None:
    sys.exit(f"ERROR: {msg}")


def load_metadata() -> dict:
    creators = json.loads(CREATORS_JSON.read_text())
    pending = [c["name"] for c in creators if "PENDING" in c.get("affiliation", "")]
    if pending:
        die(f"creators JSON still has placeholder affiliations: {pending}. "
            "Fill them in before uploading.")
    for c in creators:
        if not c.get("orcid"):
            die(f"creator missing ORCID: {c['name']}")
    description = DESCRIPTION_HTML.read_text().strip()
    if not description:
        die("description HTML is empty")
    return {
        "metadata": {
            "title": TITLE,
            "upload_type": "dataset",
            "version": VERSION_LABEL,
            "description": description,
            "creators": creators,
            "license": "cc-by-4.0",
            "access_right": "open",
            "keywords": KEYWORDS,
        }
    }


def check_files() -> list[Path]:
    paths = []
    for name in UPLOAD_FILES:
        p = STAGING / name
        if not p.is_file():
            die(f"missing upload file: {p}")
        paths.append(p)
    return paths


def get_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true",
                    help="actually create the draft and upload (default: dry run)")
    args = ap.parse_args()

    token = os.environ.get("ZENODO_TOKEN")
    if not token:
        die("ZENODO_TOKEN not set in environment")

    metadata = load_metadata()
    files = check_files()
    total_gb = sum(p.stat().st_size for p in files) / 1e9
    creators = metadata["metadata"]["creators"]

    print(f"Target record (latest version): {LATEST_RECORD_ID}")
    print(f"Version label: {VERSION_LABEL}")
    print(f"Creators ({len(creators)}):")
    for i, c in enumerate(creators, 1):
        print(f"  {i:2}. {c['name']} — {c['affiliation']} ({c['orcid']})")
    print(f"Files to upload ({len(files)}, {total_gb:.1f} GB):")
    for p in files:
        print(f"  {p.name} ({p.stat().st_size / 1e9:.2f} GB)")

    s = get_session(token)

    # Read-only validation: confirm token works and can see the record.
    r = s.get(f"{API}/deposit/depositions/{LATEST_RECORD_ID}")
    if r.status_code == 401:
        die("token rejected (401). Check ZENODO_TOKEN and its scopes.")
    if r.status_code == 403:
        die("token valid but lacks access to this record (403). "
            "Confirm the record is shared with your account at 'Can edit' or higher.")
    r.raise_for_status()
    print(f"\nToken OK; record accessible: {r.json()['metadata'].get('title', '?')}")

    if not args.execute:
        print("\nDRY RUN — no draft created, nothing uploaded. Re-run with --execute to proceed.")
        return

    # 1. Create (or fetch existing) new-version draft.
    r = s.post(f"{API}/deposit/depositions/{LATEST_RECORD_ID}/actions/newversion")
    r.raise_for_status()
    body = r.json()
    draft_url = body.get("links", {}).get("latest_draft")
    if draft_url:
        draft = s.get(draft_url)
        draft.raise_for_status()
        draft = draft.json()
    else:
        draft = body  # some API versions return the draft directly
    draft_id = draft["id"]
    bucket = draft["links"]["bucket"]
    if draft_id == LATEST_RECORD_ID:
        die("refusing to continue: draft id equals the published record id "
            "(would edit v17b). Aborting before any change.")
    print(f"\nDraft created: id={draft_id}")

    # 2. Set draft metadata.
    r = s.put(f"{API}/deposit/depositions/{draft_id}", data=json.dumps(metadata),
              headers={"Content-Type": "application/json"})
    r.raise_for_status()
    print("Draft metadata set (title, version, description, creators, license).")

    # 3. Upload files via the bucket API (streams; resumable — skips files
    #    already present at the same size).
    existing = {f["key"]: f["size"] for f in draft.get("files", [])}
    for p in files:
        size = p.stat().st_size
        if existing.get(p.name) == size:
            print(f"  skip (already uploaded): {p.name}")
            continue
        print(f"  uploading {p.name} ({size / 1e9:.2f} GB)...", flush=True)
        with open(p, "rb") as fh:
            up = s.put(f"{bucket}/{p.name}", data=fh, timeout=None)
        up.raise_for_status()
    print("All files uploaded.")

    print("\nDONE — draft is UNPUBLISHED. Review and publish here:")
    print(f"  https://zenodo.org/uploads/{draft_id}")
    print("This script does not publish. Publishing is your manual step.")


if __name__ == "__main__":
    main()
