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
import hashlib
import json
import os
import sys
import time
import zipfile
from pathlib import Path

import requests

API = "https://zenodo.org/api"
LATEST_RECORD_ID = 15299138  # v17b — newversion must target the latest version's id
REPO = Path(__file__).resolve().parents[2]
STAGING = REPO / "data/exports/v17c_beta/zenodo_v17c"
CREATORS_JSON = REPO / "scripts/export/zenodo_creators_v17c.json"
DESCRIPTION_HTML = REPO / "scripts/export/zenodo_description_v17c.html"

TITLE = "SWOT River Database (SWORD)"
VERSION_LABEL = "v17c"
KEYWORDS = ["SWORD", "SWOT", "Rivers", "Hydrology", "Hydrography", "River Networks", "Global"]

UPLOAD_FILES = [
    "SWORD_v17c_netcdf.zip",
    "SWORD_v17c_gpkg.zip",
    "SWORD_v17c_shp.zip",
    "SWORD_v17c_parquet.zip",
    "SWORD_v17c_duckdb.zip",
    "SWORD_v17c_global.zip",
    "SHA256SUMS.txt",
    "SHA256SUMS_files.txt",
]


def die(msg: str) -> None:
    sys.exit(f"ERROR: {msg}")


def load_metadata() -> tuple[dict, list[str]]:
    """Return (metadata, pending_affiliation_names). Caller decides whether
    pending placeholders are fatal (they are for --execute, a warning for dry runs)."""
    creators = json.loads(CREATORS_JSON.read_text())
    pending = [c["name"] for c in creators if "PENDING" in c.get("affiliation", "")]
    for c in creators:
        if not c.get("orcid"):
            die(f"creator missing ORCID: {c['name']}")
    description = DESCRIPTION_HTML.read_text().strip()
    if not description:
        die("description HTML is empty")
    metadata = {
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
    return metadata, pending


def check_files() -> list[Path]:
    paths = []
    for name in UPLOAD_FILES:
        p = STAGING / name
        if not p.is_file():
            die(f"missing upload file: {p}")
        paths.append(p)
    return paths


def stale_bundle_docs() -> list[str]:
    """Return doc names whose copy inside the bundle ZIPs differs from the
    current repo docs (guards against shipping stale release notes / var ref)."""
    docs = REPO / "docs"
    repo_docs = {
        "v17c_release_notes.md": docs / "v17c_release_notes.md",
        "v17c_release_notes.pdf": docs / "v17c_release_notes.pdf",
        "v17c_variable_reference.md": docs / "v17c_variable_reference.md",
        "v17c_variable_reference.pdf": docs / "v17c_variable_reference.pdf",
    }
    ref_zip = STAGING / "SWORD_v17c_netcdf.zip"
    if not ref_zip.is_file():
        die(f"missing bundle zip for freshness check: {ref_zip}")
    stale = []
    with zipfile.ZipFile(ref_zip) as zf:
        names = set(zf.namelist())
        for name, src in repo_docs.items():
            if name not in names:
                stale.append(f"{name} (absent from zip)")
            elif hashlib.sha256(zf.read(name)).hexdigest() != hashlib.sha256(src.read_bytes()).hexdigest():
                stale.append(name)
    return stale


def get_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


def put_file_with_retry(s: requests.Session, url: str, path: Path, attempts: int = 6) -> None:
    """Stream-upload a file, retrying transient network/SSL errors. The bucket
    PUT is idempotent (overwrites), so re-sending a failed file is safe."""
    for i in range(1, attempts + 1):
        try:
            with open(path, "rb") as fh:
                r = s.put(url, data=fh, timeout=None)
            r.raise_for_status()
            return
        except requests.exceptions.RequestException as e:
            if i == attempts:
                raise
            wait = min(60, 2 ** i)
            print(f"    transient upload error ({type(e).__name__}); "
                  f"retry {i}/{attempts - 1} in {wait}s...", flush=True)
            time.sleep(wait)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true",
                    help="actually create the draft and upload (default: dry run)")
    ap.add_argument("--draft-id", type=int, default=None,
                    help="resume an existing unpublished draft by id instead of "
                         "creating a new version (find it at zenodo.org/me/uploads)")
    args = ap.parse_args()

    token = os.environ.get("ZENODO_TOKEN")
    if not token:
        die("ZENODO_TOKEN not set in environment")

    s = get_session(token)

    # Read-only validation first: confirm token works and can see the record.
    # (Done before metadata checks so a dry run validates the token even while
    # some author affiliations are still placeholders.)
    r = s.get(f"{API}/deposit/depositions/{LATEST_RECORD_ID}")
    if r.status_code == 401:
        die("token rejected (401). Check ZENODO_TOKEN and its scopes.")
    if r.status_code == 403:
        die("token valid but lacks access to this record (403). "
            "Confirm the record is shared with your account at 'Can edit' or higher.")
    r.raise_for_status()
    print(f"Token OK; record accessible: {r.json()['metadata'].get('title', '?')}")

    metadata, pending = load_metadata()
    files = check_files()
    total_gb = sum(p.stat().st_size for p in files) / 1e9
    creators = metadata["metadata"]["creators"]

    print(f"\nTarget record (latest version): {LATEST_RECORD_ID}")
    print(f"Version label: {VERSION_LABEL}")
    print(f"Creators ({len(creators)}):")
    for i, c in enumerate(creators, 1):
        print(f"  {i:2}. {c['name']} — {c['affiliation']} ({c['orcid']})")
    print(f"Files to upload ({len(files)}, {total_gb:.1f} GB):")
    for p in files:
        print(f"  {p.name} ({p.stat().st_size / 1e9:.2f} GB)")
    if pending:
        print(f"\n⚠️  {len(pending)} author(s) still have placeholder affiliations: {pending}")

    stale = stale_bundle_docs()
    if stale:
        print(f"\n⚠️  bundle docs stale vs docs/: {stale}"
              "\n    Run: uv run python scripts/export/refresh_bundle_docs.py")

    if not args.execute:
        print("\nDRY RUN — no draft created, nothing uploaded. Re-run with --execute to proceed.")
        return

    if pending:
        die(f"cannot upload while affiliations are placeholders: {pending}. Fill them in first.")
    if stale:
        die(f"cannot upload stale bundle docs: {stale}. "
            "Run scripts/export/refresh_bundle_docs.py first.")

    # 1. Get the new-version draft. With --draft-id, resume that draft (robust
    #    path for a re-run: Zenodo returns 400 from newversion when an
    #    unpublished draft already exists, and its legacy latest_draft link is
    #    unreliable). Otherwise create a fresh new-version draft.
    if args.draft_id:
        draft = s.get(f"{API}/deposit/depositions/{args.draft_id}")
        draft.raise_for_status()
        draft = draft.json()
        if draft.get("state") != "unsubmitted":
            die(f"draft {args.draft_id} is not an unpublished draft "
                f"(state={draft.get('state')}).")
        print(f"\nResuming existing draft: id={draft['id']}")
    else:
        r = s.post(f"{API}/deposit/depositions/{LATEST_RECORD_ID}/actions/newversion")
        if r.status_code == 400:
            die("newversion returned 400 — an unpublished draft likely already "
                "exists. Re-run with --draft-id <id> to resume it "
                "(find the id at https://zenodo.org/me/uploads).")
        r.raise_for_status()
        body = r.json()
        draft_url = body.get("links", {}).get("latest_draft")
        draft = s.get(draft_url).json() if draft_url else body
        print(f"\nDraft created: id={draft['id']}")
    draft_id = draft["id"]
    if draft_id == LATEST_RECORD_ID:
        die("refusing to continue: draft id equals the published record id "
            "(would edit v17b). Aborting before any change.")
    bucket = draft["links"]["bucket"]

    # 2. Set draft metadata.
    r = s.put(f"{API}/deposit/depositions/{draft_id}", data=json.dumps(metadata),
              headers={"Content-Type": "application/json"})
    r.raise_for_status()
    print("Draft metadata set (title, version, description, creators, license).")

    # 3. Reconcile files. A new-version draft inherits the previous version's
    #    files (v17b's zips). Remove any file not in the v17c set, then upload
    #    the v17c files (skipping any already present at the same size, so a
    #    re-run resumes). The legacy deposit API uses filename/filesize; the
    #    newer representation uses key/size — handle both.
    def _fname(f: dict):
        return f.get("key") or f.get("filename")

    def _fsize(f: dict):
        return f.get("size") if f.get("size") is not None else f.get("filesize")

    wanted = set(UPLOAD_FILES)
    existing = {}
    for f in draft.get("files", []):
        nm = _fname(f)
        if nm is None:
            continue
        if nm not in wanted:
            link = f.get("links", {}).get("self")
            resp = (s.delete(link) if link
                    else s.delete(f"{API}/deposit/depositions/{draft_id}/files/{f['id']}"))
            resp.raise_for_status()
            print(f"  removed inherited file: {nm}")
        else:
            existing[nm] = _fsize(f)

    for p in files:
        size = p.stat().st_size
        if existing.get(p.name) == size:
            print(f"  skip (already uploaded): {p.name}")
            continue
        print(f"  uploading {p.name} ({size / 1e9:.2f} GB)...", flush=True)
        put_file_with_retry(s, f"{bucket}/{p.name}", p)
    print("All files uploaded.")

    print("\nDONE — draft is UNPUBLISHED. Review and publish here:")
    print(f"  https://zenodo.org/uploads/{draft_id}")
    print("This script does not publish. Publishing is your manual step.")


if __name__ == "__main__":
    main()
