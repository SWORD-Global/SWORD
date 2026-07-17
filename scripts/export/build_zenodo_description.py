"""Assemble the Zenodo v17c description HTML.

Combines the hand-written intro (scripts/export/zenodo_description_intro.html)
with an HTML rendering of the variable reference (docs/v17c_variable_reference.md,
via pandoc) so the full attribute catalog in the Zenodo description can never
drift from the shipped variable reference.

Zenodo sanitizes description HTML to a fixed tag whitelist
(https://developers.zenodo.org/ -> deposit representation). Tables ARE allowed
(table/caption/thead/tbody/tr/th/td) but headings (h1-h6), colgroup, col, hr,
and sup are NOT. This script renders the variable reference, then rewrites it
to the whitelist: headings become <p><strong>...</strong></p>, and disallowed
structural tags are removed. It asserts the assembled HTML uses only allowed
tags so nothing silently vanishes on upload.

Output: scripts/export/zenodo_description_v17c.html (consumed by the uploader).

Usage:
    uv run --with beautifulsoup4 python scripts/export/build_zenodo_description.py
    (pandoc must be installed)
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from bs4 import BeautifulSoup

REPO = Path(__file__).resolve().parents[2]
INTRO = REPO / "scripts/export/zenodo_description_intro.html"
VAR_REF = REPO / "docs/v17c_variable_reference.md"
OUT = REPO / "scripts/export/zenodo_description_v17c.html"

# Zenodo deposit description sanitization whitelist (developers.zenodo.org).
ALLOWED_TAGS = {
    "a", "abbr", "acronym", "b", "blockquote", "br", "code", "caption", "div",
    "em", "i", "li", "ol", "p", "pre", "span", "strike", "strong", "sub",
    "table", "tbody", "thead", "th", "td", "tr", "u", "ul",
}
HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
# Disallowed structural tags to drop entirely (Zenodo strips them anyway;
# removing here keeps the local preview faithful to the uploaded result).
DROP_TAGS = {"colgroup", "col", "hr"}


def main() -> None:
    for p in (INTRO, VAR_REF):
        if not p.is_file():
            sys.exit(f"missing input: {p}")

    res = subprocess.run(
        ["pandoc", str(VAR_REF), "-f", "markdown", "-t", "html", "--no-highlight"],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        sys.exit(f"pandoc failed:\n{res.stderr}")

    soup = BeautifulSoup(res.stdout, "html.parser")

    # Drop the H1 title and the "Quick-lookup ..." intro paragraph (the intro
    # file provides the section 3 lead-in); keep everything from Fill values on.
    if soup.h1:
        soup.h1.decompose()
    for p in soup.find_all("p"):
        if p.get_text(strip=True).startswith("Quick-lookup for all variables"):
            p.decompose()
            break

    # Remove disallowed structural tags outright.
    for tag in soup.find_all(list(DROP_TAGS)):
        tag.decompose()

    # Convert headings to bold paragraphs so section structure survives.
    for tag in soup.find_all(list(HEADING_TAGS)):
        tag.name = "p"
        strong = soup.new_tag("strong")
        strong.string = tag.get_text()
        tag.clear()
        tag.append(strong)

    # Strip id/style/width attrs (Zenodo drops unknown attrs; keep href only).
    for tag in soup.find_all(True):
        allowed_attrs = {"href"} if tag.name == "a" else set()
        for attr in list(tag.attrs):
            if attr not in allowed_attrs:
                del tag[attr]

    frag = str(soup)
    # Normalize build-number mentions so the record reads as v17c full stop.
    frag = frag.replace("0.0.12", "v17c").replace("v17c v17c", "v17c")

    intro = INTRO.read_text().rstrip()
    combined = intro + "\n" + frag
    OUT.write_text(combined)

    # Verify only whitelisted tags remain (fail loudly rather than ship HTML
    # that Zenodo will silently mangle).
    check = BeautifulSoup(combined, "html.parser")
    used = {t.name for t in check.find_all(True)}
    bad = used - ALLOWED_TAGS
    if bad:
        sys.exit(f"assembled description uses non-whitelisted tags: {sorted(bad)}")
    for token in ("0.0.12", "beta", "swordexplorer"):
        if token.lower() in combined.lower():
            sys.exit(f"assembled description still contains '{token}'")

    print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")
    print(f"tags used (all whitelisted): {sorted(used)}")


if __name__ == "__main__":
    main()
