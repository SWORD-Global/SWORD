#!/usr/bin/env bash
# Build the v17c release-notes and variable-reference PDFs from markdown.
# Layout matches the historical pandoc/pdfLaTeX builds (letter portrait
# 0.7in margins for release notes; letter landscape 0.45in margins for the
# wide variable-reference tables).
set -euo pipefail

cd "$(dirname "$0")/../.."

pandoc docs/v17c_release_notes.md -o docs/v17c_release_notes.pdf \
  --pdf-engine=pdflatex -V geometry:margin=0.7in -V fontsize=10pt

pandoc docs/v17c_variable_reference.md -o docs/v17c_variable_reference.pdf \
  --pdf-engine=pdflatex -V geometry:landscape -V geometry:margin=0.45in \
  -V fontsize=10pt

echo "Built docs/v17c_release_notes.pdf and docs/v17c_variable_reference.pdf"
