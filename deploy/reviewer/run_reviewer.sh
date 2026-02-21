#!/bin/bash
# SWORD QA Reviewer - One-click launch
# Run this script to start the reviewer UI.

set -e

echo "Checking setup..."
python scripts/maintenance/check_reviewer_setup.py

echo ""
echo "Launching SWORD Reviewer..."
streamlit run deploy/reviewer/app.py
