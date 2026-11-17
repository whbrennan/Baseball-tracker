#!/bin/bash
# Devcontainer / local setup. Uses the same pinned packages as CI.
set -e

echo "Installing Python dependencies..."
pip install -r requirements-dev.txt

echo "Installing Chromium and its system libraries for Playwright..."
# --with-deps installs the apt packages Chromium needs (via sudo when not root).
python -m playwright install --with-deps chromium

echo ""
echo "Setup complete. Run the tests with: python -m pytest"
echo "Run the scraper with: python scraper.py (needs GOOGLE_CREDENTIALS and SPREADSHEET_ID)"
