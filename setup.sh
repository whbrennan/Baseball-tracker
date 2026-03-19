#!/bin/bash
set -e

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Installing Playwright system dependencies..."
sudo apt-get install -y libasound2t64

echo "Installing Playwright Chromium browser..."
python -m playwright install chromium
python -m playwright install-deps chromium

echo ""
echo "Setup complete. Run with: python scraper.py"
