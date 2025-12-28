#!/bin/bash

# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy .env template
cp .env.example .env

# Create temp folder
mkdir -p temp_files

echo "Setup complete! Edit .env with your bot token and run: python main.py"
