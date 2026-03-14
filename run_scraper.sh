#!/bin/bash

###############################################################################
# Globe Pest Solutions Scraper - Runner Script for Ubuntu/EC2
# 
# This script runs the scraper with proper environment setup and logging
# Designed to be triggered manually via SSH
###############################################################################

# Configuration
PROJECT_DIR="$HOME/globe-scraper"
LOG_DIR="$PROJECT_DIR/logs"
DATE_SUFFIX=$(date +%Y%m%d_%H%M%S)

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Change to project directory
cd "$PROJECT_DIR" || {
    echo "Error: Could not change to project directory: $PROJECT_DIR"
    exit 1
}

# Activate virtual environment (python-dotenv will load .env automatically)
if [ -d venv ]; then
    source venv/bin/activate
else
    echo "Error: Virtual environment not found in $PROJECT_DIR/venv"
    exit 1
fi

# Log start
echo "========================================" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"
echo "Scraper started at: $(date)" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"
echo "========================================" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"

# Scraper writes to same table N8n reads
export SUPABASE_TABLE=globe_sku

# Run scraper with logging
python main.py >> "$LOG_DIR/cron_$(date +%Y%m%d).log" 2>&1

# Capture exit code
EXIT_CODE=$?

# Log completion
echo "========================================" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"
echo "Scraper finished at: $(date)" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"
echo "Exit code: $EXIT_CODE" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"
echo "========================================" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"
echo "" >> "$LOG_DIR/cron_$(date +%Y%m%d).log"

# Deactivate virtual environment
deactivate

# Exit with the same code as the Python script
exit $EXIT_CODE
