#!/bin/bash
# Run SUNAT Scraper

echo "🚀 Starting SUNAT Company Scraper..."

# Change to script directory
cd /root/sunatscraper

# Activate virtual environment
source venv/bin/activate

# Check if .env exists
if [ ! -f .env ]; then
    echo "❌ .env file not found! Please create it from .env.example"
    exit 1
fi

# Parse command line arguments
BATCH_SIZE=${1:-100}  # Default batch size is 100

echo "📊 Scraping batch size: $BATCH_SIZE companies"
echo "📝 Log file: sunat_scraper.log"
echo ""

# Start scraping
python sunat_scraper.py $BATCH_SIZE

echo "✅ Scraping completed. Check sunat_scraper.log for details."