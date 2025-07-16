#!/bin/bash
# Production script to process all 25M records using Peru Consult API

echo "🚀 Starting PRODUCTION Peru Consult API scraper..."
echo "📊 Target: ~25 million RUC records"
echo ""

# Configuration
BATCH_SIZE=1000
MAX_WORKERS=50
DELAY_MIN=0.05
LOG_FILE="production_scraper.log"

# Update .env for production settings
echo "⚙️ Configuring production settings..."
cd /root/sunatscraper

# Backup current .env
cp .env .env.backup

# Update production settings
sed -i "s/BATCH_SIZE=.*/BATCH_SIZE=$BATCH_SIZE/" .env
sed -i "s/MAX_WORKERS=.*/MAX_WORKERS=$MAX_WORKERS/" .env
sed -i "s/DELAY_MIN=.*/DELAY_MIN=$DELAY_MIN/" .env

# Add MAX_WORKERS if not exists
if ! grep -q "MAX_WORKERS" .env; then
    echo "MAX_WORKERS=$MAX_WORKERS" >> .env
fi

echo "✅ Production settings:"
echo "   - Batch size: $BATCH_SIZE"
echo "   - Max workers: $MAX_WORKERS" 
echo "   - Delay: $DELAY_MIN seconds"
echo ""

# Check Peru Consult API status
echo "🔍 Checking Peru Consult API status..."
if ! curl -s "http://localhost:8080/api/v1/ruc/20131312955?token=$(grep PERU_CONSULT_API_TOKEN .env | cut -d'=' -f2)" > /dev/null; then
    echo "❌ Peru Consult API is not responding!"
    echo "🔧 Restarting API container..."
    docker restart peru-consult-api
    sleep 10
fi

# Test API again
if curl -s "http://localhost:8080/api/v1/ruc/20131312955?token=$(grep PERU_CONSULT_API_TOKEN .env | cut -d'=' -f2)" > /dev/null; then
    echo "✅ Peru Consult API is running"
else
    echo "❌ Failed to start Peru Consult API. Exiting."
    exit 1
fi

echo ""
echo "🎯 Starting production scraping..."
echo "📋 Progress will be logged to: $LOG_FILE"
echo "⏰ Estimated time: 8-12 hours for 25M records"
echo ""

# Activate virtual environment and run scraper
source venv/bin/activate

# Run in background with nohup to handle disconnections
nohup python peru_consult_scraper.py > $LOG_FILE 2>&1 &

SCRAPER_PID=$!
echo "🚀 Scraper started with PID: $SCRAPER_PID"
echo "📝 Log file: $LOG_FILE"
echo ""
echo "📋 To monitor progress:"
echo "   tail -f $LOG_FILE"
echo ""
echo "📋 To check status:"
echo "   ps aux | grep $SCRAPER_PID"
echo ""
echo "📋 To stop scraper:"
echo "   kill $SCRAPER_PID"
echo ""

# Show initial progress
echo "🔍 Initial log output:"
sleep 5
tail -20 $LOG_FILE

echo ""
echo "✅ Production scraper is running in background!"
echo "📊 Check progress with: tail -f $LOG_FILE"