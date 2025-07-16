#!/bin/bash
# Test SUNAT Scraper with a small batch

echo "🧪 Testing SUNAT Scraper..."

# Change to script directory
cd /root/sunatscraper

# Activate virtual environment
source venv/bin/activate

# Test with just 5 companies
echo "🔍 Testing with 5 companies..."
python sunat_scraper.py 5

echo "✅ Test completed. Check sunat_scraper.log for results."

# Show some results
echo ""
echo "📊 Recent database updates:"
python3 -c "
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', '31.97.103.28'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'sunat_data'),
        user=os.getenv('DB_USER', 'empresaintel'),
        password=os.getenv('DB_PASSWORD', 'Madalbal1')
    )
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ruc, razon_social, estado 
        FROM sunat_empresas 
        WHERE razon_social IS NOT NULL 
        ORDER BY id DESC 
        LIMIT 10
    ''')
    
    results = cursor.fetchall()
    
    if results:
        print('Recent companies with names:')
        for ruc, name, estado in results:
            print(f'  - {ruc}: {name} ({estado})')
    else:
        print('No companies with names found yet.')
        
    # Show total count
    cursor.execute('SELECT COUNT(*) FROM sunat_empresas WHERE razon_social IS NOT NULL')
    count = cursor.fetchone()[0]
    print(f'\\nTotal companies with names: {count:,}')
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f'Database error: {e}')
"