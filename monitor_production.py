#!/usr/bin/env python3
"""
Production monitoring script for RUC lookup scraping
"""

import psycopg2
import time
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def monitor_progress():
    """Monitor scraping progress in real-time"""
    
    ruc_db_config = {
        'host': os.getenv('RUC_DB_HOST', os.getenv('DB_HOST')),
        'port': os.getenv('RUC_DB_PORT', os.getenv('DB_PORT', '5432')),
        'database': os.getenv('RUC_DB_NAME', 'ruc_lookup'),
        'user': os.getenv('RUC_DB_USER', os.getenv('DB_USER')),
        'password': os.getenv('RUC_DB_PASSWORD', os.getenv('DB_PASSWORD'))
    }
    
    start_time = time.time()
    last_scraped = 0
    
    print("📊 RUC Lookup Production Monitor")
    print("=" * 50)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📝 Monitoring ruc_lookup database...")
    print("")
    
    try:
        while True:
            try:
                conn = psycopg2.connect(**ruc_db_config)
                cursor = conn.cursor()
                
                # Get current stats
                cursor.execute('SELECT COUNT(*) FROM ruc_lookup')
                total = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM ruc_lookup WHERE razon_social IS NOT NULL')
                scraped = cursor.fetchone()[0]
                
                pending = total - scraped
                progress = (scraped / total) * 100 if total > 0 else 0
                
                # Calculate rate
                if last_scraped > 0:
                    new_scraped = scraped - last_scraped
                    rate_per_minute = new_scraped  # Since we check every minute
                    rate_per_hour = rate_per_minute * 60
                else:
                    rate_per_minute = 0
                    rate_per_hour = 0
                
                # Estimate completion time
                if rate_per_hour > 0 and pending > 0:
                    hours_remaining = pending / rate_per_hour
                    completion_time = datetime.fromtimestamp(time.time() + hours_remaining * 3600)
                    eta = completion_time.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    eta = "Unknown"
                
                # Display stats
                print(f"\\r⏰ {datetime.now().strftime('%H:%M:%S')} | "
                      f"📊 {scraped:,}/{total:,} ({progress:.2f}%) | "
                      f"📋 Pending: {pending:,} | "
                      f"⚡ Rate: +{new_scraped}/min (~{rate_per_hour:,}/hr) | "
                      f"🎯 ETA: {eta}", end="", flush=True)
                
                last_scraped = scraped
                
                cursor.close()
                conn.close()
                
                # Check if completed
                if pending == 0:
                    print("\\n\\n🎉 SCRAPING COMPLETED!")
                    break
                
            except Exception as e:
                print(f"\\n❌ Monitor error: {e}")
            
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\\n\\n👋 Monitoring stopped by user")

if __name__ == "__main__":
    monitor_progress()