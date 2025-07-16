#!/usr/bin/env python3
"""
Monitor progress of the Peru Consult API scraper
"""

import psycopg2
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def get_progress_stats():
    """Get current progress statistics"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cursor = conn.cursor()
        
        # Total records
        cursor.execute('SELECT COUNT(*) FROM sunat_empresas')
        total = cursor.fetchone()[0]
        
        # Completed records
        cursor.execute('SELECT COUNT(*) FROM sunat_empresas WHERE razon_social IS NOT NULL')
        completed = cursor.fetchone()[0]
        
        # Remaining records
        remaining = total - completed
        
        # Progress percentage
        progress = (completed / total) * 100 if total > 0 else 0
        
        # Recent activity (last hour)
        cursor.execute('''
            SELECT COUNT(*) FROM sunat_empresas 
            WHERE razon_social IS NOT NULL 
            AND razon_social != ''
        ''')
        completed_today = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            'total': total,
            'completed': completed,
            'remaining': remaining,
            'progress': progress,
            'completed_today': completed_today
        }
        
    except Exception as e:
        print(f"Database error: {e}")
        return None

def estimate_completion_time(completed, remaining, start_time):
    """Estimate completion time based on current rate"""
    if completed == 0:
        return "Unknown"
    
    elapsed = time.time() - start_time
    rate = completed / elapsed  # records per second
    
    if rate > 0:
        remaining_seconds = remaining / rate
        completion_time = datetime.now() + timedelta(seconds=remaining_seconds)
        return completion_time.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return "Unknown"

def main():
    """Monitor progress continuously"""
    print("📊 Peru Consult API Scraper Progress Monitor")
    print("=" * 50)
    
    start_time = time.time()
    last_completed = 0
    
    try:
        while True:
            stats = get_progress_stats()
            
            if stats:
                # Calculate rate
                if last_completed > 0:
                    rate = stats['completed'] - last_completed
                    rate_per_hour = rate * 12  # 12 checks per hour (every 5 min)
                else:
                    rate = 0
                    rate_per_hour = 0
                
                # Estimate completion
                eta = estimate_completion_time(stats['completed'], stats['remaining'], start_time)
                
                # Display progress
                print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"📊 Progress: {stats['completed']:,} / {stats['total']:,} ({stats['progress']:.2f}%)")
                print(f"📋 Remaining: {stats['remaining']:,}")
                print(f"⚡ Rate: +{rate} records (last 5 min), ~{rate_per_hour:,}/hour")
                print(f"🎯 ETA: {eta}")
                
                # Progress bar
                bar_length = 40
                filled = int(bar_length * stats['progress'] / 100)
                bar = "█" * filled + "░" * (bar_length - filled)
                print(f"[{bar}] {stats['progress']:.1f}%")
                
                last_completed = stats['completed']
                
            else:
                print("❌ Unable to get progress stats")
            
            # Wait 5 minutes
            time.sleep(300)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped by user")
    except Exception as e:
        print(f"\n❌ Monitor error: {e}")

if __name__ == "__main__":
    main()