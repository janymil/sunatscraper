#!/usr/bin/env python3
"""
RUC Scraper using apis.net.pe API
Optimized for 8.84M unique RUCs in ruc_lookup database
"""

import os
import requests
import psycopg2
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/sunatscraper/apis_net_pe_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ApisNetPeScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Database configuration  
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'), 
            'password': os.getenv('DB_PASSWORD')
        }
        
        # RUC lookup database config
        self.ruc_db_config = {
            'host': os.getenv('RUC_DB_HOST', os.getenv('DB_HOST')),
            'port': int(os.getenv('RUC_DB_PORT', os.getenv('DB_PORT', '5432'))),
            'database': os.getenv('RUC_DB_NAME', 'ruc_lookup'),
            'user': os.getenv('RUC_DB_USER', os.getenv('DB_USER')),
            'password': os.getenv('RUC_DB_PASSWORD', os.getenv('DB_PASSWORD'))
        }
        
        # Thread-safe counters
        self.lock = threading.Lock()
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        
    def lookup_ruc_apis_net_pe(self, ruc):
        """Lookup RUC using apis.net.pe API"""
        try:
            url = f"https://api.apis.net.pe/v1/ruc?numero={ruc}"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                company_name = data.get('nombre', '').strip()
                
                if company_name:
                    return {
                        'success': True,
                        'ruc': ruc,
                        'razon_social': company_name,
                        'estado': data.get('estado', ''),
                        'condicion': data.get('condicion', ''),
                        'direccion': data.get('direccion', '')
                    }
                else:
                    return {'success': False, 'ruc': ruc, 'error': 'No company name in response'}
                    
            elif response.status_code == 404:
                return {'success': False, 'ruc': ruc, 'error': 'RUC not found'}
            elif response.status_code == 422:
                return {'success': False, 'ruc': ruc, 'error': 'Invalid RUC format'}
            elif response.status_code == 429:
                return {'success': False, 'ruc': ruc, 'error': 'Rate limit exceeded'}
            else:
                return {'success': False, 'ruc': ruc, 'error': f'HTTP {response.status_code}'}
                
        except requests.exceptions.Timeout:
            return {'success': False, 'ruc': ruc, 'error': 'Request timeout'}
        except requests.exceptions.RequestException as e:
            return {'success': False, 'ruc': ruc, 'error': f'Request error: {str(e)}'}
        except Exception as e:
            return {'success': False, 'ruc': ruc, 'error': f'Unexpected error: {str(e)}'}
    
    def update_ruc_lookup_database(self, result):
        """Update the ruc_lookup database with scraped data"""
        if not result['success']:
            return False
            
        try:
            conn = psycopg2.connect(**self.ruc_db_config)
            cursor = conn.cursor()
            
            # Update the razon_social for this RUC
            cursor.execute(
                "UPDATE ruc_lookup SET razon_social = %s WHERE ruc = %s",
                (result['razon_social'], result['ruc'])
            )
            
            conn.commit()
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Database update error for RUC {result['ruc']}: {e}")
            return False
    
    def process_ruc_batch(self, rucs):
        """Process a batch of RUCs"""
        results = []
        
        for ruc in rucs:
            result = self.lookup_ruc_apis_net_pe(ruc)
            results.append(result)
            
            # Update database if successful
            if result['success']:
                self.update_ruc_lookup_database(result)
            
            # Update counters
            with self.lock:
                self.processed_count += 1
                if result['success']:
                    self.success_count += 1
                else:
                    self.error_count += 1
                
                # Progress report every 100 RUCs
                if self.processed_count % 100 == 0:
                    elapsed = datetime.now() - self.start_time
                    rate = self.processed_count / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                    logger.info(f"Progress: {self.processed_count:,} processed | "
                              f"Success: {self.success_count:,} | "
                              f"Errors: {self.error_count:,} | "
                              f"Rate: {rate:.1f} RUCs/sec")
            
            # Small delay to be respectful to the API
            time.sleep(0.1)
        
        return results
    
    def get_pending_rucs(self, limit=None):
        """Get RUCs that haven't been scraped yet"""
        try:
            conn = psycopg2.connect(**self.ruc_db_config)
            cursor = conn.cursor()
            
            query = "SELECT ruc FROM ruc_lookup WHERE razon_social IS NULL"
            if limit:
                query += f" LIMIT {limit}"
                
            cursor.execute(query)
            rucs = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return rucs
            
        except Exception as e:
            logger.error(f"Error getting pending RUCs: {e}")
            return []
    
    def run_production_scraping(self, max_workers=10, batch_size=50):
        """Run the production scraping with threading"""
        logger.info("🚀 Starting production scraping with apis.net.pe")
        logger.info(f"Workers: {max_workers}, Batch size: {batch_size}")
        
        # Get all pending RUCs
        pending_rucs = self.get_pending_rucs()
        total_rucs = len(pending_rucs)
        
        if total_rucs == 0:
            logger.info("✅ No pending RUCs found. All RUCs have been processed!")
            return
        
        logger.info(f"📊 Found {total_rucs:,} pending RUCs to process")
        
        # Create batches
        batches = [pending_rucs[i:i + batch_size] for i in range(0, len(pending_rucs), batch_size)]
        
        logger.info(f"📦 Created {len(batches):,} batches")
        
        # Process batches with threading
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.process_ruc_batch, batch) for batch in batches]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Batch processing error: {e}")
        
        # Final report
        elapsed = datetime.now() - self.start_time
        logger.info(f"🏁 Scraping completed!")
        logger.info(f"📊 Total processed: {self.processed_count:,}")
        logger.info(f"✅ Successful: {self.success_count:,}")
        logger.info(f"❌ Errors: {self.error_count:,}")
        logger.info(f"⏱️ Total time: {elapsed}")
        logger.info(f"📈 Average rate: {self.processed_count / elapsed.total_seconds():.1f} RUCs/sec")

def main():
    scraper = ApisNetPeScraper()
    
    print("🧪 Testing apis.net.pe API with sample RUCs...")
    
    # Test with a few RUCs first
    test_rucs = ['10411592982', '20131312955', '20100070970']
    for ruc in test_rucs:
        result = scraper.lookup_ruc_apis_net_pe(ruc)
        if result['success']:
            print(f"✅ {ruc}: {result['razon_social']}")
        else:
            print(f"❌ {ruc}: {result['error']}")
    
    print("\n" + "="*60)
    response = input("Start production scraping? (y/N): ")
    
    if response.lower() in ['y', 'yes']:
        scraper.run_production_scraping(max_workers=8, batch_size=25)
    else:
        print("Scraping cancelled.")

if __name__ == "__main__":
    main()