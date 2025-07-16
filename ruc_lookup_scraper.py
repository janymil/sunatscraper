#!/usr/bin/env python3
"""
RUC Lookup Scraper - Optimized version that works with separate ruc_lookup database
Scrapes 8.84M unique RUCs using Peru Consult API
"""

import os
import sys
import time
import logging
import psycopg2
import requests
from dotenv import load_dotenv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ruc_lookup_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class RucLookupScraper:
    def __init__(self):
        # RUC lookup database config
        self.ruc_db_config = {
            'host': os.getenv('RUC_DB_HOST', os.getenv('DB_HOST')),
            'port': os.getenv('RUC_DB_PORT', os.getenv('DB_PORT', '5432')),
            'database': os.getenv('RUC_DB_NAME', 'ruc_lookup'),
            'user': os.getenv('RUC_DB_USER', os.getenv('DB_USER')),
            'password': os.getenv('RUC_DB_PASSWORD', os.getenv('DB_PASSWORD'))
        }
        
        # Peru Consult API configuration
        self.api_url = os.getenv('PERU_CONSULT_API_URL', 'http://localhost:8080')
        self.api_token = os.getenv('PERU_CONSULT_API_TOKEN')
        
        if not self.api_token:
            logger.error("❌ PERU_CONSULT_API_TOKEN not found in .env file")
            raise ValueError("Peru Consult API token is required")
        
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '20'))
        self.delay_between_requests = float(os.getenv('DELAY_MIN', '0.1'))
        
        self.db_connection = None
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'RUC Lookup Scraper v1.0'
        })
        
        # Thread lock for database operations
        self.db_lock = threading.Lock()
        
        logger.info(f"✅ RUC Lookup Scraper configured")
        logger.info(f"   - API: {self.api_url}")
        logger.info(f"   - Database: {self.ruc_db_config['database']}")
        logger.info(f"   - Max workers: {self.max_workers}")
        
    def setup_database(self):
        """Setup database connection"""
        try:
            self.db_connection = psycopg2.connect(**self.ruc_db_config)
            logger.info("✅ Connected to RUC lookup database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def get_rucs_to_scrape(self, limit=None):
        """Get RUCs that need scraping from ruc_lookup database"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                SELECT ruc FROM ruc_lookup 
                WHERE razon_social IS NULL 
                ORDER BY id
            """
            
            if limit:
                query += f" LIMIT {limit}"
                
            cursor.execute(query)
            rucs = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"📋 Found {len(rucs)} RUCs to scrape")
            return rucs
            
        except Exception as e:
            logger.error(f"❌ Error getting RUCs: {e}")
            return []
    
    def get_scraping_stats(self):
        """Get current scraping statistics"""
        try:
            cursor = self.db_connection.cursor()
            
            # Total RUCs
            cursor.execute('SELECT COUNT(*) FROM ruc_lookup')
            total = cursor.fetchone()[0]
            
            # Scraped RUCs
            cursor.execute('SELECT COUNT(*) FROM ruc_lookup WHERE razon_social IS NOT NULL')
            scraped = cursor.fetchone()[0]
            
            # Pending RUCs
            pending = total - scraped
            
            # Progress percentage
            progress = (scraped / total) * 100 if total > 0 else 0
            
            cursor.close()
            
            return {
                'total': total,
                'scraped': scraped,
                'pending': pending,
                'progress': progress
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting stats: {e}")
            return None
    
    def lookup_ruc_peru_consult(self, ruc):
        """Lookup RUC using Peru Consult API"""
        try:
            url = f"{self.api_url}/api/v1/ruc/{ruc}"
            params = {'token': self.api_token}
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                company_name = data.get('razonSocial')
                
                if company_name and len(company_name.strip()) > 3:
                    return company_name.strip()
                else:
                    return None
            
            elif response.status_code == 404:
                logger.debug(f"RUC {ruc} not found")
                return "NOT_FOUND"  # Mark as processed but not found
            elif response.status_code == 422:
                logger.warning(f"Invalid RUC format: {ruc}")
                return "INVALID"
            else:
                logger.warning(f"HTTP {response.status_code} for RUC {ruc}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for RUC {ruc}")
            return None
        except Exception as e:
            logger.error(f"Error for RUC {ruc}: {e}")
            return None
    
    def update_ruc_lookup(self, ruc, company_name):
        """Update RUC lookup database (thread-safe)"""
        try:
            with self.db_lock:
                conn = psycopg2.connect(**self.ruc_db_config)
                cursor = conn.cursor()
                
                query = "UPDATE ruc_lookup SET razon_social = %s, scraped_at = CURRENT_TIMESTAMP WHERE ruc = %s"
                cursor.execute(query, (company_name, ruc))
                
                conn.commit()
                cursor.close()
                conn.close()
                
                return True
                
        except Exception as e:
            logger.error(f"Database update failed for {ruc}: {e}")
            return False
    
    def process_ruc(self, ruc):
        """Process a single RUC (for threading)"""
        try:
            company_name = self.lookup_ruc_peru_consult(ruc)
            
            if company_name is not None:
                success = self.update_ruc_lookup(ruc, company_name)
                if success:
                    if company_name not in ["NOT_FOUND", "INVALID"]:
                        logger.info(f"✅ {ruc} -> {company_name}")
                    return {'ruc': ruc, 'success': True, 'company_name': company_name}
                else:
                    return {'ruc': ruc, 'success': False, 'company_name': None}
            else:
                return {'ruc': ruc, 'success': False, 'company_name': None}
                
        except Exception as e:
            logger.error(f"Error processing RUC {ruc}: {e}")
            return {'ruc': ruc, 'success': False, 'company_name': None}
    
    def run_scraping(self, batch_size=None):
        """Run RUC lookup scraping"""
        if not batch_size:
            batch_size = self.batch_size
            
        try:
            # Setup database
            if not self.setup_database():
                return False
            
            # Get current stats
            stats = self.get_scraping_stats()
            if stats:
                logger.info("📊 Current Progress:")
                logger.info(f"   - Total RUCs: {stats['total']:,}")
                logger.info(f"   - Scraped: {stats['scraped']:,}")
                logger.info(f"   - Pending: {stats['pending']:,}")
                logger.info(f"   - Progress: {stats['progress']:.2f}%")
                
                if stats['pending'] == 0:
                    logger.info("✅ All RUCs already scraped!")
                    return True
            
            # Get RUCs to process
            rucs = self.get_rucs_to_scrape(batch_size)
            
            if not rucs:
                logger.info("✅ No RUCs to process")
                return True
            
            logger.info(f"🚀 Processing {len(rucs)} RUCs with {self.max_workers} workers")
            
            # Process RUCs with threading
            successful = 0
            failed = 0
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_ruc = {executor.submit(self.process_ruc, ruc): ruc for ruc in rucs}
                
                # Process completed tasks
                for i, future in enumerate(as_completed(future_to_ruc), 1):
                    result = future.result()
                    
                    if result['success']:
                        successful += 1
                    else:
                        failed += 1
                    
                    # Progress logging
                    if i % 50 == 0 or i == len(rucs):
                        progress = (i / len(rucs)) * 100
                        elapsed = time.time() - start_time
                        rate = i / elapsed if elapsed > 0 else 0
                        
                        logger.info(f"📊 Progress: {i}/{len(rucs)} ({progress:.1f}%) - ✅ {successful} successful, ❌ {failed} failed - Rate: {rate:.1f} RUCs/sec")
                    
                    # Small delay to avoid overwhelming the API
                    if self.delay_between_requests > 0:
                        time.sleep(self.delay_between_requests)
            
            elapsed = time.time() - start_time
            final_rate = len(rucs) / elapsed if elapsed > 0 else 0
            
            logger.info(f"🎉 Batch completed: {successful} successful, {failed} failed")
            logger.info(f"⏱️ Time: {elapsed:.1f}s, Rate: {final_rate:.1f} RUCs/sec")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Scraping failed: {e}")
            return False
            
        finally:
            if self.db_connection:
                self.db_connection.close()

def main():
    """Main function"""
    logger.info("🚀 Starting RUC Lookup Scraper")
    
    try:
        scraper = RucLookupScraper()
        
        # Check command line arguments
        batch_size = None
        if len(sys.argv) > 1:
            try:
                batch_size = int(sys.argv[1])
            except ValueError:
                logger.error("❌ Invalid batch size argument")
                sys.exit(1)
        
        success = scraper.run_scraping(batch_size)
        
        if success:
            logger.info("✅ RUC lookup scraping completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ RUC lookup scraping failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()