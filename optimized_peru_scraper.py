#!/usr/bin/env python3
"""
Optimized Peru Consult scraper that:
1. Targets only first 13M unique RUCs (ordered by ID)
2. Skips already scraped RUCs
3. Creates efficient lookup for future use
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
        logging.FileHandler('optimized_peru_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class OptimizedPeruScraper:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        # Peru Consult API configuration
        self.api_url = os.getenv('PERU_CONSULT_API_URL', 'http://localhost:8080')
        self.api_token = os.getenv('PERU_CONSULT_API_TOKEN')
        
        if not self.api_token:
            logger.error("❌ PERU_CONSULT_API_TOKEN not found in .env file")
            raise ValueError("Peru Consult API token is required")
        
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '30'))
        self.delay_between_requests = float(os.getenv('DELAY_MIN', '0.05'))
        
        self.db_connection = None
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Peru Consult Optimized Scraper v1.0'
        })
        
        # Thread lock for database operations
        self.db_lock = threading.Lock()
        
        # Cache for already processed RUCs to avoid duplicates in same session
        self.processed_rucs = set()
        
        logger.info(f"✅ Optimized Peru Consult API configured: {self.api_url}")
        
    def setup_database(self):
        """Setup database connection"""
        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            logger.info("✅ Connected to database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def get_unique_rucs_to_scrape(self, limit=None):
        """Get unique RUCs from first 13M records that need scraping"""
        try:
            cursor = self.db_connection.cursor()
            
            logger.info("🔍 Getting unique RUCs from first 13M records...")
            
            # Get unique RUCs from first 13M records, prioritizing active companies
            # and those that don't have razon_social yet
            query = """
                SELECT DISTINCT ON (ruc) ruc, MIN(id) as first_id
                FROM sunat_empresas 
                WHERE id <= (
                    SELECT id FROM sunat_empresas ORDER BY id LIMIT 1 OFFSET 12999999
                )
                AND (razon_social IS NULL OR razon_social = '')
                GROUP BY ruc
                ORDER BY ruc, first_id
            """
            
            if limit:
                query += f" LIMIT {limit}"
                
            cursor.execute(query)
            results = cursor.fetchall()
            rucs = [row[0] for row in results]
            cursor.close()
            
            logger.info(f"📋 Found {len(rucs)} unique RUCs to scrape from first 13M records")
            return rucs
            
        except Exception as e:
            logger.error(f"❌ Error getting unique RUCs: {e}")
            return []
    
    def get_analysis_stats(self):
        """Get database analysis statistics"""
        try:
            cursor = self.db_connection.cursor()
            
            # Total records
            cursor.execute('SELECT COUNT(*) FROM sunat_empresas')
            total = cursor.fetchone()[0]
            
            # First 13M records
            cursor.execute('''
                SELECT COUNT(*) FROM sunat_empresas 
                WHERE id <= (SELECT id FROM sunat_empresas ORDER BY id LIMIT 1 OFFSET 12999999)
            ''')
            first_13m = cursor.fetchone()[0]
            
            # Unique RUCs in first 13M
            cursor.execute('''
                SELECT COUNT(DISTINCT ruc) FROM sunat_empresas 
                WHERE id <= (SELECT id FROM sunat_empresas ORDER BY id LIMIT 1 OFFSET 12999999)
            ''')
            unique_rucs_13m = cursor.fetchone()[0]
            
            # Already scraped in first 13M
            cursor.execute('''
                SELECT COUNT(DISTINCT ruc) FROM sunat_empresas 
                WHERE id <= (SELECT id FROM sunat_empresas ORDER BY id LIMIT 1 OFFSET 12999999)
                AND razon_social IS NOT NULL AND razon_social != ''
            ''')
            scraped_13m = cursor.fetchone()[0]
            
            # Pending to scrape in first 13M
            pending_13m = unique_rucs_13m - scraped_13m
            
            cursor.close()
            
            return {
                'total_records': total,
                'first_13m_records': first_13m,
                'unique_rucs_13m': unique_rucs_13m,
                'scraped_13m': scraped_13m,
                'pending_13m': pending_13m
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting analysis stats: {e}")
            return None
    
    def lookup_ruc_peru_consult(self, ruc):
        """Lookup RUC using Peru Consult API"""
        try:
            # Skip if already processed in this session
            if ruc in self.processed_rucs:
                return None
                
            url = f"{self.api_url}/api/v1/ruc/{ruc}"
            params = {'token': self.api_token}
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                company_name = data.get('razonSocial')
                
                if company_name and len(company_name.strip()) > 3:
                    logger.info(f"✅ Found: {ruc} -> {company_name}")
                    self.processed_rucs.add(ruc)
                    return company_name.strip()
                else:
                    logger.warning(f"⚠️ No valid company name for RUC {ruc}")
                    self.processed_rucs.add(ruc)
                    return None
            
            elif response.status_code == 404:
                logger.info(f"ℹ️ RUC {ruc} not found")
                self.processed_rucs.add(ruc)
                return None
            elif response.status_code == 422:
                logger.warning(f"⚠️ Invalid RUC format: {ruc}")
                self.processed_rucs.add(ruc)
                return None
            else:
                logger.warning(f"⚠️ HTTP {response.status_code} for RUC {ruc}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ Timeout for RUC {ruc}")
            return None
        except Exception as e:
            logger.error(f"❌ Error for RUC {ruc}: {e}")
            return None
    
    def update_all_ruc_instances(self, ruc, company_name):
        """Update ALL instances of this RUC in the database (thread-safe)"""
        try:
            with self.db_lock:
                conn = psycopg2.connect(**self.db_config)
                cursor = conn.cursor()
                
                # Update ALL records with this RUC
                query = "UPDATE sunat_empresas SET razon_social = %s WHERE ruc = %s AND (razon_social IS NULL OR razon_social = '')"
                cursor.execute(query, (company_name, ruc))
                updated_count = cursor.rowcount
                
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"✅ Updated {updated_count} records for RUC {ruc} -> {company_name}")
                return updated_count
                
        except Exception as e:
            logger.error(f"❌ Database update failed for {ruc}: {e}")
            return 0
    
    def process_ruc(self, ruc):
        """Process a single RUC (for threading)"""
        try:
            company_name = self.lookup_ruc_peru_consult(ruc)
            
            if company_name:
                updated_count = self.update_all_ruc_instances(ruc, company_name)
                return {'ruc': ruc, 'success': updated_count > 0, 'company_name': company_name, 'updated_count': updated_count}
            else:
                return {'ruc': ruc, 'success': False, 'company_name': None, 'updated_count': 0}
                
        except Exception as e:
            logger.error(f"❌ Error processing RUC {ruc}: {e}")
            return {'ruc': ruc, 'success': False, 'company_name': None, 'updated_count': 0}
    
    def run_optimized_scraping(self, batch_size=None):
        """Run optimized scraping targeting first 13M unique RUCs"""
        if not batch_size:
            batch_size = self.batch_size
            
        try:
            # Setup database
            if not self.setup_database():
                return False
            
            # Get analysis stats
            stats = self.get_analysis_stats()
            if stats:
                logger.info("📊 Database Analysis:")
                logger.info(f"   - Total records: {stats['total_records']:,}")
                logger.info(f"   - First 13M records: {stats['first_13m_records']:,}")
                logger.info(f"   - Unique RUCs in first 13M: {stats['unique_rucs_13m']:,}")
                logger.info(f"   - Already scraped: {stats['scraped_13m']:,}")
                logger.info(f"   - Pending to scrape: {stats['pending_13m']:,}")
                
                if stats['pending_13m'] == 0:
                    logger.info("✅ All RUCs in first 13M records already scraped!")
                    return True
            
            # Get unique RUCs to process
            rucs = self.get_unique_rucs_to_scrape(batch_size)
            
            if not rucs:
                logger.info("✅ No unique RUCs to process")
                return True
            
            logger.info(f"🚀 Processing {len(rucs)} unique RUCs with {self.max_workers} workers")
            
            # Process RUCs with threading
            successful = 0
            failed = 0
            total_records_updated = 0
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_ruc = {executor.submit(self.process_ruc, ruc): ruc for ruc in rucs}
                
                # Process completed tasks
                for i, future in enumerate(as_completed(future_to_ruc), 1):
                    result = future.result()
                    
                    if result['success']:
                        successful += 1
                        total_records_updated += result['updated_count']
                    else:
                        failed += 1
                    
                    # Progress logging
                    if i % 10 == 0 or i == len(rucs):
                        progress = (i / len(rucs)) * 100
                        logger.info(f"📊 Progress: {i}/{len(rucs)} ({progress:.1f}%) - ✅ {successful} successful, ❌ {failed} failed, 📝 {total_records_updated} records updated")
                    
                    # Small delay to avoid overwhelming the API
                    if self.delay_between_requests > 0:
                        time.sleep(self.delay_between_requests)
            
            logger.info(f"🎉 Batch completed: {successful} unique RUCs successful, {failed} failed, {total_records_updated} total records updated")
            return True
            
        except Exception as e:
            logger.error(f"❌ Optimized scraping failed: {e}")
            return False
            
        finally:
            if self.db_connection:
                self.db_connection.close()

def main():
    """Main function"""
    logger.info("🚀 Starting Optimized Peru Consult API scraper (First 13M unique RUCs)")
    
    try:
        scraper = OptimizedPeruScraper()
        
        # Check command line arguments
        batch_size = None
        if len(sys.argv) > 1:
            try:
                batch_size = int(sys.argv[1])
            except ValueError:
                logger.error("❌ Invalid batch size argument")
                sys.exit(1)
        
        success = scraper.run_optimized_scraping(batch_size)
        
        if success:
            logger.info("✅ Optimized scraping completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Optimized scraping failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()