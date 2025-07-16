#!/usr/bin/env python3
"""
SUNAT RUC scraper using Peru Consult API
Implements the actual APIs you suggested from GitHub
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
        logging.FileHandler('peru_consult_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PeruConsultScraper:
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
        
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.delay_between_requests = float(os.getenv('DELAY_MIN', '0.1'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '20'))
        
        self.db_connection = None
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Peru Consult Scraper v1.0'
        })
        
        # Thread lock for database operations
        self.db_lock = threading.Lock()
        
        logger.info(f"✅ Peru Consult API configured: {self.api_url}")
        
    def setup_database(self):
        """Setup database connection"""
        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            logger.info("✅ Connected to database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def get_rucs_to_scrape(self, limit=None):
        """Get RUCs that need company names"""
        try:
            cursor = self.db_connection.cursor()
            
            query = """
                SELECT ruc FROM sunat_empresas 
                WHERE razon_social IS NULL 
                ORDER BY CASE WHEN estado = 'ACTIVO' THEN 1 ELSE 2 END,
                         id
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
    
    def lookup_ruc_peru_consult(self, ruc):
        """Lookup RUC using Peru Consult API"""
        try:
            # Peru Consult API endpoint: /api/v1/ruc/{ruc}?token={token}
            url = f"{self.api_url}/api/v1/ruc/{ruc}"
            params = {'token': self.api_token}
            
            logger.debug(f"🔍 Querying Peru Consult API for RUC: {ruc}")
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract company name from Peru Consult API response
                company_name = data.get('razonSocial')
                
                if company_name and len(company_name.strip()) > 3:
                    logger.info(f"✅ Peru Consult: Found {company_name}")
                    return company_name.strip()
                else:
                    logger.warning(f"⚠️ Peru Consult: No valid company name in response for RUC {ruc}")
                    return None
            
            elif response.status_code == 404:
                logger.info(f"ℹ️ Peru Consult: RUC {ruc} not found")
                return None
            elif response.status_code == 422:
                logger.warning(f"⚠️ Peru Consult: Invalid RUC format {ruc}")
                return None
            else:
                logger.warning(f"⚠️ Peru Consult: HTTP {response.status_code} for RUC {ruc}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ Peru Consult: Request timeout for RUC {ruc}")
            return None
        except Exception as e:
            logger.error(f"❌ Peru Consult: Error for RUC {ruc}: {e}")
            return None
    
    def test_api_connection(self):
        """Test the Peru Consult API connection"""
        try:
            test_ruc = "20131312955"  # SUNAT's own RUC
            logger.info(f"🧪 Testing Peru Consult API with RUC: {test_ruc}")
            
            company_name = self.lookup_ruc_peru_consult(test_ruc)
            
            if company_name:
                logger.info(f"✅ API test successful: {company_name}")
                return True
            else:
                logger.error("❌ API test failed: No company name returned")
                return False
                
        except Exception as e:
            logger.error(f"❌ API test failed: {e}")
            return False
    
    def update_database(self, ruc, company_name):
        """Update database with company name (thread-safe)"""
        try:
            with self.db_lock:
                # Create new connection for thread safety
                conn = psycopg2.connect(**self.db_config)
                cursor = conn.cursor()
                
                query = "UPDATE sunat_empresas SET razon_social = %s WHERE ruc = %s"
                cursor.execute(query, (company_name, ruc))
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"✅ Updated database: {ruc} -> {company_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Database update failed for {ruc}: {e}")
            return False
    
    def process_ruc(self, ruc):
        """Process a single RUC (for threading)"""
        try:
            company_name = self.lookup_ruc_peru_consult(ruc)
            
            if company_name:
                success = self.update_database(ruc, company_name)
                return {'ruc': ruc, 'success': success, 'company_name': company_name}
            else:
                return {'ruc': ruc, 'success': False, 'company_name': None}
                
        except Exception as e:
            logger.error(f"❌ Error processing RUC {ruc}: {e}")
            return {'ruc': ruc, 'success': False, 'company_name': None}
    
    def run_batch_scraping(self, batch_size=None):
        """Run batch scraping using Peru Consult API with threading"""
        if not batch_size:
            batch_size = self.batch_size
            
        try:
            # Test API connection first
            if not self.test_api_connection():
                logger.error("❌ API connection test failed. Aborting.")
                return False
            
            # Setup database
            if not self.setup_database():
                return False
            
            # Get RUCs to process
            rucs = self.get_rucs_to_scrape(batch_size)
            
            if not rucs:
                logger.info("✅ No RUCs to process")
                return True
            
            logger.info(f"🚀 Processing {len(rucs)} RUCs with {self.max_workers} workers")
            
            # Process RUCs with threading
            successful = 0
            failed = 0
            
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
                    if i % 10 == 0 or i == len(rucs):
                        progress = (i / len(rucs)) * 100
                        logger.info(f"📊 Progress: {i}/{len(rucs)} ({progress:.1f}%) - ✅ {successful} successful, ❌ {failed} failed")
                    
                    # Small delay to avoid overwhelming the API
                    if self.delay_between_requests > 0:
                        time.sleep(self.delay_between_requests)
            
            logger.info(f"🎉 Batch completed: {successful} successful, {failed} failed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Batch scraping failed: {e}")
            return False
            
        finally:
            if self.db_connection:
                self.db_connection.close()

def main():
    """Main function"""
    logger.info("🚀 Starting Peru Consult API RUC scraper")
    
    try:
        scraper = PeruConsultScraper()
        
        # Check command line arguments
        batch_size = None
        if len(sys.argv) > 1:
            try:
                batch_size = int(sys.argv[1])
            except ValueError:
                logger.error("❌ Invalid batch size argument")
                sys.exit(1)
        
        success = scraper.run_batch_scraping(batch_size)
        
        if success:
            logger.info("✅ Peru Consult API scraping completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Peru Consult API scraping failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()