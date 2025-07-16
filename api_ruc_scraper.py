#!/usr/bin/env python3
"""
SUNAT RUC API-based scraper - Much faster and more reliable than web scraping
Uses external APIs to get company information by RUC
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
        logging.FileHandler('api_ruc_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ApiRucScraper:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.delay_between_requests = float(os.getenv('DELAY_MIN', '0.5'))
        self.max_workers = int(os.getenv('MAX_WORKERS', '10'))
        
        # API configurations - multiple backup APIs
        self.apis = [
            {
                'name': 'APIs.net.pe',
                'url': 'https://dni.apis.net.pe/v1/ruc',
                'method': 'GET',
                'params': {'numero': '{ruc}'},
                'headers': {'Content-Type': 'application/json'},
                'response_field': 'razonSocial',
                'free': True
            },
            {
                'name': 'RUC.pe',
                'url': 'https://ruc.pe/api/v1/ruc/{ruc}',
                'method': 'GET',
                'params': {},
                'headers': {'Content-Type': 'application/json'},
                'response_field': 'razon_social',
                'free': True
            },
            {
                'name': 'Facturapi',
                'url': 'https://api.facturapi.io/v1/organizations/lookup',
                'method': 'GET',
                'params': {'q': '{ruc}'},
                'headers': {'Content-Type': 'application/json'},
                'response_field': 'legal_name',
                'free': False,
                'requires_key': True
            }
        ]
        
        self.db_connection = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
        })
        
        # Thread lock for database operations
        self.db_lock = threading.Lock()
        
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
    
    def lookup_ruc_api(self, ruc, api_config):
        """Lookup RUC using a specific API"""
        try:
            # Prepare URL and parameters
            url = api_config['url'].format(ruc=ruc)
            params = {}
            for key, value in api_config['params'].items():
                params[key] = value.format(ruc=ruc)
            
            # Make API request
            if api_config['method'] == 'GET':
                response = self.session.get(
                    url, 
                    params=params, 
                    headers=api_config['headers'],
                    timeout=10
                )
            else:
                response = self.session.post(
                    url, 
                    json=params, 
                    headers=api_config['headers'],
                    timeout=10
                )
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract company name based on API structure
                company_name = None
                response_field = api_config['response_field']
                
                if isinstance(data, dict):
                    company_name = data.get(response_field)
                    # Try alternative field names
                    if not company_name:
                        for field in ['razonSocial', 'razon_social', 'name', 'nombre', 'legal_name']:
                            company_name = data.get(field)
                            if company_name:
                                break
                
                if company_name and len(company_name.strip()) > 3:
                    logger.info(f"✅ {api_config['name']}: Found {company_name}")
                    return company_name.strip()
                else:
                    logger.warning(f"⚠️ {api_config['name']}: No valid company name in response")
                    return None
            
            elif response.status_code == 404:
                logger.info(f"ℹ️ {api_config['name']}: RUC {ruc} not found")
                return None
            else:
                logger.warning(f"⚠️ {api_config['name']}: HTTP {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ {api_config['name']}: Request timeout for RUC {ruc}")
            return None
        except Exception as e:
            logger.error(f"❌ {api_config['name']}: Error for RUC {ruc}: {e}")
            return None
    
    def lookup_ruc(self, ruc):
        """Lookup RUC using multiple APIs as fallback"""
        for api_config in self.apis:
            try:
                # Skip paid APIs if no key configured
                if api_config.get('requires_key') and not os.getenv(f"{api_config['name'].upper()}_API_KEY"):
                    continue
                
                logger.info(f"🔍 Trying {api_config['name']} for RUC: {ruc}")
                company_name = self.lookup_ruc_api(ruc, api_config)
                
                if company_name:
                    return company_name
                
                # Small delay between API attempts
                time.sleep(self.delay_between_requests)
                
            except Exception as e:
                logger.error(f"❌ Error with {api_config['name']}: {e}")
                continue
        
        logger.warning(f"⚠️ No company name found for RUC {ruc} after trying all APIs")
        return None
    
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
            company_name = self.lookup_ruc(ruc)
            
            if company_name:
                success = self.update_database(ruc, company_name)
                return {'ruc': ruc, 'success': success, 'company_name': company_name}
            else:
                return {'ruc': ruc, 'success': False, 'company_name': None}
                
        except Exception as e:
            logger.error(f"❌ Error processing RUC {ruc}: {e}")
            return {'ruc': ruc, 'success': False, 'company_name': None}
    
    def run_batch_scraping(self, batch_size=None):
        """Run batch scraping using APIs with threading"""
        if not batch_size:
            batch_size = self.batch_size
            
        try:
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
    logger.info("🚀 Starting API-based RUC scraper")
    
    scraper = ApiRucScraper()
    
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
        logger.info("✅ API scraping completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ API scraping failed")
        sys.exit(1)

if __name__ == "__main__":
    main()