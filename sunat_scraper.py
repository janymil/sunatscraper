#!/usr/bin/env python3
"""
SUNAT Company Data Scraper
Scrapes company information from https://e-consultaruc.sunat.gob.pe
and updates the PostgreSQL database with company names (razon_social)
"""

import os
import sys
import time
import random
import logging
import psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc
from fake_useragent import UserAgent
from dotenv import load_dotenv
import requests
from twocaptcha import TwoCaptcha

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sunat_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SUNATScraper:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        self.delay_min = int(os.getenv('DELAY_MIN', '2'))
        self.delay_max = int(os.getenv('DELAY_MAX', '5'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.headless = os.getenv('HEADLESS', 'true').lower() == 'true'
        
        # CAPTCHA service setup
        self.captcha_api_key = os.getenv('2CAPTCHA_API_KEY')
        self.solver = TwoCaptcha(self.captcha_api_key) if self.captcha_api_key else None
        
        self.driver = None
        self.db_connection = None
        
    def setup_database(self):
        """Setup database connection"""
        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            logger.info("✅ Connected to database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def setup_driver(self):
        """Setup Chrome driver with anti-detection"""
        try:
            # Chrome options with better stealth
            options = uc.ChromeOptions()
            
            if self.headless:
                options.add_argument('--headless=new')
                
            # Basic stealth arguments
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--no-first-run')
            options.add_argument('--disable-default-apps')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-features=VizDisplayCompositor')
            options.add_argument('--window-size=1920,1080')
            
            # User agent - use a realistic Windows Chrome agent
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
            options.add_argument(f'--user-agent={user_agent}')
            
            # Create driver with better stealth
            self.driver = uc.Chrome(options=options, version_main=None, driver_executable_path=None)
            
            # Additional stealth JavaScript
            stealth_js = """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                window.chrome = {runtime: {}};
            """
            self.driver.execute_script(stealth_js)
            
            logger.info("✅ Chrome driver initialized with stealth mode")
            return True
            
        except Exception as e:
            logger.error(f"❌ Driver setup failed: {e}")
            return False
    
    def solve_captcha(self, site_key, page_url):
        """Solve reCAPTCHA using 2captcha service"""
        if not self.solver:
            logger.warning("⚠️ No CAPTCHA solver configured")
            return None
            
        try:
            logger.info("🔐 Solving CAPTCHA...")
            result = self.solver.recaptcha(
                sitekey=site_key,
                url=page_url
            )
            logger.info("✅ CAPTCHA solved")
            return result['code']
        except Exception as e:
            logger.error(f"❌ CAPTCHA solving failed: {e}")
            return None
    
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
    
    def scrape_company_name(self, ruc):
        """Scrape company name for a specific RUC"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                logger.info(f"🔍 Scraping RUC: {ruc} (attempt {attempt + 1})")
                
                # Navigate to SUNAT page with session setup
                logger.info("🌐 Navigating to SUNAT website...")
                self.driver.get("https://e-consultaruc.sunat.gob.pe/")
                time.sleep(3)  # Allow initial redirect and session setup
                
                # Follow redirect to search page
                self.driver.get("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp")
                
                # Wait for page to load
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.ID, "txtRuc"))
                )
                
                # Wait a bit more for page to fully load
                time.sleep(2)
                
                # Select RUC search option (ensure it's clickable)
                ruc_radio = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "rbtnTipo01"))
                )
                self.driver.execute_script("arguments[0].click();", ruc_radio)
                
                # Wait for RUC input to be available
                ruc_input = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "txtRuc"))
                )
                
                # Clear and enter RUC
                ruc_input.clear()
                ruc_input.send_keys(ruc)
                
                # Wait a moment
                time.sleep(1)
                
                # Check for CAPTCHA
                try:
                    captcha_element = self.driver.find_element(By.CSS_SELECTOR, "[data-sitekey]")
                    site_key = captcha_element.get_attribute("data-sitekey")
                    
                    if site_key and self.solver:
                        captcha_response = self.solve_captcha(site_key, self.driver.current_url)
                        if captcha_response:
                            # Inject CAPTCHA response
                            self.driver.execute_script(f"document.getElementById('g-recaptcha-response').innerHTML='{captcha_response}';")
                            
                except Exception:
                    logger.info("No CAPTCHA found or failed to handle")
                
                # Find and click submit button
                try:
                    submit_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "btnAceptar"))
                    )
                    self.driver.execute_script("arguments[0].click();", submit_button)
                except Exception:
                    # Fallback to other submit button selectors
                    submit_button = self.driver.find_element(By.CSS_SELECTOR, "input[type='submit'], button[type='submit']")
                    self.driver.execute_script("arguments[0].click();", submit_button)
                
                # Wait for results page
                WebDriverWait(self.driver, 20).until(
                    lambda driver: "FrameCriterioBusquedaWeb.jsp" not in driver.current_url
                )
                
                # Extract company name
                company_name = self.extract_company_name()
                
                if company_name:
                    logger.info(f"✅ Found: {company_name}")
                    return company_name
                else:
                    logger.warning(f"⚠️ No company name found for RUC {ruc}")
                    
                # Random delay between requests
                time.sleep(random.uniform(self.delay_min, self.delay_max))
                
            except Exception as e:
                logger.error(f"❌ Error scraping RUC {ruc}: {e}")
                if attempt == max_attempts - 1:
                    return None
                time.sleep(random.uniform(2, 5))
        
        return None
    
    def extract_company_name(self):
        """Extract company name from result page"""
        try:
            # Common selectors for company name
            selectors = [
                "td:contains('Razón Social') + td",
                "td:contains('RAZÓN SOCIAL') + td", 
                "td:contains('Nombre o Razón Social') + td",
                "[class*='razon'], [class*='nombre']",
                "table td:nth-child(2)"
            ]
            
            for selector in selectors:
                try:
                    if "contains" in selector:
                        # Use JavaScript to find elements by text content
                        elements = self.driver.execute_script(f"""
                            return Array.from(document.querySelectorAll('td')).filter(el => 
                                el.textContent.includes('Razón Social') || 
                                el.textContent.includes('RAZÓN SOCIAL') ||
                                el.textContent.includes('Nombre o Razón Social')
                            );
                        """)
                        
                        for element in elements:
                            next_sibling = self.driver.execute_script("return arguments[0].nextElementSibling;", element)
                            if next_sibling:
                                name = next_sibling.text.strip()
                                if name and len(name) > 3:
                                    return name
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            name = element.text.strip()
                            if name and len(name) > 3:
                                return name
                                
                except Exception:
                    continue
            
            # Fallback: look for any table data that might be company name
            all_tds = self.driver.find_elements(By.TAG_NAME, "td")
            for td in all_tds:
                text = td.text.strip()
                if text and len(text) > 10 and not text.isdigit() and "RUC" not in text.upper():
                    return text
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error extracting company name: {e}")
            return None
    
    def update_database(self, ruc, company_name):
        """Update database with scraped company name"""
        try:
            cursor = self.db_connection.cursor()
            
            query = "UPDATE sunat_empresas SET razon_social = %s WHERE ruc = %s"
            cursor.execute(query, (company_name, ruc))
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"✅ Updated database: {ruc} -> {company_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database update failed: {e}")
            return False
    
    def run_batch_scraping(self, batch_size=None):
        """Run batch scraping of companies"""
        if not batch_size:
            batch_size = self.batch_size
            
        try:
            # Setup
            if not self.setup_database():
                return False
                
            if not self.setup_driver():
                return False
            
            # Get RUCs to scrape
            rucs = self.get_rucs_to_scrape(batch_size)
            
            if not rucs:
                logger.info("✅ No RUCs to scrape")
                return True
            
            # Process each RUC
            successful = 0
            failed = 0
            
            for i, ruc in enumerate(rucs, 1):
                logger.info(f"📊 Progress: {i}/{len(rucs)} ({(i/len(rucs)*100):.1f}%)")
                
                company_name = self.scrape_company_name(ruc)
                
                if company_name:
                    if self.update_database(ruc, company_name):
                        successful += 1
                    else:
                        failed += 1
                else:
                    failed += 1
                
                # Progress logging
                if i % 10 == 0:
                    logger.info(f"📈 Batch progress: {successful} successful, {failed} failed")
            
            logger.info(f"🎉 Batch completed: {successful} successful, {failed} failed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Batch scraping failed: {e}")
            return False
            
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            self.driver.quit()
        if self.db_connection:
            self.db_connection.close()
        logger.info("🧹 Cleanup completed")

def main():
    """Main function"""
    logger.info("🚀 Starting SUNAT scraper")
    
    scraper = SUNATScraper()
    
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
        logger.info("✅ Scraping completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ Scraping failed")
        sys.exit(1)

if __name__ == "__main__":
    main()