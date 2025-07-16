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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
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
                
            # Enhanced stealth arguments
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--no-first-run')
            options.add_argument('--disable-default-apps')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-features=VizDisplayCompositor')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-web-security')
            options.add_argument('--disable-features=ChromeWhatsNewUI')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-renderer-backgrounding')
            options.add_argument('--disable-field-trial-config')
            options.add_argument('--disable-back-forward-cache')
            options.add_argument('--disable-ipc-flooding-protection')
            
            # User agent - use a realistic Windows Chrome agent
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            options.add_argument(f'--user-agent={user_agent}')
            
            # Create driver with better stealth
            self.driver = uc.Chrome(options=options, version_main=None, driver_executable_path=None)
            
            # Set timeouts
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            # Additional stealth JavaScript
            stealth_js = """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['es-PE', 'es', 'en-US', 'en']});
                window.chrome = {runtime: {}};
                
                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """
            self.driver.execute_script(stealth_js)
            
            logger.info("✅ Chrome driver initialized with enhanced stealth mode")
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
                url=page_url,
                version='v3',
                action='submit',
                min_score=0.3
            )
            logger.info("✅ CAPTCHA solved")
            return result['code']
        except Exception as e:
            logger.error(f"❌ CAPTCHA solving failed: {e}")
            return None
    
    def solve_image_captcha(self, captcha_image_url):
        """Solve image captcha using 2captcha service"""
        if not self.solver:
            logger.warning("⚠️ No CAPTCHA solver configured")
            return None
            
        try:
            logger.info("🔐 Solving image CAPTCHA...")
            result = self.solver.normal(captcha_image_url)
            logger.info("✅ Image CAPTCHA solved")
            return result['code']
        except Exception as e:
            logger.error(f"❌ Image CAPTCHA solving failed: {e}")
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
                try:
                    ruc_radio = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "rbtnTipo01"))
                    )
                    self.driver.execute_script("arguments[0].click();", ruc_radio)
                    logger.info("✅ Selected RUC search option")
                except Exception as e:
                    logger.warning(f"⚠️ Could not select RUC radio button: {e}")
                    # Try alternative selector
                    try:
                        ruc_radio = self.driver.find_element(By.NAME, "rbtnTipo")
                        self.driver.execute_script("arguments[0].click();", ruc_radio)
                    except:
                        logger.warning("⚠️ Alternative RUC radio button not found")
                
                # Wait for RUC input to be available
                ruc_input = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "txtRuc"))
                )
                
                # Clear and enter RUC
                ruc_input.clear()
                ruc_input.send_keys(ruc)
                logger.info(f"✅ Entered RUC: {ruc}")
                
                # Wait a moment
                time.sleep(1)
                
                # Handle different types of CAPTCHA
                captcha_solved = False
                
                # Check for reCAPTCHA v3
                try:
                    recaptcha_element = self.driver.find_element(By.CSS_SELECTOR, "[data-sitekey]")
                    site_key = recaptcha_element.get_attribute("data-sitekey")
                    
                    if site_key and self.solver:
                        logger.info("🔐 Found reCAPTCHA v3")
                        captcha_response = self.solve_captcha(site_key, self.driver.current_url)
                        if captcha_response:
                            # Inject CAPTCHA response
                            self.driver.execute_script(f"document.getElementById('g-recaptcha-response').innerHTML='{captcha_response}';")
                            # Also try to set the token in the form
                            self.driver.execute_script(f"if(typeof grecaptcha !== 'undefined') grecaptcha.execute();")
                            captcha_solved = True
                            logger.info("✅ reCAPTCHA v3 solved")
                        
                except Exception:
                    logger.info("No reCAPTCHA v3 found")
                
                # Check for image CAPTCHA
                if not captcha_solved:
                    try:
                        captcha_img = self.driver.find_element(By.ID, "imgCaptcha")
                        if captcha_img:
                            logger.info("🔐 Found image CAPTCHA")
                            captcha_src = captcha_img.get_attribute("src")
                            if captcha_src and self.solver:
                                captcha_response = self.solve_image_captcha(captcha_src)
                                if captcha_response:
                                    captcha_input = self.driver.find_element(By.ID, "txtCodigo")
                                    captcha_input.clear()
                                    captcha_input.send_keys(captcha_response)
                                    captcha_solved = True
                                    logger.info("✅ Image CAPTCHA solved")
                    except Exception:
                        logger.info("No image CAPTCHA found")
                
                # Find and click submit button
                try:
                    submit_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "btnAceptar"))
                    )
                    self.driver.execute_script("arguments[0].click();", submit_button)
                    logger.info("✅ Clicked submit button")
                except Exception as e:
                    logger.warning(f"⚠️ Could not click btnAceptar: {e}")
                    # Try alternative submit methods
                    try:
                        submit_button = self.driver.find_element(By.CSS_SELECTOR, "input[type='submit'], button[type='submit']")
                        self.driver.execute_script("arguments[0].click();", submit_button)
                    except:
                        # Try form submission
                        try:
                            form = self.driver.find_element(By.ID, "form01")
                            self.driver.execute_script("arguments[0].submit();", form)
                        except:
                            logger.error("❌ Could not submit form")
                            continue
                
                # Wait for results page with multiple conditions
                try:
                    WebDriverWait(self.driver, 20).until(
                        lambda driver: (
                            "FrameCriterioBusquedaWeb.jsp" not in driver.current_url or
                            driver.find_elements(By.CSS_SELECTOR, "table") or
                            driver.find_elements(By.CSS_SELECTOR, ".result") or
                            "error" in driver.current_url.lower()
                        )
                    )
                except TimeoutException:
                    logger.warning("⚠️ Timeout waiting for results page")
                
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
            # Wait a bit for page to load
            time.sleep(2)
            
            # Check if we're on an error page
            page_text = self.driver.page_source.lower()
            if any(error in page_text for error in ["no se encontró", "no existe", "error", "no encontrado"]):
                logger.warning("⚠️ Error page detected - RUC not found")
                return None
                
            # Log current URL for debugging
            logger.info(f"📍 Current URL: {self.driver.current_url}")
            
            # Multiple strategies to find company name
            strategies = [
                self._extract_by_table_structure,
                self._extract_by_label_text,
                self._extract_by_css_patterns,
                self._extract_by_content_analysis
            ]
            
            for strategy in strategies:
                try:
                    result = strategy()
                    if result:
                        logger.info(f"✅ Found company name using {strategy.__name__}: {result}")
                        return result
                except Exception as e:
                    logger.debug(f"Strategy {strategy.__name__} failed: {e}")
                    continue
            
            logger.warning("⚠️ No company name found with any strategy")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error extracting company name: {e}")
            return None
    
    def _extract_by_table_structure(self):
        """Extract company name from table structure"""
        # Look for table with company information
        tables = self.driver.find_elements(By.TAG_NAME, "table")
        
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    label_cell = cells[0].text.strip().lower()
                    value_cell = cells[1].text.strip()
                    
                    # Check if this row contains company name
                    if any(keyword in label_cell for keyword in [
                        "razón social", "razon social", "nombre", "denominación",
                        "company name", "business name"
                    ]):
                        if value_cell and len(value_cell) > 3:
                            return value_cell
        
        return None
    
    def _extract_by_label_text(self):
        """Extract company name by finding label text"""
        # Use JavaScript to find elements by text content
        elements = self.driver.execute_script("""
            return Array.from(document.querySelectorAll('td, th, span, div, label')).filter(el => {
                const text = el.textContent.toLowerCase();
                return text.includes('razón social') || 
                       text.includes('razon social') ||
                       text.includes('nombre') ||
                       text.includes('denominación');
            });
        """)
        
        for element in elements:
            # Try to find the next sibling or parent that might contain the name
            next_sibling = self.driver.execute_script("return arguments[0].nextElementSibling;", element)
            if next_sibling:
                name = next_sibling.text.strip()
                if name and len(name) > 3 and not name.lower().startswith('razón'):
                    return name
            
            # Try parent's next sibling
            parent = self.driver.execute_script("return arguments[0].parentElement;", element)
            if parent:
                next_sibling = self.driver.execute_script("return arguments[0].nextElementSibling;", parent)
                if next_sibling:
                    name = next_sibling.text.strip()
                    if name and len(name) > 3 and not name.lower().startswith('razón'):
                        return name
        
        return None
    
    def _extract_by_css_patterns(self):
        """Extract company name using CSS patterns"""
        # Common CSS selectors for company information
        selectors = [
            "[class*='razon']",
            "[class*='nombre']",
            "[class*='company']",
            "[class*='business']",
            "[id*='razon']",
            "[id*='nombre']",
            ".content td:nth-child(2)",
            ".result td:nth-child(2)",
            "table.data td:nth-child(2)"
        ]
        
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and len(text) > 5 and not text.isdigit():
                        # Additional validation
                        if not any(skip in text.lower() for skip in ['ruc', 'documento', 'número', 'codigo']):
                            return text
            except Exception:
                continue
        
        return None
    
    def _extract_by_content_analysis(self):
        """Extract company name by analyzing all content"""
        # Get all text content from the page
        all_tds = self.driver.find_elements(By.TAG_NAME, "td")
        
        # Filter potential company names
        candidates = []
        
        for td in all_tds:
            text = td.text.strip()
            if text and len(text) > 10:
                # Skip if it's clearly not a company name
                if any(skip in text.lower() for skip in [
                    'ruc', 'documento', 'número', 'codigo', 'fecha', 'estado',
                    'dirección', 'distrito', 'provincia', 'departamento',
                    'teléfono', 'email', 'www', 'http'
                ]):
                    continue
                
                # Skip if it's all numbers
                if text.isdigit():
                    continue
                
                # Skip if it's too short or too long
                if len(text) < 10 or len(text) > 200:
                    continue
                
                # Skip if it contains too many special characters
                special_chars = sum(1 for c in text if not c.isalnum() and c not in ' .-&,()[]')
                if special_chars > len(text) * 0.3:
                    continue
                
                candidates.append(text)
        
        # Return the most likely candidate (first one that passes all filters)
        if candidates:
            return candidates[0]
        
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
            driver_restarts = 0
            max_driver_restarts = 3
            
            for i, ruc in enumerate(rucs, 1):
                logger.info(f"📊 Progress: {i}/{len(rucs)} ({(i/len(rucs)*100):.1f}%)")
                
                # Restart driver if it's been used too much
                if i > 1 and i % 50 == 0:
                    logger.info("🔄 Restarting driver after 50 requests")
                    try:
                        self.driver.quit()
                        time.sleep(2)
                        if not self.setup_driver():
                            logger.error("❌ Failed to restart driver")
                            break
                    except Exception as e:
                        logger.error(f"❌ Driver restart failed: {e}")
                        break
                
                company_name = self.scrape_company_name(ruc)
                
                if company_name:
                    if self.update_database(ruc, company_name):
                        successful += 1
                    else:
                        failed += 1
                else:
                    failed += 1
                    
                    # Check if driver is still responsive
                    try:
                        self.driver.current_url
                    except (WebDriverException, Exception) as e:
                        logger.warning(f"⚠️ Driver seems unresponsive: {e}")
                        if driver_restarts < max_driver_restarts:
                            logger.info("🔄 Attempting to restart driver")
                            try:
                                self.driver.quit()
                                time.sleep(3)
                                if self.setup_driver():
                                    driver_restarts += 1
                                    logger.info("✅ Driver restarted successfully")
                                else:
                                    logger.error("❌ Failed to restart driver")
                                    break
                            except Exception as restart_error:
                                logger.error(f"❌ Driver restart failed: {restart_error}")
                                break
                        else:
                            logger.error("❌ Max driver restarts reached")
                            break
                
                # Progress logging
                if i % 10 == 0:
                    logger.info(f"📈 Batch progress: {successful} successful, {failed} failed")
                    
                # Longer delay every 20 requests to avoid being blocked
                if i % 20 == 0:
                    delay = random.uniform(10, 20)
                    logger.info(f"😴 Taking longer break: {delay:.1f} seconds")
                    time.sleep(delay)
            
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