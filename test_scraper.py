#!/usr/bin/env python3
"""
Test script for the SUNAT scraper
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sunat_scraper import SUNATScraper

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def test_scraper():
    """Test the scraper with a known RUC"""
    
    # Test RUCs - these are public companies that should exist
    test_rucs = [
        "20100070970",  # Banco de Credito del Peru
        "20100139436",  # Banco de la Nacion
        "20131312955",  # Telefonica del Peru
    ]
    
    logger.info("🚀 Starting SUNAT scraper test")
    
    scraper = SUNATScraper()
    
    # Test database connection
    if not scraper.setup_database():
        logger.error("❌ Database setup failed - check your .env file")
        return False
    
    # Test driver setup
    if not scraper.setup_driver():
        logger.error("❌ Driver setup failed")
        return False
    
    logger.info("✅ Setup completed successfully")
    
    # Test scraping for each RUC
    successful = 0
    failed = 0
    
    for ruc in test_rucs:
        logger.info(f"🧪 Testing RUC: {ruc}")
        
        try:
            company_name = scraper.scrape_company_name(ruc)
            
            if company_name:
                logger.info(f"✅ Success: {ruc} -> {company_name}")
                successful += 1
            else:
                logger.warning(f"⚠️ Failed: {ruc} -> No company name found")
                failed += 1
                
        except Exception as e:
            logger.error(f"❌ Error testing {ruc}: {e}")
            failed += 1
    
    # Cleanup
    scraper.cleanup()
    
    # Summary
    logger.info(f"🎯 Test Results: {successful} successful, {failed} failed")
    
    if successful > 0:
        logger.info("✅ Scraper is working!")
        return True
    else:
        logger.error("❌ Scraper failed all tests")
        return False

if __name__ == "__main__":
    success = test_scraper()
    sys.exit(0 if success else 1)