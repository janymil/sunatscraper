#!/usr/bin/env python3
"""
Simple test to check SUNAT website response
"""

import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_sunat_simple():
    """Test SUNAT website with simple requests"""
    try:
        # Test main page
        logger.info("Testing main SUNAT page...")
        response = requests.get("https://e-consultaruc.sunat.gob.pe/", timeout=10)
        logger.info(f"Main page status: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            logger.info(f"Page title: {soup.title.string if soup.title else 'No title'}")
        
        # Test search page
        logger.info("Testing search page...")
        response = requests.get("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp", timeout=10)
        logger.info(f"Search page status: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            logger.info(f"Search page title: {soup.title.string if soup.title else 'No title'}")
            
            # Check for form elements
            form = soup.find('form', {'id': 'form01'})
            if form:
                logger.info("✅ Found form with ID 'form01'")
            else:
                logger.warning("⚠️ Form 'form01' not found")
                forms = soup.find_all('form')
                logger.info(f"Found {len(forms)} forms")
            
            # Check for RUC input
            ruc_input = soup.find('input', {'id': 'txtRuc'})
            if ruc_input:
                logger.info("✅ Found RUC input field")
            else:
                logger.warning("⚠️ RUC input field not found")
            
            # Check for submit button
            submit_btn = soup.find('input', {'id': 'btnAceptar'})
            if submit_btn:
                logger.info("✅ Found submit button")
            else:
                logger.warning("⚠️ Submit button not found")
                
            # Check for captcha
            captcha = soup.find('img', {'id': 'imgCaptcha'})
            if captcha:
                logger.info("✅ Found captcha image")
            else:
                logger.warning("⚠️ Captcha image not found")
                
            # Check for recaptcha
            recaptcha = soup.find(attrs={'data-sitekey': True})
            if recaptcha:
                logger.info(f"✅ Found reCAPTCHA with site key: {recaptcha.get('data-sitekey')}")
            else:
                logger.warning("⚠️ reCAPTCHA not found")
                
            # Show some of the HTML structure
            logger.info("\n=== HTML Structure Sample ===")
            if soup.body:
                # Find main content area
                main_content = soup.find('div', {'class': 'content'}) or soup.find('div', {'id': 'content'}) or soup.body
                if main_content:
                    text_sample = main_content.get_text()[:500]
                    logger.info(f"Content sample: {text_sample}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Simple test failed: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting simple SUNAT test")
    success = test_sunat_simple()
    
    if success:
        logger.info("✅ Simple test completed")
    else:
        logger.error("❌ Simple test failed")