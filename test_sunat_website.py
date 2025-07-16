#!/usr/bin/env python3
"""
Test script to analyze current SUNAT website structure
"""

import os
import sys
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_sunat_website():
    """Test SUNAT website structure"""
    driver = None
    try:
        # Setup Chrome driver
        options = uc.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        
        driver = uc.Chrome(options=options)
        
        # Navigate to SUNAT
        logger.info("🌐 Navigating to SUNAT website...")
        driver.get("https://e-consultaruc.sunat.gob.pe/")
        time.sleep(3)
        
        logger.info(f"Current URL: {driver.current_url}")
        logger.info(f"Page title: {driver.title}")
        
        # Try to navigate to search page
        logger.info("🔍 Trying to access search page...")
        driver.get("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp")
        time.sleep(3)
        
        logger.info(f"Search page URL: {driver.current_url}")
        logger.info(f"Search page title: {driver.title}")
        
        # Analyze page structure
        logger.info("📋 Analyzing page structure...")
        
        # Check for form
        try:
            form = driver.find_element(By.ID, "form01")
            logger.info("✅ Found form with ID 'form01'")
        except NoSuchElementException:
            logger.warning("⚠️ Form 'form01' not found")
            # Try to find any form
            forms = driver.find_elements(By.TAG_NAME, "form")
            logger.info(f"Found {len(forms)} forms on page")
        
        # Check for RUC radio button
        try:
            ruc_radio = driver.find_element(By.ID, "rbtnTipo01")
            logger.info("✅ Found RUC radio button")
        except NoSuchElementException:
            logger.warning("⚠️ RUC radio button not found")
            # Try to find any radio buttons
            radios = driver.find_elements(By.CSS_SELECTOR, "input[type='radio']")
            logger.info(f"Found {len(radios)} radio buttons")
            for i, radio in enumerate(radios):
                logger.info(f"  Radio {i}: ID={radio.get_attribute('id')}, Name={radio.get_attribute('name')}")
        
        # Check for RUC input
        try:
            ruc_input = driver.find_element(By.ID, "txtRuc")
            logger.info("✅ Found RUC input field")
        except NoSuchElementException:
            logger.warning("⚠️ RUC input field not found")
            # Try to find any text inputs
            inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text']")
            logger.info(f"Found {len(inputs)} text inputs")
            for i, inp in enumerate(inputs):
                logger.info(f"  Input {i}: ID={inp.get_attribute('id')}, Name={inp.get_attribute('name')}")
        
        # Check for submit button
        try:
            submit_btn = driver.find_element(By.ID, "btnAceptar")
            logger.info("✅ Found submit button")
        except NoSuchElementException:
            logger.warning("⚠️ Submit button 'btnAceptar' not found")
            # Try to find any submit buttons
            buttons = driver.find_elements(By.CSS_SELECTOR, "input[type='submit'], button")
            logger.info(f"Found {len(buttons)} buttons")
            for i, btn in enumerate(buttons):
                logger.info(f"  Button {i}: ID={btn.get_attribute('id')}, Type={btn.get_attribute('type')}, Text={btn.text}")
        
        # Check for captcha
        try:
            captcha_img = driver.find_element(By.ID, "imgCaptcha")
            logger.info("✅ Found captcha image")
        except NoSuchElementException:
            logger.warning("⚠️ Captcha image not found")
            # Try to find any captcha elements
            captcha_elements = driver.find_elements(By.CSS_SELECTOR, "[id*='captcha'], [class*='captcha']")
            logger.info(f"Found {len(captcha_elements)} captcha-related elements")
        
        # Check for reCAPTCHA
        try:
            recaptcha = driver.find_element(By.CSS_SELECTOR, "[data-sitekey]")
            site_key = recaptcha.get_attribute("data-sitekey")
            logger.info(f"✅ Found reCAPTCHA with site key: {site_key}")
        except NoSuchElementException:
            logger.warning("⚠️ reCAPTCHA not found")
        
        # Get page source snippet
        logger.info("📄 Page source analysis...")
        page_source = driver.page_source
        
        # Check for specific elements in source
        if "txtRuc" in page_source:
            logger.info("✅ txtRuc found in page source")
        if "btnAceptar" in page_source:
            logger.info("✅ btnAceptar found in page source")
        if "rbtnTipo01" in page_source:
            logger.info("✅ rbtnTipo01 found in page source")
        if "captcha" in page_source.lower():
            logger.info("✅ Captcha references found in page source")
        if "recaptcha" in page_source.lower():
            logger.info("✅ reCAPTCHA references found in page source")
        
        # Try a simple test search
        logger.info("🧪 Testing search functionality...")
        test_ruc = "20100070970"  # Example RUC
        
        try:
            # Click RUC radio if found
            try:
                ruc_radio = driver.find_element(By.ID, "rbtnTipo01")
                driver.execute_script("arguments[0].click();", ruc_radio)
                logger.info("✅ Clicked RUC radio button")
            except:
                logger.warning("⚠️ Could not click RUC radio button")
            
            # Enter RUC if input found
            try:
                ruc_input = driver.find_element(By.ID, "txtRuc")
                ruc_input.clear()
                ruc_input.send_keys(test_ruc)
                logger.info(f"✅ Entered test RUC: {test_ruc}")
            except:
                logger.warning("⚠️ Could not enter RUC")
            
            time.sleep(2)
            logger.info("✅ Test completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Test search failed: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Website test failed: {e}")
        return False
        
    finally:
        if driver:
            driver.quit()
            logger.info("🧹 Browser closed")

if __name__ == "__main__":
    logger.info("🚀 Starting SUNAT website test")
    success = test_sunat_website()
    
    if success:
        logger.info("✅ Website test completed")
    else:
        logger.error("❌ Website test failed")
        sys.exit(1)