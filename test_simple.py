#!/usr/bin/env python3
"""
Simple test script to check SUNAT website access
"""

import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def test_sunat_access():
    print("🧪 Testing SUNAT website access...")
    
    # Setup Chrome
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = uc.Chrome(options=options, version_main=None)
    
    try:
        print("📡 Navigating to SUNAT...")
        driver.get("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp")
        
        print("⏳ Waiting for page to load...")
        time.sleep(5)
        
        print("📄 Current page title:", driver.title)
        print("🔗 Current URL:", driver.current_url)
        
        # Check for key elements
        try:
            ruc_input = driver.find_element(By.ID, "txtRuc")
            print("✅ Found RUC input field")
        except:
            print("❌ RUC input field not found")
            
        try:
            radio_button = driver.find_element(By.ID, "rbtnTipo01")
            print("✅ Found RUC radio button")
        except:
            print("❌ RUC radio button not found")
            
        try:
            submit_btn = driver.find_element(By.ID, "btnAceptar")
            print("✅ Found submit button")
        except:
            print("❌ Submit button not found")
            
        # Check for CAPTCHA
        try:
            captcha = driver.find_element(By.CSS_SELECTOR, "[data-sitekey]")
            print("🔐 CAPTCHA detected:", captcha.get_attribute("data-sitekey"))
        except:
            print("✅ No CAPTCHA detected")
            
        # Get page source length
        page_source = driver.page_source
        print(f"📏 Page source length: {len(page_source)} characters")
        
        # Check for error messages or blocks
        if "blocked" in page_source.lower() or "access denied" in page_source.lower():
            print("🚫 Possible access blocked")
        else:
            print("✅ No obvious blocking detected")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    test_sunat_access()