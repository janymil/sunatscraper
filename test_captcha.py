#!/usr/bin/env python3
"""
Test script to detect and handle CAPTCHA on SUNAT website
"""

import time
import os
from dotenv import load_dotenv
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from twocaptcha import TwoCaptcha

load_dotenv()

def test_captcha_detection():
    print("🔐 Testing CAPTCHA detection on SUNAT website...")
    
    # Check if API key is configured
    api_key = os.getenv('2CAPTCHA_API_KEY')
    if not api_key:
        print("❌ 2CAPTCHA_API_KEY not configured in .env file")
        print("📋 To configure CAPTCHA solving:")
        print("   1. Go to https://2captcha.com/")
        print("   2. Register and add funds ($1-2 minimum)")
        print("   3. Get your API key")
        print("   4. Edit .env file and set: 2CAPTCHA_API_KEY=your_key_here")
        return False
    else:
        print(f"✅ 2CAPTCHA API key found: {api_key[:8]}...")
    
    # Setup Chrome with stealth
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    options.add_argument(f'--user-agent={user_agent}')
    
    driver = uc.Chrome(options=options, version_main=None)
    
    try:
        print("🌐 Navigating to SUNAT...")
        driver.get("https://e-consultaruc.sunat.gob.pe/")
        time.sleep(3)
        
        driver.get("https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp")
        
        print("⏳ Waiting for page to load...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "txtRuc"))
        )
        
        print("✅ Page loaded successfully")
        print("🔍 Checking for CAPTCHA...")
        
        # Check for different CAPTCHA types
        captcha_found = False
        
        # Check for reCAPTCHA
        try:
            recaptcha = driver.find_element(By.CSS_SELECTOR, "[data-sitekey]")
            site_key = recaptcha.get_attribute("data-sitekey")
            print(f"🔐 reCAPTCHA detected with site key: {site_key}")
            captcha_found = True
            
            if api_key:
                print("🧪 Testing CAPTCHA solving...")
                solver = TwoCaptcha(api_key)
                try:
                    result = solver.recaptcha(sitekey=site_key, url=driver.current_url)
                    print(f"✅ CAPTCHA solved successfully: {result['code'][:20]}...")
                    return True
                except Exception as e:
                    print(f"❌ CAPTCHA solving failed: {e}")
                    return False
            
        except:
            print("ℹ️ No reCAPTCHA found")
        
        # Check for image CAPTCHA
        try:
            img_captcha = driver.find_element(By.CSS_SELECTOR, "img[src*='captcha'], img[alt*='captcha']")
            print("🖼️ Image CAPTCHA detected")
            captcha_found = True
        except:
            print("ℹ️ No image CAPTCHA found")
        
        if not captcha_found:
            print("✅ No CAPTCHA detected - ready to proceed!")
            
            # Try filling form
            print("📝 Testing form interaction...")
            ruc_radio = driver.find_element(By.ID, "rbtnTipo01")
            driver.execute_script("arguments[0].click();", ruc_radio)
            
            ruc_input = driver.find_element(By.ID, "txtRuc")
            ruc_input.send_keys("20100070970")  # Test RUC
            
            print("✅ Form interaction successful")
            return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
        
    finally:
        driver.quit()
    
    return captcha_found

if __name__ == "__main__":
    success = test_captcha_detection()
    if success:
        print("🎉 CAPTCHA test completed successfully!")
    else:
        print("⚠️ CAPTCHA configuration needed")