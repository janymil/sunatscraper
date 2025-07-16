#!/usr/bin/env python3
"""
Alternative CAPTCHA solving methods - including free options
"""

import time
import os
import requests
import base64
from io import BytesIO
from PIL import Image
import pytesseract
from selenium.webdriver.common.by import By

class FreeCaptchaSolver:
    def __init__(self, driver):
        self.driver = driver
    
    def solve_image_captcha_ocr(self, captcha_element):
        """
        Free method: Use OCR (Tesseract) for simple image CAPTCHAs
        Works for text-based CAPTCHAs but not reCAPTCHA
        """
        try:
            print("🔍 Attempting OCR-based CAPTCHA solving...")
            
            # Take screenshot of CAPTCHA
            captcha_screenshot = captcha_element.screenshot_as_png
            image = Image.open(BytesIO(captcha_screenshot))
            
            # Preprocess image for better OCR
            image = image.convert('L')  # Convert to grayscale
            image = image.resize((image.width * 3, image.height * 3))  # Upscale
            
            # Use Tesseract OCR
            captcha_text = pytesseract.image_to_string(image, config='--psm 8 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
            captcha_text = captcha_text.strip().replace(' ', '')
            
            if len(captcha_text) >= 4:
                print(f"✅ OCR result: {captcha_text}")
                return captcha_text
            else:
                print("❌ OCR failed - text too short")
                return None
                
        except Exception as e:
            print(f"❌ OCR CAPTCHA solving failed: {e}")
            return None
    
    def solve_recaptcha_free(self, site_key, page_url):
        """
        Free method: Use free APIs (limited requests per day)
        """
        try:
            print("🔍 Attempting free reCAPTCHA solving...")
            
            # Method 1: Try free tier of various services
            free_apis = [
                {
                    'name': 'Anti-Captcha Free Tier',
                    'url': 'https://api.anti-captcha.com/createTask',
                    'free_credits': 'Limited free credits for new accounts'
                },
                {
                    'name': 'CapMonster Cloud Free',
                    'url': 'https://api.capmonster.cloud/createTask', 
                    'free_credits': '$0.50 free credits for new accounts'
                }
            ]
            
            print("📋 Available free options:")
            for api in free_apis:
                print(f"   - {api['name']}: {api['free_credits']}")
            
            # These require API keys even for free tiers
            print("⚠️ Free APIs still require registration and API keys")
            return None
            
        except Exception as e:
            print(f"❌ Free reCAPTCHA solving failed: {e}")
            return None
    
    def manual_captcha_prompt(self):
        """
        Manual method: Prompt user to solve CAPTCHA
        """
        print("🖐️ Manual CAPTCHA solving required")
        print("📱 Options:")
        print("   1. Run scraper in non-headless mode")
        print("   2. Solve CAPTCHA manually in browser")
        print("   3. Use browser automation with manual intervention")
        
        return input("Enter CAPTCHA solution manually: ").strip()

def install_tesseract():
    """Install Tesseract OCR for free image CAPTCHA solving"""
    try:
        import subprocess
        print("📦 Installing Tesseract OCR...")
        subprocess.run(['apt', 'update'], check=True)
        subprocess.run(['apt', 'install', '-y', 'tesseract-ocr', 'tesseract-ocr-eng'], check=True)
        subprocess.run(['pip', 'install', 'pytesseract', 'Pillow'], check=True)
        print("✅ Tesseract OCR installed successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to install Tesseract: {e}")
        return False

if __name__ == "__main__":
    print("🆓 Free CAPTCHA Solving Options:")
    print()
    print("1. 📸 OCR Method (Tesseract)")
    print("   - Free and open source")
    print("   - Works for simple text CAPTCHAs")
    print("   - NOT effective for reCAPTCHA")
    print()
    print("2. 🎁 Free API Credits")
    print("   - 2captcha.com: ~$0.50-1.00 free for new accounts")
    print("   - Anti-captcha.com: Limited free credits")
    print("   - CapMonster.cloud: $0.50 free credits")
    print()
    print("3. 🖐️ Manual Solving")
    print("   - Run in non-headless mode")
    print("   - Solve manually when prompted")
    print("   - Good for testing/small batches")
    print()
    print("4. 🔄 Session Reuse")
    print("   - Solve CAPTCHA once, reuse session")
    print("   - Limited effectiveness")
    print()
    
    choice = input("Install Tesseract OCR for free image CAPTCHA solving? (y/n): ")
    if choice.lower() == 'y':
        install_tesseract()