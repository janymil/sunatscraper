# SUNAT Scraper Updates

## Overview
Updated the SUNAT scraper to work with the current website structure at https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp

## Key Improvements Made

### 1. Enhanced CAPTCHA Handling
- **reCAPTCHA v3 Support**: Added support for reCAPTCHA v3 with token-based verification
- **Image CAPTCHA Support**: Added fallback support for traditional image CAPTCHAs
- **Dual CAPTCHA Strategy**: The scraper now attempts both reCAPTCHA v3 and image CAPTCHA solving

### 2. Improved Element Detection
- **Multiple Selection Strategies**: Added fallback methods for radio button selection
- **Enhanced Form Submission**: Multiple fallback methods for form submission
- **Better Error Handling**: More robust error handling for missing elements

### 3. Advanced Company Name Extraction
- **Multi-Strategy Extraction**: Implemented 4 different strategies:
  1. **Table Structure Analysis**: Looks for standard table layouts
  2. **Label Text Matching**: Finds labels like "Razón Social" and extracts adjacent values
  3. **CSS Pattern Matching**: Uses CSS selectors to find company information
  4. **Content Analysis**: Analyzes all page content to identify likely company names

### 4. Enhanced Stealth Capabilities
- **Updated User Agent**: Using more recent Chrome user agent
- **Additional Stealth Arguments**: More Chrome arguments to avoid detection
- **Peru-specific Localization**: Added Spanish/Peru language preferences
- **Permission Override**: Added permission query overrides

### 5. Better Error Recovery
- **Driver Restart Logic**: Automatically restarts the driver every 50 requests
- **Connection Monitoring**: Monitors driver responsiveness and restarts if needed
- **Progressive Delays**: Longer delays every 20 requests to avoid being blocked
- **Error Page Detection**: Detects error pages and handles them appropriately

### 6. Improved Logging and Debugging
- **Detailed Logging**: More comprehensive logging for debugging
- **Strategy Success Tracking**: Logs which extraction strategy worked
- **URL Tracking**: Logs current URL for debugging
- **Progress Indicators**: Better progress reporting

## New Functions Added

### CAPTCHA Functions
- `solve_captcha()`: Enhanced reCAPTCHA v3 solving
- `solve_image_captcha()`: New image CAPTCHA solving

### Extraction Functions
- `_extract_by_table_structure()`: Extract from table layouts
- `_extract_by_label_text()`: Extract using label text matching
- `_extract_by_css_patterns()`: Extract using CSS selectors
- `_extract_by_content_analysis()`: Extract by analyzing all content

## Configuration Updates

### Environment Variables
The scraper now supports both 2CAPTCHA and ANTICAPTCHA services:
```env
TWOCAPTCHA_API_KEY=your_2captcha_api_key
ANTICAPTCHA_API_KEY=your_anticaptcha_api_key
```

### New Features
- **Headless Mode**: Can run in headless mode for server deployment
- **Batch Processing**: Improved batch processing with automatic driver restarts
- **Retry Logic**: Enhanced retry logic with exponential backoff

## Testing
- Created `test_scraper.py` for testing the scraper functionality
- Added test RUCs for validation
- Comprehensive error handling and reporting

## Usage
The scraper maintains the same interface but with improved reliability:

```python
from sunat_scraper import SUNATScraper

scraper = SUNATScraper()
scraper.run_batch_scraping(batch_size=50)
```

## Compatibility
- Works with the current SUNAT website structure
- Handles both reCAPTCHA v3 and image CAPTCHAs
- Compatible with various response formats
- Robust error handling for website changes

## Performance Improvements
- Automatic driver restarts prevent memory leaks
- Progressive delays prevent IP blocking
- Multiple extraction strategies increase success rate
- Better resource management and cleanup