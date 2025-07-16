# SUNAT RUC Scraper

A comprehensive scraping solution for collecting company information from SUNAT (Superintendencia Nacional de Aduanas y de Administración Tributaria) and other Peruvian business data sources.

## Features

- Multiple scraping sources:
  - SUNAT official website (e-consultaruc.sunat.gob.pe)
  - Peru Consult API
  - APIs.net.pe
  - RUC Lookup service
- Captcha solving integration (2Captcha, Anti-Captcha)
- PostgreSQL database storage
- Concurrent scraping with thread pools
- Progress monitoring and logging
- Undetected Chrome driver for avoiding detection

## Requirements

- Python 3.8+
- PostgreSQL database
- Chrome/Chromium browser
- Captcha solving service account (optional)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd sunatscraper
```

2. Install dependencies:
```bash
./install.sh
```

Or manually:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials and API keys
```

## Configuration

Create a `.env` file with your configuration:

```env
# Database Configuration
DB_HOST=your_database_host
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password

# Captcha Services (optional)
TWOCAPTCHA_API_KEY=your_2captcha_api_key
ANTICAPTCHA_API_KEY=your_anticaptcha_api_key
```

## Usage

### Main SUNAT Scraper
```bash
python3 sunat_scraper.py
```

### Alternative API Scrapers
```bash
python3 peru_consult_scraper.py
python3 api_ruc_scraper.py
python3 apis_net_pe_scraper.py
```

### Database Setup
```bash
python3 create_ruc_database.py
python3 setup_optimized_database.py
```

### Monitoring Progress
```bash
python3 monitor_progress.py
python3 monitor_production.py
```

## Project Structure

- `sunat_scraper.py` - Main SUNAT website scraper
- `peru_consult_scraper.py` - Peru Consult API integration
- `api_ruc_scraper.py` - Alternative RUC API scraper
- `apis_net_pe_scraper.py` - APIs.net.pe scraper
- `ruc_lookup_scraper.py` - RUC lookup service integration
- `create_ruc_database.py` - Database initialization script
- `monitor_*.py` - Progress monitoring tools
- `test_*.py` - Testing scripts

## Database Schema

The scrapers use PostgreSQL with tables for storing:
- Company RUC numbers
- Company names (razón social)
- Scraping status and timestamps
- Error logs

## Legal Notice

This tool is for educational and legitimate business intelligence purposes only. Users must comply with:
- SUNAT's terms of service
- Peruvian data protection laws
- Rate limiting and ethical scraping practices

## License

[Specify your license here]

## Contributing

[Add contribution guidelines if applicable]