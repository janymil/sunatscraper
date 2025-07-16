#!/bin/bash
# SUNAT Scraper Installation Script for Ubuntu VPS

echo "🚀 Installing SUNAT Scraper..."

# Update system
echo "📦 Updating system packages..."
apt update && apt upgrade -y

# Install Python and pip
echo "🐍 Installing Python..."
apt install -y python3 python3-pip python3-venv

# Install Chrome dependencies
echo "🌐 Installing Chrome and dependencies..."
apt install -y wget gnupg2 software-properties-common apt-transport-https ca-certificates curl

# Add Google Chrome repository
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add -
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list

# Install Google Chrome
apt update
apt install -y google-chrome-stable

# Install additional dependencies
apt install -y xvfb unzip

# Create virtual environment
echo "📁 Setting up Python environment..."
cd /root/sunatscraper
python3 -m venv venv
source venv/bin/activate

# Install Python packages
echo "📚 Installing Python packages..."
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "⚙️ Creating .env file..."
    cp .env.example .env
    echo "✏️ Please edit .env file with your settings"
fi

# Make scripts executable
chmod +x *.sh
chmod +x *.py

# Create systemd service (optional)
echo "🔧 Creating systemd service..."
cat > /etc/systemd/system/sunat-scraper.service << EOF
[Unit]
Description=SUNAT Company Data Scraper
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/sunatscraper
Environment=PATH=/root/sunatscraper/venv/bin
ExecStart=/root/sunatscraper/venv/bin/python /root/sunatscraper/sunat_scraper.py
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
systemctl daemon-reload

echo "✅ Installation completed!"
echo ""
echo "📋 Next steps:"
echo "1. Edit .env file with your CAPTCHA API key"
echo "2. Test: ./run_test.sh"
echo "3. Run scraper: ./run_scraper.sh"
echo "4. Monitor: tail -f sunat_scraper.log"
echo ""
echo "🔧 Service commands:"
echo "- Start: systemctl start sunat-scraper"
echo "- Stop: systemctl stop sunat-scraper"
echo "- Status: systemctl status sunat-scraper"
echo "- Enable auto-start: systemctl enable sunat-scraper"