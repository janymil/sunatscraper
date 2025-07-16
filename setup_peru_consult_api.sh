#!/bin/bash
# Setup Peru Consult API using Docker

echo "🚀 Setting up Peru Consult API..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "📦 Installing Docker..."
    apt update
    apt install -y docker.io
    systemctl start docker
    systemctl enable docker
fi

# Generate a random API token
API_TOKEN=$(openssl rand -hex 16)
echo "🔑 Generated API token: $API_TOKEN"

# Run the Peru Consult API container
echo "🐳 Starting Peru Consult API container..."
docker run -d \
    --name peru-consult-api \
    --restart unless-stopped \
    -p 8080:8080 \
    -e API_TOKEN=$API_TOKEN \
    giansalex/peru-consult-api

# Wait for container to start
echo "⏳ Waiting for API to start..."
sleep 10

# Test the API
echo "🧪 Testing API..."
TEST_RUC="20131312955"
curl -H "Accept: application/json" \
     "http://localhost:8080/api/v1/ruc/$TEST_RUC?token=$API_TOKEN"

echo ""
echo "✅ Peru Consult API setup complete!"
echo "🔗 API URL: http://localhost:8080"
echo "🔑 API Token: $API_TOKEN"
echo ""
echo "Add this to your .env file:"
echo "PERU_CONSULT_API_URL=http://localhost:8080"
echo "PERU_CONSULT_API_TOKEN=$API_TOKEN"