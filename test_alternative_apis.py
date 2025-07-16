#!/usr/bin/env python3
"""
Test alternative RUC APIs to find the best performing one
"""

import requests
import time
import json

def test_consulta_peru_api():
    """Test the consulta-peru open source API"""
    print("🧪 Testing consulta-peru API...")
    
    # This seems to be self-hosted, let's try common endpoints
    base_urls = [
        "https://api.consulta-peru.com",
        "https://consulta-peru.herokuapp.com", 
        "https://consulta-peru.vercel.app"
    ]
    
    test_ruc = "20131312955"  # SUNAT's RUC
    
    for base_url in base_urls:
        try:
            url = f"{base_url}/ruc/{test_ruc}"
            print(f"   Trying: {url}")
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                company_name = data.get('nombre__razonsocial') or data.get('razonSocial') or data.get('name')
                
                if company_name:
                    print(f"   ✅ SUCCESS: {company_name}")
                    return base_url
                else:
                    print(f"   ⚠️ No company name in response")
                    print(f"   Response: {data}")
            else:
                print(f"   ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None

def test_apis_net_pe():
    """Test apis.net.pe (requires token)"""
    print("🧪 Testing apis.net.pe...")
    
    # This requires authentication, let's see if it has public endpoints
    test_ruc = "20131312955"
    
    endpoints = [
        f"https://api.apis.net.pe/v1/ruc?numero={test_ruc}",
        f"https://api.apis.net.pe/v2/sunat/ruc?numero={test_ruc}",
        f"https://apis.net.pe/api/v1/ruc/{test_ruc}",
    ]
    
    for url in endpoints:
        try:
            print(f"   Trying: {url}")
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ SUCCESS: {data}")
                return url
            elif response.status_code == 401:
                print(f"   🔐 Requires authentication")
            else:
                print(f"   ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None

def test_other_public_apis():
    """Test other public RUC APIs"""
    print("🧪 Testing other public APIs...")
    
    test_ruc = "20131312955"
    
    # Try various API patterns
    endpoints = [
        f"https://ruc.pe/api/v1/ruc/{test_ruc}",
        f"https://api.ruc.pe/v1/ruc/{test_ruc}",
        f"https://consultaruc.pe/api/ruc/{test_ruc}",
        f"https://api.sunat.pe/ruc/{test_ruc}",
        f"https://apisperu.com/api/ruc/{test_ruc}",
        f"https://www.datos.gob.pe/api/ruc/{test_ruc}",
    ]
    
    for url in endpoints:
        try:
            print(f"   Trying: {url}")
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Response: {data}")
                
                # Look for company name in various fields
                company_name = None
                for field in ['razonSocial', 'nombre__razonsocial', 'name', 'companyName', 'nombreComercial']:
                    if field in data:
                        company_name = data[field]
                        break
                
                if company_name:
                    print(f"   ✅ Found company: {company_name}")
                    return url
                    
            else:
                print(f"   ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None

def main():
    """Test all alternative APIs"""
    print("🔍 Testing Alternative RUC APIs")
    print("=" * 50)
    
    working_apis = []
    
    # Test consulta-peru
    api1 = test_consulta_peru_api()
    if api1:
        working_apis.append(('consulta-peru', api1))
    
    print()
    
    # Test apis.net.pe
    api2 = test_apis_net_pe()
    if api2:
        working_apis.append(('apis.net.pe', api2))
    
    print()
    
    # Test other public APIs
    api3 = test_other_public_apis()
    if api3:
        working_apis.append(('other', api3))
    
    print()
    print("📊 RESULTS:")
    print("=" * 30)
    
    if working_apis:
        print("✅ Working APIs found:")
        for name, url in working_apis:
            print(f"   - {name}: {url}")
    else:
        print("❌ No working public APIs found")
        print("   Recommendation: Continue with Peru Consult API or register for paid services")

if __name__ == "__main__":
    main()