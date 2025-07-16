#!/usr/bin/env python3
"""
Setup optimized database architecture:
1. Create separate ruc_cache table for scraped data
2. Analyze main table to get first 13M unique RUCs
3. Create efficient lookup system
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def setup_optimized_database():
    """Setup the optimized database structure"""
    
    print("🚀 Setting up optimized database architecture...")
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cursor = conn.cursor()
        
        print("✅ Connected to database")
        
        # 1. Create RUC cache table
        print("📋 Creating ruc_cache table...")
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ruc_cache (
                id SERIAL PRIMARY KEY,
                ruc VARCHAR(11) UNIQUE NOT NULL,
                razon_social TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        
        # 2. Create indexes for performance
        print("⚡ Creating indexes...")
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ruc_cache_ruc ON ruc_cache(ruc);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ruc_cache_scraped ON ruc_cache(scraped_at);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sunat_empresas_ruc ON sunat_empresas(ruc);')
        
        # 3. Get statistics
        print("📊 Analyzing current data...")
        
        cursor.execute('SELECT COUNT(*) FROM sunat_empresas')
        total_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT ruc) FROM sunat_empresas')
        unique_rucs = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM sunat_empresas WHERE razon_social IS NOT NULL')
        already_scraped = cursor.fetchone()[0]
        
        print(f"📊 Database Statistics:")
        print(f"   - Total records: {total_records:,}")
        print(f"   - Unique RUCs: {unique_rucs:,}")
        print(f"   - Already scraped: {already_scraped:,}")
        
        # 4. Get first 13M unique RUCs ordered by ID
        print("🔍 Getting first 13M unique RUCs...")
        cursor.execute('''
            SELECT DISTINCT ruc 
            FROM sunat_empresas 
            ORDER BY MIN(id) 
            LIMIT 13000000
        ''')
        
        target_rucs = cursor.fetchall()
        print(f"✅ Found {len(target_rucs):,} unique RUCs in first 13M records")
        
        # 5. Insert unique RUCs into cache table (if not exists)
        print("📥 Populating ruc_cache with target RUCs...")
        
        # Insert in batches for performance
        batch_size = 10000
        inserted = 0
        
        for i in range(0, len(target_rucs), batch_size):
            batch = target_rucs[i:i + batch_size]
            
            # Use INSERT ... ON CONFLICT DO NOTHING for efficiency
            values = ','.join([f"('{ruc[0]}')" for ruc in batch])
            cursor.execute(f'''
                INSERT INTO ruc_cache (ruc) 
                VALUES {values}
                ON CONFLICT (ruc) DO NOTHING
            ''')
            
            inserted += cursor.rowcount
            
            if (i + batch_size) % 100000 == 0:
                print(f"   📥 Processed {i + batch_size:,} RUCs...")
                conn.commit()
        
        conn.commit()
        print(f"✅ Inserted {inserted:,} new RUCs into cache")
        
        # 6. Create lookup function
        print("🔧 Creating lookup function...")
        cursor.execute('''
            CREATE OR REPLACE FUNCTION get_razon_social(input_ruc VARCHAR(11))
            RETURNS TEXT AS $$
            DECLARE
                result TEXT;
            BEGIN
                SELECT razon_social INTO result 
                FROM ruc_cache 
                WHERE ruc = input_ruc 
                AND razon_social IS NOT NULL;
                
                RETURN result;
            END;
            $$ LANGUAGE plpgsql;
        ''')
        
        # 7. Get final statistics
        cursor.execute('SELECT COUNT(*) FROM ruc_cache')
        cache_total = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ruc_cache WHERE razon_social IS NOT NULL')
        cache_scraped = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ruc_cache WHERE razon_social IS NULL')
        cache_pending = cursor.fetchone()[0]
        
        print("")
        print("🎯 Optimized Database Setup Complete!")
        print("=" * 50)
        print(f"📊 RUC Cache Statistics:")
        print(f"   - Total RUCs to scrape: {cache_total:,}")
        print(f"   - Already scraped: {cache_scraped:,}")
        print(f"   - Pending scraping: {cache_pending:,}")
        print("")
        print("🔧 Created Components:")
        print("   ✅ ruc_cache table with indexes")
        print("   ✅ Lookup function: get_razon_social(ruc)")
        print("   ✅ Populated with first 13M unique RUCs")
        print("")
        print("📋 Next Steps:")
        print("   1. Run scraper on ruc_cache table")
        print("   2. Use lookup function for future records")
        print("   3. No need to scrape duplicates again!")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

if __name__ == "__main__":
    setup_optimized_database()