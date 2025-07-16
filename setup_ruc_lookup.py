#!/usr/bin/env python3
"""
Setup RUC lookup database with unique RUCs from main database
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def setup_ruc_lookup():
    """Setup RUC lookup database"""
    
    print("🚀 Setting up RUC lookup database...")
    
    # Main database config
    main_db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    # RUC lookup database config
    ruc_db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': 'ruc_lookup',
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    try:
        # Connect to RUC lookup database
        print("📡 Connecting to ruc_lookup database...")
        ruc_conn = psycopg2.connect(**ruc_db_config)
        ruc_cursor = ruc_conn.cursor()
        
        # Create RUC lookup table
        print("📋 Creating ruc_lookup table...")
        ruc_cursor.execute('''
            CREATE TABLE IF NOT EXISTS ruc_lookup (
                id SERIAL PRIMARY KEY,
                ruc VARCHAR(11) UNIQUE NOT NULL,
                razon_social TEXT,
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        
        # Create indexes
        print("⚡ Creating indexes...")
        ruc_cursor.execute('CREATE INDEX IF NOT EXISTS idx_ruc_lookup_ruc ON ruc_lookup(ruc);')
        ruc_cursor.execute('CREATE INDEX IF NOT EXISTS idx_ruc_lookup_scraped ON ruc_lookup(scraped_at);')
        
        ruc_conn.commit()
        
        # Check if table is empty
        ruc_cursor.execute('SELECT COUNT(*) FROM ruc_lookup')
        existing_count = ruc_cursor.fetchone()[0]
        
        if existing_count == 0:
            print("📥 Populating RUC lookup table with unique RUCs...")
            
            # Connect to main database
            print("📡 Connecting to main database...")
            main_conn = psycopg2.connect(**main_db_config)
            main_cursor = main_conn.cursor()
            
            # Get unique RUCs from first 13M records
            print("🔍 Extracting unique RUCs from first 13M records...")
            main_cursor.execute('''
                WITH numbered_rows AS (
                    SELECT ruc, ROW_NUMBER() OVER (ORDER BY id) as rn
                    FROM sunat_empresas
                )
                SELECT DISTINCT ruc 
                FROM numbered_rows 
                WHERE rn <= 13000000
                ORDER BY ruc
            ''')
            
            unique_rucs = main_cursor.fetchall()
            print(f"✅ Found {len(unique_rucs):,} unique RUCs from first 13M records")
            
            # Insert in batches
            batch_size = 10000
            inserted = 0
            
            print("📥 Inserting RUCs into lookup table...")
            for i in range(0, len(unique_rucs), batch_size):
                batch = unique_rucs[i:i + batch_size]
                
                # Prepare batch insert
                values = ','.join([f"('{ruc[0]}')" for ruc in batch])
                ruc_cursor.execute(f'''
                    INSERT INTO ruc_lookup (ruc) 
                    VALUES {values}
                    ON CONFLICT (ruc) DO NOTHING
                ''')
                
                inserted += ruc_cursor.rowcount
                ruc_conn.commit()
                
                if (i + batch_size) % 100000 == 0 or i + batch_size >= len(unique_rucs):
                    print(f"   📥 Inserted {min(i + batch_size, len(unique_rucs)):,} / {len(unique_rucs):,} RUCs...")
            
            print(f"✅ Inserted {inserted:,} unique RUCs")
            
            main_cursor.close()
            main_conn.close()
        else:
            print(f"ℹ️ RUC lookup table already has {existing_count:,} records")
        
        # Get final statistics
        ruc_cursor.execute('SELECT COUNT(*) FROM ruc_lookup')
        total_rucs = ruc_cursor.fetchone()[0]
        
        ruc_cursor.execute('SELECT COUNT(*) FROM ruc_lookup WHERE razon_social IS NOT NULL')
        scraped_rucs = ruc_cursor.fetchone()[0]
        
        pending_rucs = total_rucs - scraped_rucs
        
        # Update .env file
        print("⚙️ Updating .env file...")
        env_path = '/root/sunatscraper/.env'
        
        # Read current .env
        with open(env_path, 'r') as f:
            env_content = f.read()
        
        # Add RUC database config if not exists
        if 'RUC_DB_HOST' not in env_content:
            with open(env_path, 'a') as f:
                f.write(f'''

# RUC Lookup Database
RUC_DB_HOST={ruc_db_config["host"]}
RUC_DB_PORT={ruc_db_config["port"]}
RUC_DB_NAME={ruc_db_config["database"]}
RUC_DB_USER={ruc_db_config["user"]}
RUC_DB_PASSWORD={ruc_db_config["password"]}
''')
        
        ruc_cursor.close()
        ruc_conn.close()
        
        print("")
        print("🎯 RUC Lookup Database Setup Complete!")
        print("=" * 50)
        print(f"📊 Statistics:")
        print(f"   - Database: ruc_lookup")
        print(f"   - Total unique RUCs: {total_rucs:,}")
        print(f"   - Already scraped: {scraped_rucs:,}")
        print(f"   - Pending scraping: {pending_rucs:,}")
        print("")
        print("🔧 Database Connection:")
        print(f"   - Host: {ruc_db_config['host']}")
        print(f"   - Database: ruc_lookup")
        print(f"   - Table: ruc_lookup")
        print(f"   - User: {ruc_db_config['user']}")
        print("")
        print("📋 Next Steps:")
        print("   1. Run scraper on ruc_lookup database")
        print("   2. Use this as master reference for all lookups")
        print("   3. Main database can lookup razon_social from here")
        
        return True
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False

if __name__ == "__main__":
    setup_ruc_lookup()