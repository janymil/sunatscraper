#!/usr/bin/env python3
"""
Create a separate database for RUC lookup with only unique RUCs
This will be our master reference for all scraped company names
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def create_ruc_database():
    """Create separate database for RUC lookup"""
    
    print("🚀 Creating separate RUC lookup database...")
    
    # Database configurations - using root access
    main_db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': 'postgres',  # Connect to default postgres database first
        'user': os.getenv('DB_ROOT_USER', 'root'),
        'password': os.getenv('DB_ROOT_PASSWORD')
    }
    
    # Configuration for main sunat database
    sunat_db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_ROOT_USER', 'root'),
        'password': os.getenv('DB_ROOT_PASSWORD')
    }
    
    # Connect to main database first to get admin connection
    try:
        print("📡 Connecting to main database...")
        main_conn = psycopg2.connect(**main_db_config)
        main_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        main_cursor = main_conn.cursor()
        
        # Create new database for RUC lookup
        ruc_db_name = 'ruc_lookup'
        
        print(f"🗄️ Creating database: {ruc_db_name}")
        try:
            main_cursor.execute(f'CREATE DATABASE {ruc_db_name}')
            print(f"✅ Database {ruc_db_name} created successfully")
        except psycopg2.errors.DuplicateDatabase:
            print(f"ℹ️ Database {ruc_db_name} already exists")
        
        main_cursor.close()
        main_conn.close()
        
        # Connect to new RUC database
        ruc_db_config = main_db_config.copy()
        ruc_db_config['database'] = ruc_db_name
        
        print(f"📡 Connecting to {ruc_db_name} database...")
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
            
            # Connect back to sunat database to get unique RUCs
            main_conn = psycopg2.connect(**sunat_db_config)
            main_cursor = main_conn.cursor()
            
            # Get first 13M unique RUCs
            print("🔍 Extracting unique RUCs from first 13M records...")
            main_cursor.execute('''
                SELECT DISTINCT ruc 
                FROM sunat_empresas 
                WHERE id <= (
                    SELECT id FROM sunat_empresas ORDER BY id LIMIT 1 OFFSET 12999999
                )
                ORDER BY ruc
            ''')
            
            unique_rucs = main_cursor.fetchall()
            print(f"✅ Found {len(unique_rucs):,} unique RUCs")
            
            # Insert in batches
            batch_size = 10000
            inserted = 0
            
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
                
                if (i + batch_size) % 100000 == 0:
                    print(f"   📥 Inserted {i + batch_size:,} RUCs...")
            
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
        
        # Update .env file with new database info
        env_path = '/root/sunatscraper/.env'
        with open(env_path, 'a') as f:
            f.write(f'\n# RUC Lookup Database\n')
            f.write(f'RUC_DB_HOST={ruc_db_config["host"]}\n')
            f.write(f'RUC_DB_PORT={ruc_db_config["port"]}\n')
            f.write(f'RUC_DB_NAME={ruc_db_config["database"]}\n')
            f.write(f'RUC_DB_USER={ruc_db_config["user"]}\n')
            f.write(f'RUC_DB_PASSWORD={ruc_db_config["password"]}\n')
        
        ruc_cursor.close()
        ruc_conn.close()
        
        print("")
        print("🎯 RUC Lookup Database Created Successfully!")
        print("=" * 50)
        print(f"📊 Statistics:")
        print(f"   - Database: {ruc_db_name}")
        print(f"   - Total unique RUCs: {total_rucs:,}")
        print(f"   - Already scraped: {scraped_rucs:,}")
        print(f"   - Pending scraping: {pending_rucs:,}")
        print("")
        print("🔧 Database Connection:")
        print(f"   - Host: {ruc_db_config['host']}")
        print(f"   - Database: {ruc_db_name}")
        print(f"   - Table: ruc_lookup")
        print("")
        print("📋 Next Steps:")
        print("   1. Run scraper on ruc_lookup database")
        print("   2. Use this as master reference for all lookups")
        print("   3. Main database can lookup razon_social from here")
        
        return True
        
    except Exception as e:
        print(f"❌ Database creation failed: {e}")
        return False

if __name__ == "__main__":
    create_ruc_database()