import requests
from bs4 import BeautifulSoup
import csv
import json
import re
import sys
import time
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Path to cookies file (same directory as this script)
COOKIES_FILE = Path(__file__).resolve().parent / "cookies.json"

def convert_cookies(cookie_list):
    """Convert cookie list from JSON format to requests cookies dict"""
    cookies = {}
    for cookie in cookie_list:
        cookies[cookie['name']] = cookie['value']
    return cookies

def get_db_connection():
    """Get PostgreSQL database connection and credentials"""
    db_config = {
        'host': os.environ.get("SUPABASE_HOST", "").strip(),
        'database': os.environ.get("SUPABASE_DBNAME", "postgres").strip(),
        'user': os.environ.get("SUPABASE_USER", "").strip(),
        'password': os.environ.get("SUPABASE_PASSWORD", "").strip(),
        'port': os.environ.get("SUPABASE_PORT", "5432").strip(),
        'table': os.environ.get("SUPABASE_TABLE", "globe_daily_data").strip()
    }
    
    if not db_config['host'] or not db_config['user'] or not db_config['password']:
        return None, None
    
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
        )
        return conn, db_config['table']
    except Exception as e:
        print(f"❌ Error connecting to database: {str(e)}")
        return None, None

def create_table_if_not_exists():
    """Create the database table if it doesn't exist"""
    conn, table_name = get_db_connection()
    
    if not conn:
        print("⚠️  PostgreSQL credentials not found. Skipping table creation.")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            url TEXT,
            product_name TEXT,
            product_code TEXT,
            sku TEXT,
            price TEXT,
            availability TEXT,
            product_quantity TEXT,
            description TEXT,
            status TEXT,
            scraped_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        
        print(f"✅ Table '{table_name}' is ready")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating table: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def clear_database():
    """Clear all data from the database table before scraping"""
    conn, table_name = get_db_connection()
    
    if not conn:
        print("⚠️  PostgreSQL credentials not found. Skipping database clear.")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Delete all records from the table
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()
        
        deleted_count = cursor.rowcount
        print(f"🗑️  Cleared {deleted_count} existing records from database")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error clearing database: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def insert_product_to_db(product_data):
    """Insert a single product into the database immediately after scraping"""
    conn, table_name = get_db_connection()
    
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Add timestamps
        product_data['scraped_at'] = datetime.now()
        product_data['created_at'] = datetime.now()
        
        # Convert empty strings to None
        for key, value in product_data.items():
            if value == '':
                product_data[key] = None
        
        # Prepare columns and values
        columns = list(product_data.keys())
        values = [product_data[col] for col in columns]
        
        # Create INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
        """
        
        # Execute insert
        cursor.execute(insert_query, values)
        conn.commit()
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ⚠️  Error inserting to database: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def scrape_product(url, session):
    """Scrape product information from a single product page"""
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'lxml')
        
        product_data = {
            'url': url,
            'product_name': '',
            'product_code': '',
            'price': '',
            'product_quantity': '',
            'availability': '',
            'description': '',
            'sku': '',
            'status': 'success'
        }
        
        # Extract product name
        name_tag = soup.find('h1', class_='page-title')
        if not name_tag:
            name_tag = soup.find('h1', {'class': re.compile(r'product.*name', re.I)})
        if name_tag:
            product_data['product_name'] = name_tag.get_text(strip=True)
        
        # Extract SKU/Product Code
        sku_tag = soup.find('div', {'class': 'product-info-stock-sku'})
        if not sku_tag:
            sku_tag = soup.find('div', {'class': re.compile(r'sku', re.I)})
        if sku_tag:
            sku_text = sku_tag.get_text(strip=True)
            sku_match = re.search(r'SKU[:\s]*([A-Za-z0-9-]+)', sku_text, re.I)
            if sku_match:
                product_data['sku'] = sku_match.group(1)
                product_data['product_code'] = sku_match.group(1)
        
        # Extract price
        price_tag = soup.find('span', {'class': 'price'})
        if not price_tag:
            price_tag = soup.find('span', {'class': re.compile(r'price', re.I)})
        if price_tag:
            price_text = price_tag.get_text(strip=True)
            product_data['price'] = price_text
        
        # Extract availability/stock status
        stock_tag = soup.find('div', {'class': re.compile(r'stock|availability', re.I)})
        if stock_tag:
            product_data['availability'] = stock_tag.get_text(strip=True)
        
        # Extract quantity if available
        qty_tag = soup.find('input', {'id': 'qty'})
        if not qty_tag:
            qty_tag = soup.find('input', {'name': 'qty'})
        if qty_tag:
            product_data['product_quantity'] = qty_tag.get('value', '')
        
        # Extract description
        desc_tag = soup.find('div', {'class': re.compile(r'product.*description', re.I)})
        if not desc_tag:
            desc_tag = soup.find('div', {'itemprop': 'description'})
        if desc_tag:
            # Get first 200 characters of description
            desc_text = desc_tag.get_text(strip=True)
            product_data['description'] = desc_text[:200] + '...' if len(desc_text) > 200 else desc_text
        
        print(f"✓ Scraped: {product_data['product_name'][:50]}...")
        return product_data
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error scraping {url}: {str(e)}")
        return {
            'url': url,
            'product_name': '',
            'product_code': '',
            'price': '',
            'product_quantity': '',
            'availability': '',
            'description': '',
            'sku': '',
            'status': f'error: {str(e)}'
        }

def main():
    # Clear database before starting
    print("=" * 60)
    print("🚀 Starting Globe Pest Solutions Scraper")
    print("=" * 60)
    print()
    
    # Create table if it doesn't exist
    create_table_if_not_exists()
    
    # Clear database
    clear_database()
    print()
    
    # Load cookies from file
    if not COOKIES_FILE.exists():
        print(f"❌ Error: cookies file not found: {COOKIES_FILE}")
        print("   Create cookies.json in the same directory as main.py (e.g. copy from globe_cookies.json).")
        sys.exit(1)
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        cookies_list = data if isinstance(data, list) else data.get("cookies", data)
        if not cookies_list or not isinstance(cookies_list, list):
            print("❌ Error: cookies.json must contain a JSON array of cookie objects (with 'name' and 'value').")
            sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error: invalid JSON in {COOKIES_FILE}: {e}")
        sys.exit(1)
    cookies = convert_cookies(cookies_list)
    
    # Create session with cookies and headers
    session = requests.Session()
    session.cookies.update(cookies)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    })
    
    # Read input CSV
    input_csv = 'attached_assets/globe_sku_rows_1760609515009.csv'
    output_csv = f'scraped_data/scraped_products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    products_data = []
    db_insert_count = 0
    
    print(f"📄 Reading product links from: {input_csv}")
    
    with open(input_csv, 'r', encoding='utf-8') as f:
        csv_reader = csv.DictReader(f)
        product_links = [row['product_link'] for row in csv_reader]
    
    print(f"📊 Found {len(product_links)} products to scrape\n")
    
    # Scrape each product
    for i, url in enumerate(product_links, 1):
        print(f"[{i}/{len(product_links)}] Scraping: {url}")
        product_data = scrape_product(url, session)
        products_data.append(product_data)
        
        # Insert into database immediately after scraping
        if product_data['status'] == 'success':
            if insert_product_to_db(product_data):
                db_insert_count += 1
                print(f"   💾 Inserted to database ({db_insert_count} total)")
        
        # Be polite to the server - add a small delay between requests
        time.sleep(1)
    
    # Write results to CSV (backup)
    print()
    if products_data:
        fieldnames = ['url', 'product_name', 'product_code', 'sku', 'price', 
                     'availability', 'product_quantity', 'description', 'status']
        
        with open(output_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products_data)
        
        print(f"✓ Successfully scraped {len(products_data)} products")
        print(f"✓ CSV backup saved to: {output_csv}")
        
        # Print summary
        success_count = sum(1 for p in products_data if p['status'] == 'success')
        error_count = len(products_data) - success_count
        print(f"\n📈 Summary:")
        print(f"  - Successful scrapes: {success_count}")
        print(f"  - Errors: {error_count}")
        print(f"  - Inserted to database: {db_insert_count}")
        
        print()
        print("=" * 60)
        print("✅ Scraping completed successfully!")
        print("=" * 60)
        
        return output_csv
    else:
        print("❌ No products were scraped!")
        return None

if __name__ == "__main__":
    main()
