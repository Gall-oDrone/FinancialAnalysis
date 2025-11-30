#!/usr/bin/env python3
"""
Test script to verify Docker container setup.

This script tests:
1. Chrome/Selenium functionality
2. Database connectivity
3. Basic scraping capabilities
"""

import os
import sys
import time

def test_environment():
    """Test environment variables are set."""
    print("\n" + "=" * 60)
    print("1. Testing Environment Variables")
    print("=" * 60)
    
    required_vars = [
        'PGDBHOST', 'PGDBPORT', 'PGDBNAME', 'PGDBUSER', 'PGDBPASS'
    ]
    
    all_set = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask password
            display_value = '***' if 'PASS' in var else value
            print(f"  ✓ {var}: {display_value}")
        else:
            print(f"  ✗ {var}: NOT SET")
            all_set = False
    
    return all_set


def test_imports():
    """Test required packages can be imported."""
    print("\n" + "=" * 60)
    print("2. Testing Package Imports")
    print("=" * 60)
    
    packages = [
        ('selenium', 'Selenium WebDriver'),
        ('pandas', 'Pandas DataFrame'),
        ('psycopg2', 'PostgreSQL Adapter'),
        ('dotenv', 'Environment Variables'),
        ('bs4', 'BeautifulSoup'),
    ]
    
    all_imported = True
    for package, name in packages:
        try:
            __import__(package)
            print(f"  ✓ {name} ({package})")
        except ImportError as e:
            print(f"  ✗ {name} ({package}): {e}")
            all_imported = False
    
    return all_imported


def test_database_connection():
    """Test database connectivity."""
    print("\n" + "=" * 60)
    print("3. Testing Database Connection")
    print("=" * 60)
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            dbname=os.getenv('PGDBNAME', 'financial_db'),
            user=os.getenv('PGDBUSER', 'financial_user'),
            password=os.getenv('PGDBPASS', ''),
            host=os.getenv('PGDBHOST', 'postgres'),
            port=os.getenv('PGDBPORT', '5432'),
            connect_timeout=10
        )
        
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"  ✓ Connected to PostgreSQL")
        print(f"    Version: {version[:60]}...")
        
        # Check tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        print(f"  ✓ Found {len(tables)} table(s):")
        for table in tables:
            print(f"    - {table[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ✗ Database connection failed: {e}")
        return False


def test_selenium_chrome():
    """Test Selenium with Chrome/Chromium."""
    print("\n" + "=" * 60)
    print("4. Testing Selenium + Chrome")
    print("=" * 60)
    
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        
        print("  → Configuring Chrome options...")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-extensions')
        
        # Use system chromium
        options.binary_location = '/usr/bin/chromium'
        
        print("  → Starting Chrome WebDriver...")
        service = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
        
        print("  ✓ Chrome WebDriver started successfully")
        
        # Test navigation
        print("  → Navigating to example.com...")
        driver.get('https://example.com')
        time.sleep(2)
        
        title = driver.title
        print(f"  ✓ Page loaded: {title}")
        
        # Test element finding
        h1 = driver.find_element(By.TAG_NAME, 'h1')
        print(f"  ✓ Found H1 element: {h1.text}")
        
        driver.quit()
        print("  ✓ Chrome WebDriver closed successfully")
        return True
        
    except Exception as e:
        print(f"  ✗ Selenium test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_yahoo_finance_access():
    """Test access to Yahoo Finance (target site)."""
    print("\n" + "=" * 60)
    print("5. Testing Yahoo Finance Access")
    print("=" * 60)
    
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        options.binary_location = '/usr/bin/chromium'
        
        print("  → Starting Chrome...")
        service = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        
        print("  → Navigating to Yahoo Finance...")
        driver.get('https://finance.yahoo.com/')
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )
        
        print(f"  ✓ Yahoo Finance loaded")
        print(f"    Title: {driver.title}")
        print(f"    URL: {driver.current_url}")
        
        driver.quit()
        return True
        
    except Exception as e:
        print(f"  ✗ Yahoo Finance access failed: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "#" * 60)
    print("# Financial Analysis - Docker Container Test")
    print("#" * 60)
    
    results = {
        'Environment': test_environment(),
        'Imports': test_imports(),
        'Database': test_database_connection(),
        'Selenium': test_selenium_chrome(),
        'Yahoo Finance': test_yahoo_finance_access(),
    }
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {test_name}: {status}")
        if not passed:
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("\n🎉 ALL TESTS PASSED! Container is ready for use.\n")
        sys.exit(0)
    else:
        print("\n⚠️  SOME TESTS FAILED. Check the output above.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()



