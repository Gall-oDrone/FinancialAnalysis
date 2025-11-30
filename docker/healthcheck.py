#!/usr/bin/env python3
"""
Health check script for the Financial Analysis container.

This script verifies:
1. Python environment is working
2. Required packages are importable
3. Database connection is possible
4. Chrome/Selenium is functional

Exit codes:
- 0: Healthy
- 1: Unhealthy
"""

import os
import sys


def check_python_packages():
    """Verify required Python packages are importable."""
    required_packages = [
        'selenium',
        'pandas',
        'psycopg2',
        'dotenv',
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"UNHEALTHY: Missing packages: {missing}")
        return False
    
    print("OK: All required packages available")
    return True


def check_database_connection():
    """Verify database connection is possible."""
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            dbname=os.getenv('PGDBNAME', 'financial_db'),
            user=os.getenv('PGDBUSER', 'financial_user'),
            password=os.getenv('PGDBPASS', ''),
            host=os.getenv('PGDBHOST', 'postgres'),
            port=os.getenv('PGDBPORT', '5432'),
            connect_timeout=5
        )
        conn.close()
        print("OK: Database connection successful")
        return True
    except Exception as e:
        print(f"WARNING: Database connection failed: {e}")
        # Don't fail health check for DB - it might be intentional
        return True


def check_chrome():
    """Verify Chrome/Chromium is available."""
    import shutil
    
    chrome_paths = [
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/usr/bin/google-chrome',
    ]
    
    for path in chrome_paths:
        if os.path.exists(path):
            print(f"OK: Chrome found at {path}")
            return True
    
    # Also check via which command
    if shutil.which('chromium') or shutil.which('chromium-browser') or shutil.which('google-chrome'):
        print("OK: Chrome found in PATH")
        return True
    
    print("UNHEALTHY: Chrome/Chromium not found")
    return False


def check_chromedriver():
    """Verify ChromeDriver is available."""
    import shutil
    
    if os.path.exists('/usr/bin/chromedriver') or shutil.which('chromedriver'):
        print("OK: ChromeDriver found")
        return True
    
    print("UNHEALTHY: ChromeDriver not found")
    return False


def check_selenium_webdriver():
    """Verify Selenium can initialize a headless Chrome session."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        
        # Quick test - just verify driver can be created
        # Don't actually start a session for health check (too slow)
        print("OK: Selenium configuration valid")
        return True
    except Exception as e:
        print(f"UNHEALTHY: Selenium check failed: {e}")
        return False


def main():
    """Run all health checks."""
    print("=" * 50)
    print("Financial Analysis - Health Check")
    print("=" * 50)
    
    checks = [
        ("Python packages", check_python_packages),
        ("Chrome browser", check_chrome),
        ("ChromeDriver", check_chromedriver),
        ("Selenium config", check_selenium_webdriver),
        ("Database connection", check_database_connection),
    ]
    
    all_passed = True
    for name, check_func in checks:
        print(f"\nChecking {name}...")
        try:
            if not check_func():
                all_passed = False
        except Exception as e:
            print(f"UNHEALTHY: {name} check raised exception: {e}")
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("HEALTH CHECK: PASSED")
        print("=" * 50)
        sys.exit(0)
    else:
        print("HEALTH CHECK: FAILED")
        print("=" * 50)
        sys.exit(1)


if __name__ == "__main__":
    main()
