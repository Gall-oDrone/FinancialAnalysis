#!/usr/bin/env python3
"""Smoke test mirroring NewsCollector-Staging.ipynb setup (DB + Selenium)."""
import os
import sys

sys.path.insert(0, "/app/Storage")
sys.path.insert(0, "/app/WebScraping/src/selectors")

from dotenv import load_dotenv

load_dotenv("/app/.env")

import pgConn
import PostgresSQL_table_queries

print("=== Database ===")
pg_conn = pgConn.PgConn()
table_name = PostgresSQL_table_queries.FINANCIAL_NEWS_TABLE_NAME
table_query = PostgresSQL_table_queries.HISTORICAL_FINANCIAL_NEWS_TABLE_QUERY_241118
pg_conn.set_table(table_name)
pg_conn.init_db(table_query)
print("DB OK:", pg_conn.connection.get_dsn_parameters()["dbname"])

print("=== Selenium (container chromedriver) ===")
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.binary_location = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
service = Service(os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver"))
driver = webdriver.Chrome(service=service, options=options)
driver.get("https://finance.yahoo.com/markets/crypto/")
print("Yahoo Finance OK:", driver.title[:60])
driver.quit()

print("=== All smoke checks passed ===")
