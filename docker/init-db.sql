-- ============================================================================
-- Financial Analysis Database - Initialization Script
-- This script runs automatically when PostgreSQL container starts
-- ============================================================================

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Financial News Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS financial_news_241118 (
    id BIGINT PRIMARY KEY,
    source VARCHAR(255),
    headline TEXT,
    href TEXT UNIQUE,
    summary TEXT,
    content TEXT,
    author VARCHAR(255),
    minsread VARCHAR(50),
    datetime TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_financial_news_datetime ON financial_news_241118(datetime);
CREATE INDEX IF NOT EXISTS idx_financial_news_source ON financial_news_241118(source);

-- ============================================================================
-- Historical Crypto Stocks Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS historical (
    id SERIAL PRIMARY KEY,
    reference VARCHAR(255),
    book VARCHAR(255),
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    adj_close FLOAT,
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(book, date)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_historical_book ON historical(book);
CREATE INDEX IF NOT EXISTS idx_historical_date ON historical(date);
CREATE INDEX IF NOT EXISTS idx_historical_book_date ON historical(book, date);

-- ============================================================================
-- Grant permissions
-- ============================================================================
-- Note: The default user already has full access to the database

-- ============================================================================
-- Confirmation message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Database initialization completed successfully!';
    RAISE NOTICE 'Tables created: financial_news_241118, historical';
END $$;

