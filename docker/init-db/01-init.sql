-- =============================================================================
-- Financial Analysis - Database Initialization (single source of truth)
-- =============================================================================
-- Runs once when the postgres volume is first created.
-- Keep in sync with Storage/PostgresSQL_table_queries.py
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create the financial_news table
CREATE TABLE IF NOT EXISTS financial_news_241118 (
    id BIGINT PRIMARY KEY,
    source VARCHAR(255),
    headline TEXT,
    href TEXT,
    summary TEXT,
    content TEXT,
    author VARCHAR(255),
    minsread VARCHAR(50),
    datetime TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_financial_news_source ON financial_news_241118(source);
CREATE INDEX IF NOT EXISTS idx_financial_news_datetime ON financial_news_241118(datetime);
CREATE INDEX IF NOT EXISTS idx_financial_news_created_at ON financial_news_241118(created_at);

-- Create the historical stock prices table
CREATE TABLE IF NOT EXISTS historical (
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
    PRIMARY KEY (book, date)
);

-- Create indexes for historical table
CREATE INDEX IF NOT EXISTS idx_historical_book ON historical(book);
CREATE INDEX IF NOT EXISTS idx_historical_date ON historical(date);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO financial_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO financial_user;

-- Display success message
DO $$
BEGIN
    RAISE NOTICE 'Database initialization completed successfully!';
END $$;



