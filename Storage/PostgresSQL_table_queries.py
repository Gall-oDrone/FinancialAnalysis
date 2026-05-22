'''
IMPORTANT! Manually change the Table name
'''
FINANCIAL_NEWS_TABLE_NAME = "financial_news_241118"
HISTORICAL_CRYPTO_STOCKS_TABLE_NAME = "historical"

HISTORICAL_CRYPTO_STOCKS_TABLE_QUERY = """
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
                UNIQUE(book, date)
            )
        """
HISTORICAL_FINANCIAL_NEWS_TABLE_QUERY = """
            CREATE TABLE IF NOT EXISTS financial_news (
                id VARCHAR(255),
                source VARCHAR(255),
                category VARCHAR(255),
                headline TEXT,
                href TEXT,
                summary TEXT,
                content TEXT,
                datetime VARCHAR(255)            )
        """
HISTORICAL_FINANCIAL_NEWS_TABLE_QUERY_241118 = """
            CREATE TABLE IF NOT EXISTS financial_news_241118 (
                id BIGINT PRIMARY KEY,
                source VARCHAR(255),
                headline TEXT,
                href TEXT,
                summary TEXT,
                content TEXT,
                author VARCHAR(255),
                minsread VARCHAR(255),
                datetime VARCHAR(255)            )
        """

# Table for ML-transformed news data
FINANCIAL_NEWS_TRANSFORMED_TABLE_NAME = "financial_news_transformed"

FINANCIAL_NEWS_TRANSFORMED_TABLE_QUERY = """
            CREATE TABLE IF NOT EXISTS financial_news_transformed (
                id VARCHAR(255) PRIMARY KEY,
                source VARCHAR(255),
                headline TEXT,
                href TEXT,
                summary TEXT,
                content TEXT,
                datetime VARCHAR(255),
                topic VARCHAR(100),
                scraped_at TIMESTAMP,
                
                -- Text processing fields
                cleaned_text TEXT,
                word_count INTEGER,
                
                -- Ticker extraction
                tickers JSONB,
                
                -- Sentiment analysis fields
                sentiment_label VARCHAR(50),
                sentiment_score FLOAT,
                positive_score FLOAT,
                negative_score FLOAT,
                neutral_score FLOAT,
                
                -- Intent extraction fields
                primary_intent VARCHAR(100),
                intent_confidence FLOAT,
                secondary_intents JSONB,
                
                -- Keyword extraction fields
                keywords JSONB,
                entities JSONB,
                
                -- Agentic (LLM) enrichment (optional)
                llm_summary TEXT,
                llm_themes JSONB,
                llm_entities JSONB,
                llm_error TEXT,
                agentic_enabled BOOLEAN NOT NULL DEFAULT false,
                
                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

# Index for faster queries on transformed table
FINANCIAL_NEWS_TRANSFORMED_INDEXES = """
            CREATE INDEX IF NOT EXISTS idx_transformed_sentiment ON financial_news_transformed(sentiment_label);
            CREATE INDEX IF NOT EXISTS idx_transformed_intent ON financial_news_transformed(primary_intent);
            CREATE INDEX IF NOT EXISTS idx_transformed_topic ON financial_news_transformed(topic);
            CREATE INDEX IF NOT EXISTS idx_transformed_datetime ON financial_news_transformed(datetime);
            CREATE INDEX IF NOT EXISTS idx_transformed_tickers ON financial_news_transformed USING GIN(tickers);
            CREATE INDEX IF NOT EXISTS idx_transformed_agentic ON financial_news_transformed(agentic_enabled);
        """

# Table for processed/transformed stock data
HISTORICAL_PROCESSED_TABLE_NAME = "historical_processed"

HISTORICAL_PROCESSED_TABLE_QUERY = """
            CREATE TABLE IF NOT EXISTS historical_processed (
                -- Original fields
                reference VARCHAR(255),
                book VARCHAR(255),
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                adj_close FLOAT,
                volume BIGINT,
                
                -- Returns
                simple_return FLOAT,
                log_return FLOAT,
                
                -- Volatility metrics
                volatility_20d FLOAT,
                volatility_60d FLOAT,
                volatility_parkinson FLOAT,
                volatility_gk FLOAT,
                
                -- Moving averages
                sma_20 FLOAT,
                sma_50 FLOAT,
                sma_200 FLOAT,
                ema_12 FLOAT,
                ema_26 FLOAT,
                
                -- Technical indicators
                rsi_14 FLOAT,
                macd FLOAT,
                macd_signal FLOAT,
                macd_histogram FLOAT,
                bb_upper FLOAT,
                bb_middle FLOAT,
                bb_lower FLOAT,
                
                -- Metadata
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Constraints
                PRIMARY KEY (book, date)
            )
        """

# Indexes for processed stocks
HISTORICAL_PROCESSED_INDEXES = """
            CREATE INDEX IF NOT EXISTS idx_processed_book ON historical_processed(book);
            CREATE INDEX IF NOT EXISTS idx_processed_date ON historical_processed(date);
            CREATE INDEX IF NOT EXISTS idx_processed_book_date ON historical_processed(book, date);
        """