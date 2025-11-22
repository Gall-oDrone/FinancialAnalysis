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
                volume BIGINT
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
                id VARCHAR(255),
                source VARCHAR(255),
                headline TEXT,
                href TEXT,
                summary TEXT,
                content TEXT,
                author VARCHAR(255),
                minsread VARCHAR(255),
                datetime VARCHAR(255)            )
        """