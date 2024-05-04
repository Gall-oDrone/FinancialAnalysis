'''
IMPORTANT! Manually change the Table name
'''

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