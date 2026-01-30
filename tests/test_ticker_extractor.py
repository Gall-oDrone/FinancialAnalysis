"""
Tests for ticker extraction module.
"""

import pytest
import pandas as pd

from DataProcessing.ticker_extractor import (
    TickerExtractor,
    TickerExtractionResult,
    extract_tickers_from_dataframe,
    extract_tickers_from_article_df,
    CRYPTO_NAME_TO_TICKER
)


class TestTickerExtractor:
    """Test TickerExtractor class."""
    
    def test_extract_standard_format(self):
        """Test extraction of standard ticker format (XXX-YYY)."""
        extractor = TickerExtractor()
        
        text = "BTC-USD rallied 10% while ETH-USD gained 5%."
        result = extractor.extract_from_text(text)
        
        assert 'BTC-USD' in result.tickers
        assert 'ETH-USD' in result.tickers
        assert result.confidence > 0.8
        assert 'pattern' in result.extraction_method
    
    def test_extract_parenthetical_format(self):
        """Test extraction of parenthetical format (CRYPTO: XXX)."""
        extractor = TickerExtractor()
        
        text = "Bitcoin (CRYPTO: BTC) surged past $100,000."
        result = extractor.extract_from_text(text)
        
        assert 'BTC-USD' in result.tickers
        assert result.confidence > 0.8
    
    def test_extract_inline_format(self):
        """Test extraction of inline format ($XXX)."""
        extractor = TickerExtractor()
        
        text = "Trading volume increased for $BTC and $ETH today."
        result = extractor.extract_from_text(text)
        
        assert 'BTC-USD' in result.tickers
        assert 'ETH-USD' in result.tickers
    
    def test_extract_by_name(self):
        """Test extraction by cryptocurrency name."""
        extractor = TickerExtractor()
        
        text = "Bitcoin and Ethereum both rallied today."
        result = extractor.extract_from_text(text)
        
        assert 'BTC-USD' in result.tickers
        assert 'ETH-USD' in result.tickers
        assert 'name_mapping' in result.extraction_method
    
    def test_extract_multiple_formats(self):
        """Test extraction with multiple formats in same text."""
        extractor = TickerExtractor()
        
        text = """
        Bitcoin (CRYPTO: BTC) and Ethereum continued their rally. 
        BTC-USD hit $100k while ETH-USD reached $5k. 
        Analysts are bullish on both bitcoin and ethereum.
        """
        result = extractor.extract_from_text(text)
        
        assert 'BTC-USD' in result.tickers
        assert 'ETH-USD' in result.tickers
        assert result.confidence >= 0.9  # High confidence with multiple methods
        assert 'pattern' in result.extraction_method
        assert 'name_mapping' in result.extraction_method
    
    def test_empty_text(self):
        """Test handling of empty text."""
        extractor = TickerExtractor()
        
        result = extractor.extract_from_text("")
        
        assert result.tickers == []
        assert result.confidence == 0.0
        assert result.extraction_method == 'none'
    
    def test_no_tickers_found(self):
        """Test when no tickers are found."""
        extractor = TickerExtractor()
        
        text = "The stock market was quiet today."
        result = extractor.extract_from_text(text)
        
        assert result.tickers == []
    
    def test_extract_from_article(self):
        """Test extraction from article dictionary."""
        extractor = TickerExtractor()
        
        article = {
            "headline": "Bitcoin Hits New High",
            "summary": "BTC-USD surged past $100,000.",
            "content": "Ethereum (CRYPTO: ETH) also gained significantly..."
        }
        
        tickers = extractor.extract_from_article(article)
        
        assert 'BTC-USD' in tickers
        assert 'ETH-USD' in tickers
    
    def test_ticker_deduplication(self):
        """Test that duplicate tickers are removed."""
        extractor = TickerExtractor()
        
        text = "BTC-USD, BTC-USD, and Bitcoin all refer to the same asset."
        result = extractor.extract_from_text(text)
        
        # Should only have one BTC-USD
        assert result.tickers.count('BTC-USD') == 1
    
    def test_case_insensitive_names(self):
        """Test case-insensitive name matching."""
        extractor = TickerExtractor()
        
        text = "BITCOIN and Ethereum both rallied today."
        result = extractor.extract_from_text(text)
        
        assert 'BTC-USD' in result.tickers
        assert 'ETH-USD' in result.tickers
    
    def test_batch_extraction(self):
        """Test batch extraction from multiple texts."""
        extractor = TickerExtractor()
        
        texts = [
            "Bitcoin surged today",
            "ETH-USD hit new highs",
            "No crypto here"
        ]
        
        results = extractor.extract_batch(texts)
        
        assert len(results) == 3
        assert 'BTC-USD' in results[0].tickers
        assert 'ETH-USD' in results[1].tickers
        assert len(results[2].tickers) == 0


class TestTickerMapping:
    """Test ticker name mapping."""
    
    def test_major_crypto_mapping(self):
        """Test major cryptocurrency mappings."""
        assert CRYPTO_NAME_TO_TICKER['bitcoin'] == 'BTC-USD'
        assert CRYPTO_NAME_TO_TICKER['ethereum'] == 'ETH-USD'
        assert CRYPTO_NAME_TO_TICKER['solana'] == 'SOL-USD'
        assert CRYPTO_NAME_TO_TICKER['cardano'] == 'ADA-USD'
    
    def test_abbreviation_mapping(self):
        """Test abbreviation to ticker mapping."""
        assert CRYPTO_NAME_TO_TICKER['btc'] == 'BTC-USD'
        assert CRYPTO_NAME_TO_TICKER['eth'] == 'ETH-USD'
        assert CRYPTO_NAME_TO_TICKER['sol'] == 'SOL-USD'


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_extract_tickers_from_dataframe(self):
        """Test DataFrame ticker extraction."""
        df = pd.DataFrame({
            'content': [
                "Bitcoin (CRYPTO: BTC) rallied",
                "ETH-USD hit new highs",
                "Market update without tickers"
            ]
        })
        
        result = extract_tickers_from_dataframe(df, 'content')
        
        assert 'tickers' in result.columns
        assert 'BTC-USD' in result.iloc[0]['tickers']
        assert 'ETH-USD' in result.iloc[1]['tickers']
        assert len(result.iloc[2]['tickers']) == 0
    
    def test_extract_tickers_from_article_df(self):
        """Test extraction from article DataFrame with multiple columns."""
        df = pd.DataFrame([
            {
                'headline': 'Bitcoin Surges',
                'summary': 'BTC-USD hits $100k',
                'content': 'Ethereum (CRYPTO: ETH) also gains'
            }
        ])
        
        result = extract_tickers_from_article_df(df)
        
        assert 'tickers' in result.columns
        tickers = result.iloc[0]['tickers']
        assert 'BTC-USD' in tickers
        assert 'ETH-USD' in tickers


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_mixed_case_tickers(self):
        """Test that ticker patterns are case-sensitive."""
        extractor = TickerExtractor()
        
        # Lowercase ticker symbols should not match
        text = "btc-usd rallied today"
        result = extractor.extract_from_text(text)
        
        # Should not extract lowercase tickers
        assert 'btc-usd' not in result.tickers
    
    def test_partial_word_matching(self):
        """Test that partial words don't cause false positives."""
        extractor = TickerExtractor()
        
        text = "The bitcoin network is secure."
        result = extractor.extract_from_text(text)
        
        # Should extract bitcoin as a name
        assert 'BTC-USD' in result.tickers
    
    def test_long_text_performance(self):
        """Test extraction on long text (truncation)."""
        extractor = TickerExtractor()
        
        # Create long text with ticker at the end
        long_text = "Lorem ipsum " * 1000 + "Bitcoin (CRYPTO: BTC) surged"
        result = extractor.extract_from_text(long_text)
        
        # Should still extract even from long text
        assert 'BTC-USD' in result.tickers
    
    def test_special_characters(self):
        """Test handling of special characters around tickers."""
        extractor = TickerExtractor()
        
        text = "Check out BTC-USD! And ETH-USD?"
        result = extractor.extract_from_text(text)
        
        assert 'BTC-USD' in result.tickers
        assert 'ETH-USD' in result.tickers
