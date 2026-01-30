"""
Tests for text transformation module.
"""

import pytest
import pandas as pd

from DataProcessing.text_transformers import (
    SentimentAnalyzer,
    SentimentLabel,
    IntentExtractor,
    FinancialIntent,
    KeywordExtractor,
    TextPreprocessor,
    TextTransformationPipeline,
    TransformedArticle
)


class TestSentimentAnalyzer:
    """Test sentiment analysis functionality."""
    
    def test_vader_positive_sentiment(self):
        """Test VADER sentiment on positive text."""
        analyzer = SentimentAnalyzer(backend="vader")
        
        text = "Bitcoin surged to new all-time high! Excellent performance and great news for investors."
        result = analyzer.transform(text)
        
        assert result.label == SentimentLabel.POSITIVE
        assert result.score > 0
        assert result.positive_score > 0
    
    def test_vader_negative_sentiment(self):
        """Test VADER sentiment on negative text."""
        analyzer = SentimentAnalyzer(backend="vader")
        
        text = "Bitcoin crashed 20% today. Terrible market conditions and panic selling."
        result = analyzer.transform(text)
        
        assert result.label == SentimentLabel.NEGATIVE
        assert result.score < 0
        assert result.negative_score > 0
    
    def test_vader_neutral_sentiment(self):
        """Test VADER sentiment on neutral text."""
        analyzer = SentimentAnalyzer(backend="vader")
        
        text = "Bitcoin traded at $50,000 today."
        result = analyzer.transform(text)
        
        assert result.label == SentimentLabel.NEUTRAL
        assert abs(result.score) < 0.05
    
    def test_textblob_backend(self):
        """Test TextBlob sentiment backend."""
        analyzer = SentimentAnalyzer(backend="textblob")
        
        text = "Great news for cryptocurrency investors!"
        result = analyzer.transform(text)
        
        assert result.label == SentimentLabel.POSITIVE
        assert isinstance(result.score, float)
    
    def test_empty_text(self):
        """Test handling of empty text."""
        analyzer = SentimentAnalyzer(backend="vader")
        
        result = analyzer.transform("")
        
        assert result.label == SentimentLabel.NEUTRAL
        assert result.score == 0.0
    
    def test_batch_analysis(self):
        """Test batch sentiment analysis."""
        analyzer = SentimentAnalyzer(backend="vader")
        
        texts = [
            "Excellent gains today!",
            "Market crashed badly.",
            "Bitcoin traded at $50k."
        ]
        
        results = analyzer.transform_batch(texts)
        
        assert len(results) == 3
        assert results[0].label == SentimentLabel.POSITIVE
        assert results[1].label == SentimentLabel.NEGATIVE
        assert results[2].label == SentimentLabel.NEUTRAL


class TestIntentExtractor:
    """Test intent extraction functionality."""
    
    def test_market_update_intent(self):
        """Test market update intent classification."""
        extractor = IntentExtractor(use_transformers=False)
        
        text = "Stock market rallied today with strong trading volume across all indices."
        result = extractor.transform(text)
        
        assert result.primary_intent == FinancialIntent.MARKET_UPDATE
        assert result.confidence > 0
    
    def test_price_prediction_intent(self):
        """Test price prediction intent."""
        extractor = IntentExtractor(use_transformers=False)
        
        text = "Analysts predict Bitcoin could reach $150,000 by year end. Price target upgraded."
        result = extractor.transform(text)
        
        assert result.primary_intent == FinancialIntent.PRICE_PREDICTION
    
    def test_regulatory_intent(self):
        """Test regulatory news intent."""
        extractor = IntentExtractor(use_transformers=False)
        
        text = "SEC announces new cryptocurrency regulations. Investigation into major exchange continues."
        result = extractor.transform(text)
        
        assert result.primary_intent == FinancialIntent.REGULATORY_NEWS
    
    def test_technology_update_intent(self):
        """Test technology update intent."""
        extractor = IntentExtractor(use_transformers=False)
        
        text = "Ethereum blockchain upgrade completed. New protocol features improve network efficiency."
        result = extractor.transform(text)
        
        assert result.primary_intent == FinancialIntent.TECHNOLOGY_UPDATE
    
    def test_empty_text_intent(self):
        """Test intent extraction on empty text."""
        extractor = IntentExtractor(use_transformers=False)
        
        result = extractor.transform("")
        
        assert result.primary_intent == FinancialIntent.GENERAL_INFO
        assert result.confidence == 0.0
    
    def test_secondary_intents(self):
        """Test that secondary intents are captured."""
        extractor = IntentExtractor(use_transformers=False)
        
        text = """
        Bitcoin market rallied after SEC approved new regulations. 
        Analysts predict further gains with strong trading volume.
        """
        result = extractor.transform(text)
        
        # Should have multiple intent matches
        assert len(result.secondary_intents) > 0


class TestKeywordExtractor:
    """Test keyword extraction functionality."""
    
    def test_tfidf_extraction(self):
        """Test TF-IDF keyword extraction."""
        extractor = KeywordExtractor(method="tfidf", top_n=5)
        
        text = "Bitcoin cryptocurrency blockchain technology trading market digital assets"
        result = extractor.transform(text)
        
        assert len(result.keywords) > 0
        assert len(result.keyword_scores) > 0
        assert all(isinstance(k, str) for k in result.keywords)
    
    def test_rake_extraction(self):
        """Test RAKE keyword extraction."""
        extractor = KeywordExtractor(method="rake", top_n=5)
        
        text = "Bitcoin surged past $100,000 as institutional adoption accelerated rapidly."
        result = extractor.transform(text)
        
        assert len(result.keywords) > 0
        assert 'bitcoin' in result.keywords or 'surged' in result.keywords
    
    def test_empty_text_keywords(self):
        """Test keyword extraction on empty text."""
        extractor = KeywordExtractor(method="tfidf")
        
        result = extractor.transform("")
        
        assert result.keywords == []
        assert result.keyword_scores == {}
    
    def test_top_n_limit(self):
        """Test that top_n parameter limits keywords."""
        extractor = KeywordExtractor(method="rake", top_n=3)
        
        text = "Bitcoin cryptocurrency blockchain technology trading market digital assets crypto fintech"
        result = extractor.transform(text)
        
        assert len(result.keywords) <= 3


class TestTextPreprocessor:
    """Test text preprocessing functionality."""
    
    def test_url_removal(self):
        """Test URL removal."""
        text = "Check out https://example.com and www.test.com for more info"
        cleaned = TextPreprocessor.clean_text(text)
        
        assert "https://example.com" not in cleaned
        assert "www.test.com" not in cleaned
    
    def test_email_removal(self):
        """Test email address removal."""
        text = "Contact us at [email protected]"
        cleaned = TextPreprocessor.clean_text(text)
        
        assert "info@example.com" not in cleaned
    
    def test_html_removal(self):
        """Test HTML tag removal."""
        text = "Bitcoin <strong>surged</strong> today"
        cleaned = TextPreprocessor.clean_text(text)
        
        assert "<strong>" not in cleaned
        assert "</strong>" not in cleaned
        assert "surged" in cleaned
    
    def test_whitespace_normalization(self):
        """Test whitespace normalization."""
        text = "Bitcoin   surged    today"
        cleaned = TextPreprocessor.clean_text(text)
        
        assert "  " not in cleaned
        assert cleaned == "Bitcoin surged today"
    
    def test_word_count(self):
        """Test word counting."""
        text = "Bitcoin surged past $100,000 today"
        count = TextPreprocessor.word_count(text)
        
        assert count == 5
    
    def test_empty_text_preprocessing(self):
        """Test preprocessing of empty text."""
        cleaned = TextPreprocessor.clean_text("")
        
        assert cleaned == ""
        assert TextPreprocessor.word_count(cleaned) == 0


class TestTextTransformationPipeline:
    """Test the complete transformation pipeline."""
    
    def test_transform_single_article(self):
        """Test transforming a single article."""
        pipeline = TextTransformationPipeline(
            sentiment_backend="vader",
            extract_tickers=True
        )
        
        article = {
            "id": 1,
            "source": "Yahoo Finance",
            "headline": "Bitcoin Surges",
            "content": "Bitcoin (CRYPTO: BTC) surged past $100,000 today!",
            "href": "https://example.com/article1",
            "datetime": "2026-01-27T10:00:00Z"
        }
        
        result = pipeline.transform_article(article)
        
        assert isinstance(result, TransformedArticle)
        assert result.id == 1
        assert result.sentiment is not None
        assert result.intent is not None
        assert result.keywords is not None
        assert 'BTC-USD' in result.tickers
        assert result.word_count > 0
    
    def test_transform_dataframe(self, sample_news_df):
        """Test transforming a DataFrame."""
        pipeline = TextTransformationPipeline(
            sentiment_backend="vader",
            extract_tickers=True
        )
        
        transformed = pipeline.transform(sample_news_df)
        
        assert isinstance(transformed, pd.DataFrame)
        assert len(transformed) == len(sample_news_df)
        assert 'sentiment_label' in transformed.columns
        assert 'primary_intent' in transformed.columns
        assert 'keywords' in transformed.columns
        assert 'tickers' in transformed.columns
    
    def test_get_ml_features(self):
        """Test ML feature extraction."""
        pipeline = TextTransformationPipeline(sentiment_backend="vader")
        
        articles = [
            {"id": 1, "headline": "Bitcoin rises", "content": "BTC up 10%"},
            {"id": 2, "headline": "ETH falls", "content": "ETH down 5%"}
        ]
        
        features = pipeline.get_ml_features(articles)
        
        assert isinstance(features, pd.DataFrame)
        assert 'word_count' in features.columns
        assert 'sentiment_score' in features.columns
        assert 'intent_confidence' in features.columns
    
    def test_pipeline_error_handling(self):
        """Test pipeline handles errors gracefully."""
        pipeline = TextTransformationPipeline()
        
        # Malformed article
        articles = [
            {"id": 1},  # Missing required fields
        ]
        
        # Should not raise exception
        result = pipeline.transform(articles)
        assert isinstance(result, pd.DataFrame)


class TestTransformedArticle:
    """Test TransformedArticle dataclass."""
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        from DataProcessing.text_transformers import SentimentResult, IntentResult, KeywordResult
        
        article = TransformedArticle(
            id=1,
            source="Test",
            headline="Test Article",
            href="https://test.com",
            sentiment=SentimentResult(
                label=SentimentLabel.POSITIVE,
                score=0.8,
                positive_score=0.8,
                negative_score=0.1,
                neutral_score=0.1
            ),
            tickers=['BTC-USD']
        )
        
        d = article.to_dict()
        
        assert d['id'] == 1
        assert d['sentiment_label'] == 'positive'
        assert d['sentiment_score'] == 0.8
        assert d['tickers'] == ['BTC-USD']
    
    def test_to_ml_features(self):
        """Test ML feature extraction."""
        from DataProcessing.text_transformers import SentimentResult, IntentResult
        
        article = TransformedArticle(
            id=1,
            source="Test",
            headline="Test",
            href="https://test.com",
            word_count=100,
            sentiment=SentimentResult(
                label=SentimentLabel.POSITIVE,
                score=0.8,
                positive_score=0.8,
                negative_score=0.1,
                neutral_score=0.1
            ),
            intent=IntentResult(
                primary_intent=FinancialIntent.MARKET_UPDATE,
                confidence=0.9
            )
        )
        
        features = article.to_ml_features()
        
        assert 'word_count' in features
        assert 'sentiment_score' in features
        assert 'intent_confidence' in features
        assert features['intent_market_update'] == 1.0
