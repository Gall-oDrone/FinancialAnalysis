"""
Text Transformation Module for ML/AI Data Preparation

This module provides text processing capabilities including:
- Sentiment Analysis (using VADER, TextBlob, or Transformers)
- Intent Extraction (classification-based)
- Keyword Extraction (TF-IDF, RAKE, or spaCy-based)

Usage:
    from DataProcessing.text_transformers import TextTransformationPipeline
    
    pipeline = TextTransformationPipeline()
    results = pipeline.transform(articles_dataframe)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any, Union
from enum import Enum
import re

import pandas as pd
import numpy as np

from utils.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Data Models
# ============================================================================

class SentimentLabel(Enum):
    """Sentiment classification labels."""
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class FinancialIntent(Enum):
    """Financial news intent categories."""
    MARKET_UPDATE = "market_update"
    PRICE_PREDICTION = "price_prediction"
    REGULATORY_NEWS = "regulatory_news"
    COMPANY_NEWS = "company_news"
    TECHNOLOGY_UPDATE = "technology_update"
    ANALYSIS_OPINION = "analysis_opinion"
    BREAKING_NEWS = "breaking_news"
    GENERAL_INFO = "general_info"


@dataclass
class SentimentResult:
    """Result of sentiment analysis."""
    label: SentimentLabel
    score: float  # -1 to 1 for compound, 0 to 1 for confidence
    positive_score: float = 0.0
    negative_score: float = 0.0
    neutral_score: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            "sentiment_label": self.label.value,
            "sentiment_score": self.score,
            "positive_score": self.positive_score,
            "negative_score": self.negative_score,
            "neutral_score": self.neutral_score,
        }


@dataclass
class IntentResult:
    """Result of intent extraction."""
    primary_intent: FinancialIntent
    confidence: float
    secondary_intents: List[Dict[str, float]] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "primary_intent": self.primary_intent.value,
            "intent_confidence": self.confidence,
            "secondary_intents": [
                {"intent": k, "score": v} 
                for item in self.secondary_intents 
                for k, v in item.items()
            ],
        }


@dataclass
class KeywordResult:
    """Result of keyword extraction."""
    keywords: List[str]
    keyword_scores: Dict[str, float]
    entities: List[Dict[str, str]] = field(default_factory=list)  # Named entities
    
    def to_dict(self) -> Dict:
        return {
            "keywords": self.keywords,
            "keyword_scores": self.keyword_scores,
            "entities": self.entities,
        }


@dataclass
class TransformedArticle:
    """Complete transformed article ready for ML/AI consumption."""
    # Original fields
    id: int
    source: str
    headline: str
    href: str
    summary: Optional[str] = None
    content: Optional[str] = None
    datetime: Optional[str] = None
    
    # Transformed fields
    sentiment: Optional[SentimentResult] = None
    intent: Optional[IntentResult] = None
    keywords: Optional[KeywordResult] = None
    
    # Processed text fields
    cleaned_text: Optional[str] = None
    word_count: int = 0
    
    def to_dict(self) -> Dict:
        """Convert to flat dictionary for storage/export."""
        result = {
            "id": self.id,
            "source": self.source,
            "headline": self.headline,
            "href": self.href,
            "summary": self.summary,
            "content": self.content,
            "datetime": self.datetime,
            "cleaned_text": self.cleaned_text,
            "word_count": self.word_count,
        }
        
        if self.sentiment:
            result.update(self.sentiment.to_dict())
        
        if self.intent:
            result.update(self.intent.to_dict())
        
        if self.keywords:
            result["keywords"] = self.keywords.keywords
            result["entities"] = self.keywords.entities
        
        return result
    
    def to_ml_features(self) -> Dict:
        """Extract features suitable for ML models."""
        features = {
            "word_count": self.word_count,
        }
        
        if self.sentiment:
            features["sentiment_score"] = self.sentiment.score
            features["positive_score"] = self.sentiment.positive_score
            features["negative_score"] = self.sentiment.negative_score
            features["neutral_score"] = self.sentiment.neutral_score
        
        if self.intent:
            features["intent_confidence"] = self.intent.confidence
            # One-hot encode primary intent
            for intent in FinancialIntent:
                features[f"intent_{intent.value}"] = (
                    1.0 if self.intent.primary_intent == intent else 0.0
                )
        
        return features


# ============================================================================
# Base Transformer Interface
# ============================================================================

class BaseTransformer(ABC):
    """Abstract base class for text transformers."""
    
    @abstractmethod
    def transform(self, text: str) -> Any:
        """Transform a single text input."""
        pass
    
    @abstractmethod
    def transform_batch(self, texts: List[str]) -> List[Any]:
        """Transform a batch of texts."""
        pass


# ============================================================================
# Sentiment Analysis
# ============================================================================

class SentimentAnalyzer(BaseTransformer):
    """
    Sentiment analyzer with multiple backend options.
    
    Backends:
        - 'vader': NLTK's VADER (fast, rule-based)
        - 'textblob': TextBlob (fast, pattern-based)
        - 'transformers': HuggingFace Transformers (accurate, slower)
    """
    
    def __init__(self, backend: str = "vader"):
        self.backend = backend
        self._analyzer = None
        self._initialize_backend()
    
    def _initialize_backend(self):
        """Initialize the sentiment analysis backend."""
        if self.backend == "vader":
            try:
                from nltk.sentiment.vader import SentimentIntensityAnalyzer
                import nltk
                try:
                    nltk.data.find('sentiment/vader_lexicon.zip')
                except LookupError:
                    nltk.download('vader_lexicon', quiet=True)
                self._analyzer = SentimentIntensityAnalyzer()
                logger.info("Initialized VADER sentiment analyzer")
            except ImportError:
                logger.error("NLTK not installed. Install with: pip install nltk")
                raise
        
        elif self.backend == "textblob":
            try:
                from textblob import TextBlob
                self._analyzer = TextBlob
                logger.info("Initialized TextBlob sentiment analyzer")
            except ImportError:
                logger.error("TextBlob not installed. Install with: pip install textblob")
                raise
        
        elif self.backend == "transformers":
            try:
                from transformers import pipeline
                self._analyzer = pipeline(
                    "sentiment-analysis",
                    model="ProsusAI/finbert",  # Financial domain model
                    truncation=True,
                    max_length=512
                )
                logger.info("Initialized FinBERT transformer sentiment analyzer")
            except ImportError:
                logger.error("Transformers not installed. Install with: pip install transformers torch")
                raise
        else:
            raise ValueError(f"Unknown backend: {self.backend}")
    
    def transform(self, text: str) -> SentimentResult:
        """Analyze sentiment of a single text."""
        if not text or not text.strip():
            return SentimentResult(
                label=SentimentLabel.NEUTRAL,
                score=0.0,
                positive_score=0.0,
                negative_score=0.0,
                neutral_score=1.0
            )
        
        if self.backend == "vader":
            return self._analyze_vader(text)
        elif self.backend == "textblob":
            return self._analyze_textblob(text)
        elif self.backend == "transformers":
            return self._analyze_transformers(text)
    
    def _analyze_vader(self, text: str) -> SentimentResult:
        """VADER sentiment analysis."""
        scores = self._analyzer.polarity_scores(text)
        
        compound = scores['compound']
        if compound >= 0.05:
            label = SentimentLabel.POSITIVE
        elif compound <= -0.05:
            label = SentimentLabel.NEGATIVE
        else:
            label = SentimentLabel.NEUTRAL
        
        return SentimentResult(
            label=label,
            score=compound,
            positive_score=scores['pos'],
            negative_score=scores['neg'],
            neutral_score=scores['neu']
        )
    
    def _analyze_textblob(self, text: str) -> SentimentResult:
        """TextBlob sentiment analysis."""
        blob = self._analyzer(text)
        polarity = blob.sentiment.polarity
        
        if polarity > 0.1:
            label = SentimentLabel.POSITIVE
        elif polarity < -0.1:
            label = SentimentLabel.NEGATIVE
        else:
            label = SentimentLabel.NEUTRAL
        
        # Convert polarity to score components
        pos = max(0, polarity)
        neg = abs(min(0, polarity))
        neu = 1 - abs(polarity)
        
        return SentimentResult(
            label=label,
            score=polarity,
            positive_score=pos,
            negative_score=neg,
            neutral_score=neu
        )
    
    def _analyze_transformers(self, text: str) -> SentimentResult:
        """Transformer-based sentiment analysis."""
        # Truncate text if too long
        text = text[:4000] if len(text) > 4000 else text
        
        result = self._analyzer(text)[0]
        label_str = result['label'].lower()
        score = result['score']
        
        if label_str == 'positive':
            label = SentimentLabel.POSITIVE
            pos, neg, neu = score, 0.0, 1 - score
        elif label_str == 'negative':
            label = SentimentLabel.NEGATIVE
            pos, neg, neu = 0.0, score, 1 - score
        else:
            label = SentimentLabel.NEUTRAL
            pos, neg, neu = 0.0, 0.0, score
        
        # Convert to compound-like score (-1 to 1)
        if label == SentimentLabel.POSITIVE:
            compound = score
        elif label == SentimentLabel.NEGATIVE:
            compound = -score
        else:
            compound = 0.0
        
        return SentimentResult(
            label=label,
            score=compound,
            positive_score=pos,
            negative_score=neg,
            neutral_score=neu
        )
    
    def transform_batch(self, texts: List[str]) -> List[SentimentResult]:
        """Analyze sentiment for a batch of texts."""
        return [self.transform(text) for text in texts]


# ============================================================================
# Intent Extraction
# ============================================================================

class IntentExtractor(BaseTransformer):
    """
    Extract intent/category from financial news text.
    
    Uses keyword-based classification with optional transformer enhancement.
    """
    
    # Intent keywords for rule-based classification
    INTENT_KEYWORDS = {
        FinancialIntent.MARKET_UPDATE: [
            'market', 'trading', 'stock', 'index', 'dow', 'nasdaq', 's&p',
            'rally', 'plunge', 'surge', 'drop', 'gain', 'loss', 'volume',
            'bull', 'bear', 'correction', 'rebound'
        ],
        FinancialIntent.PRICE_PREDICTION: [
            'predict', 'forecast', 'expect', 'target', 'outlook', 'projection',
            'estimate', 'anticipate', 'could reach', 'may hit', 'potential',
            'analyst', 'rating', 'upgrade', 'downgrade'
        ],
        FinancialIntent.REGULATORY_NEWS: [
            'regulation', 'sec', 'fda', 'approval', 'compliance', 'lawsuit',
            'investigation', 'fine', 'penalty', 'ban', 'policy', 'government',
            'legislation', 'legal', 'court', 'ruling'
        ],
        FinancialIntent.COMPANY_NEWS: [
            'ceo', 'earnings', 'revenue', 'profit', 'acquisition', 'merger',
            'ipo', 'dividend', 'quarterly', 'annual', 'report', 'announces',
            'partnership', 'deal', 'contract', 'layoffs', 'hiring'
        ],
        FinancialIntent.TECHNOLOGY_UPDATE: [
            'blockchain', 'crypto', 'bitcoin', 'ethereum', 'defi', 'nft',
            'ai', 'artificial intelligence', 'machine learning', 'innovation',
            'technology', 'platform', 'protocol', 'network', 'upgrade'
        ],
        FinancialIntent.ANALYSIS_OPINION: [
            'analysis', 'opinion', 'perspective', 'review', 'commentary',
            'insight', 'expert', 'strategy', 'recommendation', 'advice',
            'think', 'believe', 'argue', 'suggest'
        ],
        FinancialIntent.BREAKING_NEWS: [
            'breaking', 'urgent', 'just in', 'developing', 'alert',
            'exclusive', 'first', 'confirms', 'announces'
        ],
    }
    
    def __init__(self, use_transformers: bool = False):
        self.use_transformers = use_transformers
        self._classifier = None
        
        if use_transformers:
            self._initialize_transformer()
    
    def _initialize_transformer(self):
        """Initialize transformer-based zero-shot classifier."""
        try:
            from transformers import pipeline
            self._classifier = pipeline(
                "zero-shot-classification",
                model="facebook/bart-large-mnli",
                truncation=True
            )
            logger.info("Initialized zero-shot intent classifier")
        except ImportError:
            logger.warning("Transformers not available, falling back to rule-based")
            self.use_transformers = False
    
    def transform(self, text: str) -> IntentResult:
        """Extract intent from text."""
        if not text or not text.strip():
            return IntentResult(
                primary_intent=FinancialIntent.GENERAL_INFO,
                confidence=0.0
            )
        
        if self.use_transformers and self._classifier:
            return self._classify_transformer(text)
        else:
            return self._classify_rules(text)
    
    def _classify_rules(self, text: str) -> IntentResult:
        """Rule-based intent classification using keywords."""
        text_lower = text.lower()
        intent_scores = {}
        
        for intent, keywords in self.INTENT_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            # Normalize by keyword count
            intent_scores[intent] = score / len(keywords) if keywords else 0
        
        # Get top intents
        sorted_intents = sorted(
            intent_scores.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        primary = sorted_intents[0] if sorted_intents else (FinancialIntent.GENERAL_INFO, 0)
        
        # Get secondary intents (those with score > 0)
        secondary = [
            {intent.value: score} 
            for intent, score in sorted_intents[1:4] 
            if score > 0
        ]
        
        return IntentResult(
            primary_intent=primary[0],
            confidence=min(primary[1] * 2, 1.0),  # Scale confidence
            secondary_intents=secondary
        )
    
    def _classify_transformer(self, text: str) -> IntentResult:
        """Transformer-based zero-shot intent classification."""
        # Truncate for model
        text = text[:1000] if len(text) > 1000 else text
        
        candidate_labels = [intent.value.replace('_', ' ') for intent in FinancialIntent]
        
        result = self._classifier(text, candidate_labels, multi_label=True)
        
        # Map back to enum
        label_to_intent = {
            intent.value.replace('_', ' '): intent 
            for intent in FinancialIntent
        }
        
        primary_label = result['labels'][0]
        primary_intent = label_to_intent.get(primary_label, FinancialIntent.GENERAL_INFO)
        
        secondary = [
            {label_to_intent.get(label, FinancialIntent.GENERAL_INFO).value: score}
            for label, score in zip(result['labels'][1:4], result['scores'][1:4])
        ]
        
        return IntentResult(
            primary_intent=primary_intent,
            confidence=result['scores'][0],
            secondary_intents=secondary
        )
    
    def transform_batch(self, texts: List[str]) -> List[IntentResult]:
        """Extract intents from a batch of texts."""
        return [self.transform(text) for text in texts]


# ============================================================================
# Keyword Extraction
# ============================================================================

class KeywordExtractor(BaseTransformer):
    """
    Extract keywords and named entities from text.
    
    Methods:
        - 'tfidf': TF-IDF based extraction
        - 'spacy': spaCy NLP-based extraction with NER
        - 'rake': RAKE algorithm (Rapid Automatic Keyword Extraction)
    """
    
    def __init__(self, method: str = "tfidf", top_n: int = 10):
        self.method = method
        self.top_n = top_n
        self._extractor = None
        self._nlp = None
        self._initialize()
    
    def _initialize(self):
        """Initialize keyword extraction method."""
        if self.method == "tfidf":
            from sklearn.feature_extraction.text import TfidfVectorizer
            self._extractor = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                ngram_range=(1, 2)
            )
            logger.info("Initialized TF-IDF keyword extractor")
        
        elif self.method == "spacy":
            try:
                import spacy
                try:
                    self._nlp = spacy.load("en_core_web_sm")
                except OSError:
                    logger.warning("Downloading spaCy model...")
                    from spacy.cli import download
                    download("en_core_web_sm")
                    self._nlp = spacy.load("en_core_web_sm")
                logger.info("Initialized spaCy keyword extractor")
            except ImportError:
                logger.error("spaCy not installed. Install with: pip install spacy")
                raise
        
        elif self.method == "rake":
            # Simple RAKE-like implementation
            self._initialize_stopwords()
            logger.info("Initialized RAKE keyword extractor")
        else:
            raise ValueError(f"Unknown method: {self.method}")
    
    def _initialize_stopwords(self):
        """Initialize stopwords for RAKE."""
        try:
            import nltk
            try:
                nltk.data.find('corpora/stopwords')
            except LookupError:
                nltk.download('stopwords', quiet=True)
            from nltk.corpus import stopwords
            self._stopwords = set(stopwords.words('english'))
        except ImportError:
            # Fallback stopwords
            self._stopwords = {
                'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
                'for', 'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were',
                'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
                'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall',
                'this', 'that', 'these', 'those', 'it', 'its', 'as', 'if', 'when',
                'than', 'because', 'while', 'where', 'after', 'so', 'though',
                'since', 'until', 'whether', 'before', 'although', 'nor', 'like',
                'once', 'unless', 'now', 'even', 'also', 'just', 'only', 'both',
                'through', 'during', 'each', 'all', 'any', 'such', 'no', 'not',
            }
    
    def transform(self, text: str) -> KeywordResult:
        """Extract keywords from text."""
        if not text or not text.strip():
            return KeywordResult(keywords=[], keyword_scores={}, entities=[])
        
        if self.method == "tfidf":
            return self._extract_tfidf(text)
        elif self.method == "spacy":
            return self._extract_spacy(text)
        elif self.method == "rake":
            return self._extract_rake(text)
    
    def _extract_tfidf(self, text: str) -> KeywordResult:
        """TF-IDF based keyword extraction."""
        try:
            # Fit on single document
            tfidf_matrix = self._extractor.fit_transform([text])
            feature_names = self._extractor.get_feature_names_out()
            scores = tfidf_matrix.toarray()[0]
            
            # Get top keywords
            top_indices = scores.argsort()[-self.top_n:][::-1]
            keywords = [feature_names[i] for i in top_indices if scores[i] > 0]
            keyword_scores = {
                feature_names[i]: float(scores[i]) 
                for i in top_indices if scores[i] > 0
            }
            
            return KeywordResult(
                keywords=keywords,
                keyword_scores=keyword_scores,
                entities=[]  # TF-IDF doesn't extract entities
            )
        except Exception as e:
            logger.warning(f"TF-IDF extraction failed: {e}")
            return KeywordResult(keywords=[], keyword_scores={}, entities=[])
    
    def _extract_spacy(self, text: str) -> KeywordResult:
        """spaCy-based keyword and entity extraction."""
        doc = self._nlp(text[:100000])  # Limit text length
        
        # Extract keywords (nouns and proper nouns)
        keywords = []
        keyword_scores = {}
        
        # Get noun chunks
        for chunk in doc.noun_chunks:
            if len(chunk.text) > 2:
                keywords.append(chunk.text.lower())
                keyword_scores[chunk.text.lower()] = 1.0
        
        # Get important single tokens
        for token in doc:
            if (token.pos_ in ['NOUN', 'PROPN'] and 
                not token.is_stop and 
                len(token.text) > 2):
                keywords.append(token.text.lower())
                keyword_scores[token.text.lower()] = 0.8
        
        # Deduplicate and limit
        keywords = list(dict.fromkeys(keywords))[:self.top_n]
        keyword_scores = {k: v for k, v in keyword_scores.items() if k in keywords}
        
        # Extract named entities
        entities = [
            {"text": ent.text, "label": ent.label_}
            for ent in doc.ents
        ]
        
        return KeywordResult(
            keywords=keywords,
            keyword_scores=keyword_scores,
            entities=entities
        )
    
    def _extract_rake(self, text: str) -> KeywordResult:
        """RAKE-like keyword extraction."""
        # Simple implementation of RAKE algorithm
        
        # Clean and tokenize
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        
        # Remove stopwords
        filtered = [w for w in words if w not in self._stopwords]
        
        # Count word frequencies
        word_freq = {}
        for word in filtered:
            word_freq[word] = word_freq.get(word, 0) + 1
        
        # Sort by frequency
        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        
        # Get top keywords
        keywords = [w[0] for w in sorted_words[:self.top_n]]
        max_freq = sorted_words[0][1] if sorted_words else 1
        keyword_scores = {
            w[0]: w[1] / max_freq 
            for w in sorted_words[:self.top_n]
        }
        
        return KeywordResult(
            keywords=keywords,
            keyword_scores=keyword_scores,
            entities=[]
        )
    
    def transform_batch(self, texts: List[str]) -> List[KeywordResult]:
        """Extract keywords from a batch of texts."""
        return [self.transform(text) for text in texts]


# ============================================================================
# Text Preprocessing
# ============================================================================

class TextPreprocessor:
    """Clean and preprocess text for analysis."""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean text by removing noise."""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+', '', text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove special characters (keep basic punctuation)
        text = re.sub(r'[^\w\s.,!?-]', '', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    @staticmethod
    def word_count(text: str) -> int:
        """Count words in text."""
        if not text:
            return 0
        return len(text.split())


# ============================================================================
# Main Transformation Pipeline
# ============================================================================

class TextTransformationPipeline:
    """
    Complete text transformation pipeline for ML/AI preparation.
    
    Combines sentiment analysis, intent extraction, and keyword extraction
    into a single pipeline that processes articles for ML consumption.
    """
    
    def __init__(
        self,
        sentiment_backend: str = "vader",
        use_transformer_intents: bool = False,
        keyword_method: str = "tfidf",
        keyword_top_n: int = 10
    ):
        """
        Initialize the transformation pipeline.
        
        Args:
            sentiment_backend: 'vader', 'textblob', or 'transformers'
            use_transformer_intents: Use transformers for intent classification
            keyword_method: 'tfidf', 'spacy', or 'rake'
            keyword_top_n: Number of top keywords to extract
        """
        self.preprocessor = TextPreprocessor()
        
        logger.info("Initializing text transformation pipeline...")
        
        self.sentiment_analyzer = SentimentAnalyzer(backend=sentiment_backend)
        self.intent_extractor = IntentExtractor(use_transformers=use_transformer_intents)
        self.keyword_extractor = KeywordExtractor(method=keyword_method, top_n=keyword_top_n)
        
        logger.info("Text transformation pipeline initialized")
    
    def transform_article(self, article_dict: Dict) -> TransformedArticle:
        """
        Transform a single article dictionary.
        
        Args:
            article_dict: Dictionary with article data (id, headline, content, etc.)
        
        Returns:
            TransformedArticle with all transformations applied
        """
        # Get text content (prefer content, fallback to summary, then headline)
        text = article_dict.get('content') or article_dict.get('summary') or article_dict.get('headline', '')
        
        # Clean text
        cleaned_text = self.preprocessor.clean_text(text)
        
        # Create transformed article
        transformed = TransformedArticle(
            id=article_dict.get('id', 0),
            source=article_dict.get('source', ''),
            headline=article_dict.get('headline', ''),
            href=article_dict.get('href', ''),
            summary=article_dict.get('summary'),
            content=article_dict.get('content'),
            datetime=article_dict.get('datetime'),
            cleaned_text=cleaned_text,
            word_count=self.preprocessor.word_count(cleaned_text)
        )
        
        # Apply transformations
        if cleaned_text:
            transformed.sentiment = self.sentiment_analyzer.transform(cleaned_text)
            transformed.intent = self.intent_extractor.transform(cleaned_text)
            transformed.keywords = self.keyword_extractor.transform(cleaned_text)
        
        return transformed
    
    def transform(self, data: Union[pd.DataFrame, List[Dict]]) -> pd.DataFrame:
        """
        Transform a DataFrame or list of article dictionaries.
        
        Args:
            data: DataFrame with article columns or list of article dicts
        
        Returns:
            DataFrame with original data plus transformation columns
        """
        if isinstance(data, pd.DataFrame):
            articles = data.to_dict('records')
        else:
            articles = data
        
        logger.info(f"Transforming {len(articles)} articles...")
        
        transformed_articles = []
        for i, article in enumerate(articles):
            try:
                transformed = self.transform_article(article)
                transformed_articles.append(transformed.to_dict())
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(articles)} articles")
            except Exception as e:
                logger.error(f"Failed to transform article {i}: {e}")
                # Add original article with empty transformations
                transformed_articles.append(article)
        
        logger.info(f"Transformation complete: {len(transformed_articles)} articles processed")
        
        return pd.DataFrame(transformed_articles)
    
    def get_ml_features(self, data: Union[pd.DataFrame, List[Dict]]) -> pd.DataFrame:
        """
        Extract ML-ready numerical features from articles.
        
        Args:
            data: DataFrame or list of article dictionaries
        
        Returns:
            DataFrame with numerical features suitable for ML models
        """
        if isinstance(data, pd.DataFrame):
            articles = data.to_dict('records')
        else:
            articles = data
        
        features = []
        for article in articles:
            transformed = self.transform_article(article)
            features.append(transformed.to_ml_features())
        
        return pd.DataFrame(features)

