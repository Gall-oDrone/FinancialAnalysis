"""
Ticker and Symbol Extraction Module

Extracts cryptocurrency and stock tickers from financial news text.
Handles various formats:
- Standard format: BTC-USD, ETH-USD, SOL-USD
- Parenthetical format: Bitcoin (CRYPTO: BTC), Ethereum (CRYPTO: ETH)
- Common names: bitcoin → BTC-USD, ethereum → ETH-USD

Usage:
    from transform.news.ticker_extractor import TickerExtractor
    
    extractor = TickerExtractor()
    tickers = extractor.extract("Bitcoin (CRYPTO: BTC) surged past $100,000...")
    # Returns: ["BTC-USD"]
"""

import re
from typing import List, Set, Dict, Optional
from dataclasses import dataclass

from core.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Ticker Mapping
# ============================================================================

# Map common cryptocurrency names to their standard ticker symbols
CRYPTO_NAME_TO_TICKER = {
    # Major cryptocurrencies
    "bitcoin": "BTC-USD",
    "btc": "BTC-USD",
    "ethereum": "ETH-USD",
    "eth": "ETH-USD",
    "solana": "SOL-USD",
    "sol": "SOL-USD",
    "ripple": "XRP-USD",
    "xrp": "XRP-USD",
    "cardano": "ADA-USD",
    "ada": "ADA-USD",
    "dogecoin": "DOGE-USD",
    "doge": "DOGE-USD",
    "polkadot": "DOT-USD",
    "dot": "DOT-USD",
    "polygon": "MATIC-USD",
    "matic": "MATIC-USD",
    "avalanche": "AVAX-USD",
    "avax": "AVAX-USD",
    "chainlink": "LINK-USD",
    "link": "LINK-USD",
    "uniswap": "UNI-USD",
    "uni": "UNI-USD",
    "litecoin": "LTC-USD",
    "ltc": "LTC-USD",
    "shiba": "SHIB-USD",
    "shib": "SHIB-USD",
    "tron": "TRX-USD",
    "trx": "TRX-USD",
    "near": "NEAR-USD",
    "algorand": "ALGO-USD",
    "algo": "ALGO-USD",
    "cosmos": "ATOM-USD",
    "atom": "ATOM-USD",
    "filecoin": "FIL-USD",
    "fil": "FIL-USD",
    "sandbox": "SAND-USD",
    "sand": "SAND-USD",
    "decentraland": "MANA-USD",
    "mana": "MANA-USD",
    "apecoin": "APE-USD",
    "ape": "APE-USD",
    "yearn": "YFI-USD",
    "yfi": "YFI-USD",
    "sushi": "SUSHI-USD",
    "curve": "CRV-USD",
    "crv": "CRV-USD",
    "synthetix": "SNX-USD",
    "snx": "SNX-USD",
    "balancer": "BAL-USD",
    "bal": "BAL-USD",
    "lido": "LDO-USD",
    "ldo": "LDO-USD",
    "enjin": "ENJ-USD",
    "enj": "ENJ-USD",
    "stellar": "XLM-USD",
    "xlm": "XLM-USD",
    "omisego": "OMG-USD",
    "omg": "OMG-USD",
    "dydx": "DYDX-USD",
    "paxos": "PAXG-USD",
    "paxg": "PAXG-USD",
    "ondo": "ONDO-USD",
    "bonk": "BONK-USD",
    "virtual": "VIRTUAL-USD",
    "tusd": "TUSD-BTC",
    "bar": "BAR-USD",
    "psg": "PSG-USD",
    "tigres": "TIGRES-USD",
}


# ============================================================================
# Ticker Extractor
# ============================================================================

@dataclass
class TickerExtractionResult:
    """Result of ticker extraction."""
    tickers: List[str]
    confidence: float  # 0-1, based on extraction method
    extraction_method: str  # 'pattern', 'name_mapping', 'combined'
    
    def to_dict(self) -> Dict:
        return {
            "tickers": self.tickers,
            "ticker_confidence": self.confidence,
            "extraction_method": self.extraction_method
        }


class TickerExtractor:
    """
    Extract cryptocurrency and stock tickers from text.
    
    Extraction methods:
    1. Pattern matching: BTC-USD, ETH-BTC, etc.
    2. Parenthetical format: "Bitcoin (CRYPTO: BTC)"
    3. Name mapping: "bitcoin" → BTC-USD
    """
    
    # Regex patterns for ticker extraction
    PATTERNS = {
        # Standard crypto ticker format: XXX-YYY
        'standard': re.compile(r'\b([A-Z]{2,5})-([A-Z]{3,4})\b'),
        
        # Parenthetical format: (CRYPTO: XXX) or (STOCK: XXX)
        'parenthetical': re.compile(r'\((CRYPTO|STOCK):\s*([A-Z]{2,5})\)'),
        
        # Inline ticker mention: $BTC, $ETH
        'inline': re.compile(r'\$([A-Z]{2,5})\b'),
    }
    
    def __init__(self, ticker_map: Optional[Dict[str, str]] = None):
        """
        Initialize ticker extractor.
        
        Args:
            ticker_map: Custom mapping of names to tickers (optional)
        """
        self.ticker_map = ticker_map or CRYPTO_NAME_TO_TICKER
        
        # Create reverse map for validation
        self.valid_tickers = set(self.ticker_map.values())
        
        # Add common variations
        self._build_variations()
    
    def _build_variations(self):
        """Build variations of ticker names for better matching."""
        variations = {}
        
        for name, ticker in self.ticker_map.items():
            # Add plural forms
            if not name.endswith('s'):
                variations[name + 's'] = ticker
            
            # Add possessive forms
            variations[name + "'s"] = ticker
            
            # Add common abbreviations
            if len(name) >= 4:
                variations[name[:3]] = ticker
        
        self.ticker_map.update(variations)
    
    def extract_from_text(self, text: str) -> TickerExtractionResult:
        """
        Extract tickers from text using all available methods.
        
        Args:
            text: Text to extract tickers from
        
        Returns:
            TickerExtractionResult with unique tickers and metadata
        """
        if not text or not text.strip():
            return TickerExtractionResult(
                tickers=[],
                confidence=0.0,
                extraction_method='none'
            )
        
        all_tickers = set()
        extraction_methods = []
        
        # Method 1: Pattern-based extraction
        pattern_tickers = self._extract_by_patterns(text)
        if pattern_tickers:
            all_tickers.update(pattern_tickers)
            extraction_methods.append('pattern')
        
        # Method 2: Name-based extraction
        name_tickers = self._extract_by_names(text)
        if name_tickers:
            all_tickers.update(name_tickers)
            extraction_methods.append('name_mapping')
        
        # Calculate confidence based on extraction method
        confidence = self._calculate_confidence(all_tickers, extraction_methods)
        
        method_str = '+'.join(extraction_methods) if extraction_methods else 'none'
        
        return TickerExtractionResult(
            tickers=sorted(list(all_tickers)),
            confidence=confidence,
            extraction_method=method_str
        )
    
    def _extract_by_patterns(self, text: str) -> Set[str]:
        """Extract tickers using regex patterns."""
        tickers = set()
        
        # Standard format: BTC-USD, ETH-BTC
        for match in self.PATTERNS['standard'].finditer(text):
            base, quote = match.groups()
            ticker = f"{base}-{quote}"
            tickers.add(ticker)
        
        # Parenthetical format: (CRYPTO: BTC)
        for match in self.PATTERNS['parenthetical'].finditer(text):
            asset_type, symbol = match.groups()
            # Convert to standard format
            if asset_type == 'CRYPTO':
                ticker = f"{symbol}-USD"
            else:
                ticker = symbol
            tickers.add(ticker)
        
        # Inline format: $BTC
        for match in self.PATTERNS['inline'].finditer(text):
            symbol = match.group(1)
            # Assume USD-quoted for crypto
            ticker = f"{symbol}-USD"
            tickers.add(ticker)
        
        return tickers
    
    def _extract_by_names(self, text: str) -> Set[str]:
        """Extract tickers by matching common cryptocurrency names."""
        text_lower = text.lower()
        tickers = set()
        
        # Word boundary pattern for each name
        for name, ticker in self.ticker_map.items():
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(name) + r'\b'
            if re.search(pattern, text_lower):
                tickers.add(ticker)
        
        return tickers
    
    def _calculate_confidence(
        self, 
        tickers: Set[str], 
        methods: List[str]
    ) -> float:
        """
        Calculate confidence score based on extraction.
        
        Pattern-based: high confidence (0.9)
        Name-based only: medium confidence (0.6)
        Both methods: very high confidence (0.95)
        """
        if not tickers:
            return 0.0
        
        if len(methods) >= 2:
            return 0.95
        elif 'pattern' in methods:
            return 0.9
        elif 'name_mapping' in methods:
            return 0.6
        else:
            return 0.5
    
    def extract_batch(self, texts: List[str]) -> List[TickerExtractionResult]:
        """
        Extract tickers from a batch of texts.
        
        Args:
            texts: List of text strings
        
        Returns:
            List of TickerExtractionResult
        """
        return [self.extract_from_text(text) for text in texts]
    
    def extract_from_article(self, article_dict: Dict) -> List[str]:
        """
        Extract tickers from an article dictionary.
        
        Combines headline, summary, and content for extraction.
        
        Args:
            article_dict: Dictionary with 'headline', 'summary', 'content'
        
        Returns:
            List of unique ticker strings
        """
        # Combine all text fields
        text_parts = []
        
        if article_dict.get('headline'):
            text_parts.append(article_dict['headline'])
        
        if article_dict.get('summary'):
            text_parts.append(article_dict['summary'])
        
        if article_dict.get('content'):
            # Take first 2000 chars of content for performance
            content = article_dict['content'][:2000]
            text_parts.append(content)
        
        combined_text = ' '.join(text_parts)
        
        result = self.extract_from_text(combined_text)
        return result.tickers


# ============================================================================
# Convenience Functions
# ============================================================================

def extract_tickers_from_dataframe(
    df,
    text_column: str = 'content',
    output_column: str = 'tickers'
):
    """
    Extract tickers from a DataFrame column.
    
    Args:
        df: DataFrame with text data
        text_column: Column name containing text
        output_column: Column name for extracted tickers
    
    Returns:
        DataFrame with added tickers column
    """
    extractor = TickerExtractor()
    
    df[output_column] = df[text_column].apply(
        lambda x: extractor.extract_from_text(str(x) if x else '').tickers
    )
    
    return df


def extract_tickers_from_article_df(df):
    """
    Extract tickers from article DataFrame (headline + summary + content).
    
    Args:
        df: DataFrame with 'headline', 'summary', 'content' columns
    
    Returns:
        DataFrame with added 'tickers' column
    """
    extractor = TickerExtractor()
    
    df['tickers'] = df.apply(
        lambda row: extractor.extract_from_article(row.to_dict()),
        axis=1
    )
    
    return df


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example texts
    examples = [
        "Bitcoin (CRYPTO: BTC) surged past $100,000 today.",
        "BTC-USD rallied 10% while ETH-USD gained 5%.",
        "Ethereum developers announced a major upgrade.",
        "The crypto market saw heavy trading in $SOL and $AVAX.",
        "Analysts predict Bitcoin could reach $150k by year end.",
    ]
    
    extractor = TickerExtractor()
    
    print("Ticker Extraction Examples")
    print("=" * 60)
    
    for text in examples:
        result = extractor.extract_from_text(text)
        print(f"\nText: {text}")
        print(f"Tickers: {result.tickers}")
        print(f"Confidence: {result.confidence:.2f}")
        print(f"Method: {result.extraction_method}")
