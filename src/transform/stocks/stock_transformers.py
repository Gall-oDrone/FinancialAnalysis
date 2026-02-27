"""
Stock Data Transformation Module

Provides transformations for stock/crypto price data including:
- Returns calculation (log, simple)
- Volatility metrics (rolling std, Parkinson, Garman-Klass)
- Technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands)

Usage:
    from DataProcessing.stock_transformers import StockTransformationPipeline
    
    pipeline = StockTransformationPipeline()
    transformed_df = pipeline.transform(stocks_df)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
import numpy as np
import pandas as pd

from core.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Returns Calculator
# ============================================================================

class ReturnsCalculator:
    """Calculate returns from price data."""
    
    @staticmethod
    def simple_return(prices: pd.Series) -> pd.Series:
        """
        Calculate simple returns: r_t = (P_t - P_{t-1}) / P_{t-1}
        
        Args:
            prices: Series of prices
        
        Returns:
            Series of simple returns
        """
        return prices.pct_change()
    
    @staticmethod
    def log_return(prices: pd.Series) -> pd.Series:
        """
        Calculate log returns: r_t = ln(P_t / P_{t-1})
        
        Args:
            prices: Series of prices
        
        Returns:
            Series of log returns
        """
        return np.log(prices / prices.shift(1))
    
    @staticmethod
    def add_returns(df: pd.DataFrame, price_col: str = 'close') -> pd.DataFrame:
        """
        Add return columns to DataFrame.
        
        Args:
            df: DataFrame with price data
            price_col: Column name for prices (default: 'close')
        
        Returns:
            DataFrame with 'simple_return' and 'log_return' columns added
        """
        df = df.copy()
        df['simple_return'] = ReturnsCalculator.simple_return(df[price_col])
        df['log_return'] = ReturnsCalculator.log_return(df[price_col])
        return df


# ============================================================================
# Volatility Calculator
# ============================================================================

class VolatilityCalculator:
    """Calculate volatility metrics."""
    
    @staticmethod
    def rolling_std(returns: pd.Series, window: int = 20) -> pd.Series:
        """
        Calculate rolling standard deviation of returns.
        
        Args:
            returns: Series of returns
            window: Rolling window size (default: 20 days)
        
        Returns:
            Series of rolling volatility
        """
        return returns.rolling(window=window).std()
    
    @staticmethod
    def parkinson_volatility(
        high: pd.Series,
        low: pd.Series,
        window: int = 20
    ) -> pd.Series:
        """
        Calculate Parkinson's volatility using high-low prices.
        
        More efficient estimator than close-to-close volatility.
        
        Formula: σ = sqrt(1/(4*ln(2)) * <(ln(H/L))^2>)
        
        Args:
            high: Series of high prices
            low: Series of low prices
            window: Rolling window size
        
        Returns:
            Series of Parkinson volatility
        """
        hl_ratio = np.log(high / low)
        parkinson = np.sqrt(
            (1 / (4 * np.log(2))) * hl_ratio.pow(2).rolling(window=window).mean()
        )
        return parkinson
    
    @staticmethod
    def garman_klass_volatility(
        open_: pd.Series,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        window: int = 20
    ) -> pd.Series:
        """
        Calculate Garman-Klass volatility using OHLC prices.
        
        More accurate than Parkinson for trending markets.
        
        Args:
            open_: Series of open prices
            high: Series of high prices
            low: Series of low prices
            close: Series of close prices
            window: Rolling window size
        
        Returns:
            Series of Garman-Klass volatility
        """
        log_hl = np.log(high / low)
        log_co = np.log(close / open_)
        
        rs = 0.5 * log_hl.pow(2) - (2 * np.log(2) - 1) * log_co.pow(2)
        
        gk_vol = np.sqrt(rs.rolling(window=window).mean())
        return gk_vol
    
    @staticmethod
    def add_volatility(
        df: pd.DataFrame,
        return_col: str = 'log_return',
        windows: List[int] = None
    ) -> pd.DataFrame:
        """
        Add volatility columns to DataFrame.
        
        Args:
            df: DataFrame with return/price data
            return_col: Column to use for rolling std calculation
            windows: List of window sizes (default: [20, 60])
        
        Returns:
            DataFrame with volatility columns added
        """
        if windows is None:
            windows = [20, 60]
        
        df = df.copy()
        
        # Rolling std of returns
        for window in windows:
            col_name = f'volatility_{window}d'
            df[col_name] = VolatilityCalculator.rolling_std(df[return_col], window)
        
        # Parkinson volatility (if high/low available)
        if 'high' in df.columns and 'low' in df.columns:
            df['volatility_parkinson'] = VolatilityCalculator.parkinson_volatility(
                df['high'], df['low'], window=20
            )
        
        # Garman-Klass volatility (if OHLC available)
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            df['volatility_gk'] = VolatilityCalculator.garman_klass_volatility(
                df['open'], df['high'], df['low'], df['close'], window=20
            )
        
        return df


# ============================================================================
# Technical Indicators
# ============================================================================

class TechnicalIndicators:
    """Calculate technical indicators using pandas."""
    
    @staticmethod
    def sma(prices: pd.Series, window: int) -> pd.Series:
        """
        Simple Moving Average.
        
        Args:
            prices: Series of prices
            window: Period for moving average
        
        Returns:
            Series of SMA values
        """
        return prices.rolling(window=window).mean()
    
    @staticmethod
    def ema(prices: pd.Series, span: int) -> pd.Series:
        """
        Exponential Moving Average.
        
        Args:
            prices: Series of prices
            span: Span for EMA calculation
        
        Returns:
            Series of EMA values
        """
        return prices.ewm(span=span, adjust=False).mean()
    
    @staticmethod
    def rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """
        Relative Strength Index.
        
        Args:
            prices: Series of prices
            period: RSI period (default: 14)
        
        Returns:
            Series of RSI values (0-100)
        """
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    def macd(
        prices: pd.Series,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> Dict[str, pd.Series]:
        """
        Moving Average Convergence Divergence.
        
        Args:
            prices: Series of prices
            fast: Fast EMA period (default: 12)
            slow: Slow EMA period (default: 26)
            signal: Signal line EMA period (default: 9)
        
        Returns:
            Dictionary with 'macd', 'signal', and 'histogram' Series
        """
        ema_fast = TechnicalIndicators.ema(prices, fast)
        ema_slow = TechnicalIndicators.ema(prices, slow)
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd': macd_line,
            'macd_signal': signal_line,
            'macd_histogram': histogram
        }
    
    @staticmethod
    def bollinger_bands(
        prices: pd.Series,
        window: int = 20,
        num_std: float = 2.0
    ) -> Dict[str, pd.Series]:
        """
        Bollinger Bands.
        
        Args:
            prices: Series of prices
            window: Period for moving average (default: 20)
            num_std: Number of standard deviations (default: 2)
        
        Returns:
            Dictionary with 'bb_upper', 'bb_middle', 'bb_lower' Series
        """
        sma = prices.rolling(window=window).mean()
        std = prices.rolling(window=window).std()
        
        upper = sma + (std * num_std)
        lower = sma - (std * num_std)
        
        return {
            'bb_upper': upper,
            'bb_middle': sma,
            'bb_lower': lower
        }
    
    @staticmethod
    def add_indicators(
        df: pd.DataFrame,
        price_col: str = 'close',
        sma_periods: List[int] = None,
        ema_periods: List[int] = None,
        add_rsi: bool = True,
        add_macd: bool = True,
        add_bollinger: bool = True
    ) -> pd.DataFrame:
        """
        Add technical indicators to DataFrame.
        
        Args:
            df: DataFrame with price data
            price_col: Column name for prices
            sma_periods: Periods for SMA (default: [20, 50, 200])
            ema_periods: Periods for EMA (default: [12, 26])
            add_rsi: Whether to add RSI (default: True)
            add_macd: Whether to add MACD (default: True)
            add_bollinger: Whether to add Bollinger Bands (default: True)
        
        Returns:
            DataFrame with indicator columns added
        """
        if sma_periods is None:
            sma_periods = [20, 50, 200]
        if ema_periods is None:
            ema_periods = [12, 26]
        
        df = df.copy()
        prices = df[price_col]
        
        # Simple Moving Averages
        for period in sma_periods:
            df[f'sma_{period}'] = TechnicalIndicators.sma(prices, period)
        
        # Exponential Moving Averages
        for period in ema_periods:
            df[f'ema_{period}'] = TechnicalIndicators.ema(prices, period)
        
        # RSI
        if add_rsi:
            df['rsi_14'] = TechnicalIndicators.rsi(prices, 14)
        
        # MACD
        if add_macd:
            macd_dict = TechnicalIndicators.macd(prices)
            for col_name, series in macd_dict.items():
                df[col_name] = series

        # Bollinger Bands
        if add_bollinger:
            bb_dict = TechnicalIndicators.bollinger_bands(prices)
            for col_name, series in bb_dict.items():
                df[col_name] = series
        
        return df


# ============================================================================
# Stock Transformation Pipeline
# ============================================================================

class StockTransformationPipeline:
    """
    Complete stock data transformation pipeline.
    
    Transforms raw OHLCV data into analysis-ready data with:
    - Returns (log and simple)
    - Volatility metrics
    - Technical indicators
    """
    
    def __init__(
        self,
        add_returns: bool = True,
        add_volatility: bool = True,
        add_indicators: bool = True,
        volatility_windows: List[int] = None,
        sma_periods: List[int] = None,
        ema_periods: List[int] = None
    ):
        """
        Initialize the stock transformation pipeline.
        
        Args:
            add_returns: Calculate returns (default: True)
            add_volatility: Calculate volatility metrics (default: True)
            add_indicators: Calculate technical indicators (default: True)
            volatility_windows: Windows for volatility calc (default: [20, 60])
            sma_periods: Periods for SMA (default: [20, 50, 200])
            ema_periods: Periods for EMA (default: [12, 26])
        """
        self.add_returns = add_returns
        self.add_volatility = add_volatility
        self.add_indicators = add_indicators
        self.volatility_windows = volatility_windows or [20, 60]
        self.sma_periods = sma_periods or [20, 50, 200]
        self.ema_periods = ema_periods or [12, 26]
        
        logger.info("Stock transformation pipeline initialized")
    
    def transform(
        self,
        df: pd.DataFrame,
        price_col: str = 'close',
        group_by: Optional[str] = 'book'
    ) -> pd.DataFrame:
        """
        Transform stock DataFrame with all configured transformations.
        
        Args:
            df: DataFrame with OHLCV data
            price_col: Column name for price (default: 'close')
            group_by: Column to group by for per-symbol transforms (default: 'book')
        
        Returns:
            Transformed DataFrame with all indicators
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return df
        
        logger.info(f"Transforming {len(df)} stock records...")
        
        # Ensure date column is datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(['book', 'date'] if group_by else 'date')
        
        # If grouping by symbol, transform each group separately
        if group_by and group_by in df.columns:
            transformed_groups = []
            
            for book, group_df in df.groupby(group_by):
                try:
                    transformed_group = self._transform_group(
                        group_df.copy(),
                        price_col
                    )
                    transformed_groups.append(transformed_group)
                except Exception as e:
                    logger.error(f"Failed to transform book {book}: {e}")
                    transformed_groups.append(group_df)
            
            result = pd.concat(transformed_groups, ignore_index=True)
        else:
            result = self._transform_group(df.copy(), price_col)
        
        logger.info(f"Stock transformation complete: {len(result)} records")
        return result
    
    def _transform_group(self, df: pd.DataFrame, price_col: str) -> pd.DataFrame:
        """Transform a single group (symbol) of stock data."""
        
        # Add returns
        if self.add_returns:
            df = ReturnsCalculator.add_returns(df, price_col)
        
        # Add volatility
        if self.add_volatility:
            return_col = 'log_return' if 'log_return' in df.columns else price_col
            df = VolatilityCalculator.add_volatility(
                df,
                return_col=return_col,
                windows=self.volatility_windows
            )
        
        # Add technical indicators
        if self.add_indicators:
            df = TechnicalIndicators.add_indicators(
                df,
                price_col=price_col,
                sma_periods=self.sma_periods,
                ema_periods=self.ema_periods,
                add_rsi=True,
                add_macd=True,
                add_bollinger=True
            )
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """
        Get list of feature column names that will be added.
        
        Returns:
            List of column names
        """
        columns = []
        
        if self.add_returns:
            columns.extend(['simple_return', 'log_return'])
        
        if self.add_volatility:
            for window in self.volatility_windows:
                columns.append(f'volatility_{window}d')
            columns.extend(['volatility_parkinson', 'volatility_gk'])
        
        if self.add_indicators:
            for period in self.sma_periods:
                columns.append(f'sma_{period}')
            for period in self.ema_periods:
                columns.append(f'ema_{period}')
            columns.extend([
                'rsi_14', 'macd', 'macd_signal', 'macd_histogram',
                'bb_upper', 'bb_middle', 'bb_lower'
            ])
        
        return columns


# ============================================================================
# Convenience Functions
# ============================================================================

def transform_stocks(
    df: pd.DataFrame,
    include_returns: bool = True,
    include_volatility: bool = True,
    include_indicators: bool = True
) -> pd.DataFrame:
    """
    Quick transformation function for stock data.
    
    Args:
        df: DataFrame with OHLCV data
        include_returns: Add return calculations
        include_volatility: Add volatility metrics
        include_indicators: Add technical indicators
    
    Returns:
        Transformed DataFrame
    """
    pipeline = StockTransformationPipeline(
        add_returns=include_returns,
        add_volatility=include_volatility,
        add_indicators=include_indicators
    )
    
    return pipeline.transform(df)


def calculate_risk_metrics(df: pd.DataFrame, window: int = 252) -> Dict[str, float]:
    """
    Calculate risk metrics for a stock DataFrame.
    
    Args:
        df: DataFrame with return data
        window: Window for calculations (default: 252 trading days)
    
    Returns:
        Dictionary with risk metrics
    """
    if 'log_return' not in df.columns:
        logger.warning("No log_return column found")
        return {}
    
    returns = df['log_return'].dropna()
    
    if len(returns) < window:
        window = len(returns)
    
    recent_returns = returns.tail(window)
    
    metrics = {
        'mean_return': float(recent_returns.mean()),
        'volatility': float(recent_returns.std()),
        'sharpe_ratio': float(recent_returns.mean() / recent_returns.std()) if recent_returns.std() > 0 else 0,
        'max_drawdown': float(calculate_max_drawdown(df)),
        'var_95': float(recent_returns.quantile(0.05)),
        'cvar_95': float(recent_returns[recent_returns <= recent_returns.quantile(0.05)].mean())
    }
    
    return metrics


def calculate_max_drawdown(df: pd.DataFrame, price_col: str = 'close') -> float:
    """
    Calculate maximum drawdown.
    
    Args:
        df: DataFrame with price data
        price_col: Column name for prices
    
    Returns:
        Maximum drawdown as a percentage (negative value)
    """
    if price_col not in df.columns:
        return 0.0
    
    prices = df[price_col]
    cummax = prices.cummax()
    drawdown = (prices - cummax) / cummax
    
    return drawdown.min()


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example stock data
    dates = pd.date_range(start='2024-01-01', periods=100, freq='D')
    
    # Simulate price data
    np.random.seed(42)
    initial_price = 100
    returns = np.random.normal(0.001, 0.02, 100)
    prices = initial_price * np.exp(returns.cumsum())
    
    df = pd.DataFrame({
        'book': ['BTC-USD'] * 100,
        'date': dates,
        'open': prices * (1 + np.random.uniform(-0.01, 0.01, 100)),
        'high': prices * (1 + np.random.uniform(0, 0.02, 100)),
        'low': prices * (1 - np.random.uniform(0, 0.02, 100)),
        'close': prices,
        'volume': np.random.randint(1000000, 5000000, 100)
    })
    
    print("Stock Transformation Example")
    print("=" * 60)
    print(f"\nInput data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Transform
    pipeline = StockTransformationPipeline()
    transformed = pipeline.transform(df)
    
    print(f"\nTransformed data shape: {transformed.shape}")
    print(f"New columns: {[c for c in transformed.columns if c not in df.columns]}")
    
    # Show sample
    print("\nSample of transformed data:")
    print(transformed[['date', 'close', 'simple_return', 'volatility_20d', 'rsi_14', 'sma_20']].tail(5))
    
    # Calculate risk metrics
    print("\nRisk Metrics:")
    metrics = calculate_risk_metrics(transformed)
    for key, value in metrics.items():
        print(f"  {key}: {value:.4f}")
