"""
Tests for stock transformation module.
"""

import pytest
import pandas as pd
import numpy as np

from DataProcessing.stock_transformers import (
    ReturnsCalculator,
    VolatilityCalculator,
    TechnicalIndicators,
    StockTransformationPipeline,
    transform_stocks,
    calculate_risk_metrics,
    calculate_max_drawdown
)


class TestReturnsCalculator:
    """Test returns calculation functionality."""
    
    def test_simple_return(self, sample_stocks_df):
        """Test simple return calculation."""
        returns = ReturnsCalculator.simple_return(sample_stocks_df['close'])
        
        assert isinstance(returns, pd.Series)
        assert len(returns) == len(sample_stocks_df)
        # First value should be NaN
        assert pd.isna(returns.iloc[0])
        # Other values should be floats
        assert not pd.isna(returns.iloc[1])
    
    def test_log_return(self, sample_stocks_df):
        """Test log return calculation."""
        returns = ReturnsCalculator.log_return(sample_stocks_df['close'])
        
        assert isinstance(returns, pd.Series)
        assert len(returns) == len(sample_stocks_df)
        assert pd.isna(returns.iloc[0])
        assert not pd.isna(returns.iloc[1])
    
    def test_add_returns(self, sample_stocks_df):
        """Test adding returns to DataFrame."""
        result = ReturnsCalculator.add_returns(sample_stocks_df, 'close')
        
        assert 'simple_return' in result.columns
        assert 'log_return' in result.columns
        assert len(result) == len(sample_stocks_df)
    
    def test_returns_values(self):
        """Test returns calculation with known values."""
        prices = pd.Series([100, 110, 105, 115])
        
        simple_ret = ReturnsCalculator.simple_return(prices)
        
        # Second value: (110-100)/100 = 0.1
        assert abs(simple_ret.iloc[1] - 0.1) < 0.001
        # Third value: (105-110)/110 = -0.0454...
        assert abs(simple_ret.iloc[2] - (-0.045454545)) < 0.001


class TestVolatilityCalculator:
    """Test volatility calculation functionality."""
    
    def test_rolling_std(self, sample_stocks_df):
        """Test rolling standard deviation."""
        returns = ReturnsCalculator.log_return(sample_stocks_df['close'])
        vol = VolatilityCalculator.rolling_std(returns, window=20)
        
        assert isinstance(vol, pd.Series)
        assert len(vol) == len(sample_stocks_df)
        # First 19 values should be NaN
        assert pd.isna(vol.iloc[19])
        # 20th value should exist
        assert not pd.isna(vol.iloc[20])
    
    def test_parkinson_volatility(self, sample_stocks_df):
        """Test Parkinson volatility calculation."""
        vol = VolatilityCalculator.parkinson_volatility(
            sample_stocks_df['high'],
            sample_stocks_df['low'],
            window=20
        )
        
        assert isinstance(vol, pd.Series)
        assert len(vol) == len(sample_stocks_df)
        assert not pd.isna(vol.iloc[-1])
    
    def test_garman_klass_volatility(self, sample_stocks_df):
        """Test Garman-Klass volatility calculation."""
        vol = VolatilityCalculator.garman_klass_volatility(
            sample_stocks_df['open'],
            sample_stocks_df['high'],
            sample_stocks_df['low'],
            sample_stocks_df['close'],
            window=20
        )
        
        assert isinstance(vol, pd.Series)
        assert len(vol) == len(sample_stocks_df)
    
    def test_add_volatility(self, sample_stocks_df):
        """Test adding volatility columns to DataFrame."""
        df = ReturnsCalculator.add_returns(sample_stocks_df)
        result = VolatilityCalculator.add_volatility(df, windows=[20, 60])
        
        assert 'volatility_20d' in result.columns
        assert 'volatility_60d' in result.columns
        assert 'volatility_parkinson' in result.columns
        assert 'volatility_gk' in result.columns


class TestTechnicalIndicators:
    """Test technical indicator functionality."""
    
    def test_sma(self, sample_stocks_df):
        """Test Simple Moving Average."""
        sma = TechnicalIndicators.sma(sample_stocks_df['close'], window=20)
        
        assert isinstance(sma, pd.Series)
        assert len(sma) == len(sample_stocks_df)
        # First 19 values should be NaN
        assert pd.isna(sma.iloc[19])
        assert not pd.isna(sma.iloc[20])
    
    def test_ema(self, sample_stocks_df):
        """Test Exponential Moving Average."""
        ema = TechnicalIndicators.ema(sample_stocks_df['close'], span=12)
        
        assert isinstance(ema, pd.Series)
        assert len(ema) == len(sample_stocks_df)
        assert not pd.isna(ema.iloc[-1])
    
    def test_rsi(self, sample_stocks_df):
        """Test RSI calculation."""
        rsi = TechnicalIndicators.rsi(sample_stocks_df['close'], period=14)
        
        assert isinstance(rsi, pd.Series)
        # RSI should be between 0 and 100
        valid_rsi = rsi.dropna()
        assert all(valid_rsi >= 0)
        assert all(valid_rsi <= 100)
    
    def test_macd(self, sample_stocks_df):
        """Test MACD calculation."""
        macd_dict = TechnicalIndicators.macd(sample_stocks_df['close'])
        
        assert 'macd' in macd_dict
        assert 'macd_signal' in macd_dict
        assert 'macd_histogram' in macd_dict
        
        assert isinstance(macd_dict['macd'], pd.Series)
        assert len(macd_dict['macd']) == len(sample_stocks_df)
    
    def test_bollinger_bands(self, sample_stocks_df):
        """Test Bollinger Bands calculation."""
        bb_dict = TechnicalIndicators.bollinger_bands(sample_stocks_df['close'])
        
        assert 'bb_upper' in bb_dict
        assert 'bb_middle' in bb_dict
        assert 'bb_lower' in bb_dict
        
        # Upper should be > middle > lower
        bb_df = pd.DataFrame(bb_dict).dropna()
        assert all(bb_df['bb_upper'] >= bb_df['bb_middle'])
        assert all(bb_df['bb_middle'] >= bb_df['bb_lower'])
    
    def test_add_indicators(self, sample_stocks_df):
        """Test adding all indicators to DataFrame."""
        result = TechnicalIndicators.add_indicators(
            sample_stocks_df,
            sma_periods=[20, 50],
            ema_periods=[12, 26]
        )
        
        assert 'sma_20' in result.columns
        assert 'sma_50' in result.columns
        assert 'ema_12' in result.columns
        assert 'ema_26' in result.columns
        assert 'rsi_14' in result.columns
        assert 'macd' in result.columns
        assert 'bb_upper' in result.columns


class TestStockTransformationPipeline:
    """Test the complete stock transformation pipeline."""
    
    def test_full_transformation(self, sample_stocks_df):
        """Test full stock transformation pipeline."""
        pipeline = StockTransformationPipeline()
        
        result = pipeline.transform(sample_stocks_df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_stocks_df)
        
        # Check that transformations were applied
        assert 'simple_return' in result.columns
        assert 'log_return' in result.columns
        assert 'volatility_20d' in result.columns
        assert 'sma_20' in result.columns
        assert 'rsi_14' in result.columns
    
    def test_grouped_transformation(self):
        """Test transformation with multiple books."""
        # Create data with two books
        dates = pd.date_range('2025-01-01', periods=50)
        
        df = pd.DataFrame({
            'book': ['btc-usd'] * 50 + ['eth-usd'] * 50,
            'date': list(dates) * 2,
            'close': np.random.uniform(90, 110, 100),
            'open': np.random.uniform(90, 110, 100),
            'high': np.random.uniform(95, 115, 100),
            'low': np.random.uniform(85, 105, 100),
            'volume': np.random.randint(1000000, 5000000, 100)
        })
        
        pipeline = StockTransformationPipeline()
        result = pipeline.transform(df, group_by='book')
        
        assert len(result) == 100
        assert 'simple_return' in result.columns
        
        # Check both books were processed
        assert 'btc-usd' in result['book'].values
        assert 'eth-usd' in result['book'].values
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        pipeline = StockTransformationPipeline()
        
        empty_df = pd.DataFrame()
        result = pipeline.transform(empty_df)
        
        assert result.empty
    
    def test_get_feature_columns(self):
        """Test getting list of feature columns."""
        pipeline = StockTransformationPipeline()
        
        columns = pipeline.get_feature_columns()
        
        assert 'simple_return' in columns
        assert 'log_return' in columns
        assert 'volatility_20d' in columns
        assert 'sma_20' in columns


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_transform_stocks(self, sample_stocks_df):
        """Test quick transform function."""
        result = transform_stocks(sample_stocks_df)
        
        assert 'simple_return' in result.columns
        assert 'volatility_20d' in result.columns
        assert 'sma_20' in result.columns
    
    def test_calculate_risk_metrics(self, sample_stocks_df):
        """Test risk metrics calculation."""
        df = ReturnsCalculator.add_returns(sample_stocks_df)
        metrics = calculate_risk_metrics(df, window=50)
        
        assert 'mean_return' in metrics
        assert 'volatility' in metrics
        assert 'sharpe_ratio' in metrics
        assert 'max_drawdown' in metrics
        assert 'var_95' in metrics
        assert 'cvar_95' in metrics
        
        assert isinstance(metrics['sharpe_ratio'], float)
    
    def test_calculate_max_drawdown(self, sample_stocks_df):
        """Test maximum drawdown calculation."""
        max_dd = calculate_max_drawdown(sample_stocks_df, 'close')
        
        assert isinstance(max_dd, float)
        assert max_dd <= 0  # Drawdown should be negative or zero


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_single_row_dataframe(self):
        """Test transformation with single row."""
        df = pd.DataFrame({
            'book': ['btc-usd'],
            'date': [pd.Timestamp('2026-01-01')],
            'close': [100.0],
            'open': [99.0],
            'high': [101.0],
            'low': [98.0],
            'volume': [1000000]
        })
        
        pipeline = StockTransformationPipeline()
        result = pipeline.transform(df)
        
        # Should not raise error
        assert len(result) == 1
    
    def test_missing_price_columns(self):
        """Test handling when high/low columns are missing."""
        df = pd.DataFrame({
            'book': ['btc-usd'] * 10,
            'date': pd.date_range('2026-01-01', periods=10),
            'close': np.random.uniform(90, 110, 10),
            'volume': np.random.randint(1000000, 5000000, 10)
        })
        
        pipeline = StockTransformationPipeline()
        result = pipeline.transform(df)
        
        # Should still calculate returns and some indicators
        assert 'simple_return' in result.columns
        # Parkinson and GK volatility should not be present
        assert 'volatility_parkinson' not in result.columns
        assert 'volatility_gk' not in result.columns
