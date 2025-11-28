"""
Pipeline Usage Examples

This script demonstrates various ways to use the DataPipeline for:
1. Full pipeline execution (Scrape → S3 → Transform → S3)
2. Transform-only on existing data
3. Custom pipeline configurations
4. Working with individual transformers

Before running, ensure you have:
1. Set up your .env file with AWS and DB credentials
2. Installed dependencies: pip install -r requirements.txt
3. Downloaded NLP models: python -m spacy download en_core_web_sm
"""

import pandas as pd
from datetime import datetime

# Add project root to path
import sys
sys.path.insert(0, '.')


def example_1_full_pipeline():
    """
    Example 1: Run the complete pipeline
    
    Scrape → Upload Raw to S3 → Transform → Upload Transformed to S3
    """
    print("\n" + "="*60)
    print("Example 1: Full Pipeline Execution")
    print("="*60)
    
    from DataProcessing.pipeline import DataPipeline, PipelineConfig, PipelineStage
    
    config = PipelineConfig(
        # Scraping settings
        topics=["crypto"],
        enrich_full_content=True,
        
        # S3 settings (set to None to skip S3 uploads)
        s3_bucket="your-bucket-name",  # Replace with your bucket
        s3_raw_prefix="raw/news",
        s3_transformed_prefix="transformed/news",
        
        # Transformation settings
        enable_transformations=True,
        sentiment_backend="vader",  # Fast, rule-based
        keyword_method="tfidf",
        keyword_top_n=10,
        
        # Database settings
        save_to_db=False,
        
        # Pipeline stages to execute
        stages=[
            PipelineStage.SCRAPE,
            PipelineStage.UPLOAD_RAW,
            PipelineStage.TRANSFORM,
            PipelineStage.UPLOAD_TRANSFORMED,
        ]
    )
    
    pipeline = DataPipeline(config)
    result = pipeline.run()
    
    print(f"\nPipeline completed: {'✓' if result.success else '✗'}")
    print(f"Articles processed: {result.articles_scraped}")
    print(f"Execution time: {result.execution_time_seconds}s")
    
    if result.errors:
        print(f"Errors: {result.errors}")
    
    return result


def example_2_transform_existing_data():
    """
    Example 2: Transform existing data without scraping
    
    Useful when you already have news data in a DataFrame or database
    """
    print("\n" + "="*60)
    print("Example 2: Transform Existing Data")
    print("="*60)
    
    from DataProcessing.text_transformers import TextTransformationPipeline
    
    # Sample data (replace with your actual data)
    sample_data = pd.DataFrame([
        {
            "id": 1,
            "source": "Yahoo Finance",
            "headline": "Bitcoin Surges Past $100,000 as Institutional Adoption Grows",
            "summary": "Bitcoin reached a new all-time high today as major institutions announce crypto investments.",
            "content": """Bitcoin surged past the $100,000 mark today, setting a new all-time high 
            as institutional adoption continues to accelerate. Major financial institutions including 
            Goldman Sachs and Morgan Stanley announced significant cryptocurrency investment products. 
            Analysts predict the rally could continue as regulatory clarity improves and the SEC 
            considers approving more spot Bitcoin ETFs. The cryptocurrency market cap has now 
            exceeded $3 trillion, with Bitcoin dominance at 52%.""",
            "datetime": "2024-01-15T10:30:00Z"
        },
        {
            "id": 2,
            "source": "CoinDesk",
            "headline": "SEC Delays Decision on Ethereum ETF Applications",
            "summary": "The Securities and Exchange Commission has postponed its ruling on multiple Ethereum ETF proposals.",
            "content": """The SEC announced today that it will delay its decision on several pending 
            Ethereum ETF applications. The delay affects applications from major asset managers 
            including BlackRock and Fidelity. Market analysts suggest the delay could impact 
            Ethereum prices in the short term, though long-term sentiment remains bullish. 
            The regulatory uncertainty continues to be a concern for crypto investors.""",
            "datetime": "2024-01-15T14:45:00Z"
        },
        {
            "id": 3,
            "source": "Bloomberg",
            "headline": "DeFi Protocol Suffers $50M Hack in Smart Contract Exploit",
            "summary": "A major DeFi platform lost millions in a security breach targeting its lending contracts.",
            "content": """A prominent DeFi lending protocol suffered a devastating $50 million hack 
            today due to a vulnerability in its smart contracts. Security researchers identified 
            the exploit as a flash loan attack that manipulated the protocol's price oracles. 
            This marks the third major DeFi hack this month, raising concerns about smart contract 
            security. Users are advised to revoke token approvals for the affected contracts.""",
            "datetime": "2024-01-15T09:15:00Z"
        }
    ])
    
    # Initialize transformation pipeline
    transform_pipeline = TextTransformationPipeline(
        sentiment_backend="vader",
        use_transformer_intents=False,  # Use rule-based for speed
        keyword_method="tfidf",
        keyword_top_n=10
    )
    
    # Transform the data
    transformed_df = transform_pipeline.transform(sample_data)
    
    # Display results
    print("\nTransformation Results:")
    print("-" * 40)
    
    for idx, row in transformed_df.iterrows():
        print(f"\nArticle {idx + 1}: {row['headline'][:50]}...")
        print(f"  Sentiment: {row.get('sentiment_label', 'N/A')} (score: {row.get('sentiment_score', 0):.3f})")
        print(f"  Intent: {row.get('primary_intent', 'N/A')} (confidence: {row.get('intent_confidence', 0):.3f})")
        print(f"  Keywords: {row.get('keywords', [])[:5]}")
        print(f"  Word count: {row.get('word_count', 0)}")
    
    return transformed_df


def example_3_individual_transformers():
    """
    Example 3: Use individual transformers for specific tasks
    
    Useful when you only need sentiment, keywords, or intents
    """
    print("\n" + "="*60)
    print("Example 3: Individual Transformers")
    print("="*60)
    
    from DataProcessing.text_transformers import (
        SentimentAnalyzer, 
        IntentExtractor, 
        KeywordExtractor
    )
    
    text = """
    Bitcoin reached a new all-time high of $100,000 today following news that 
    the SEC approved the first spot Bitcoin ETF. Institutional investors are 
    expected to pour billions into the cryptocurrency market. Analysts at 
    Goldman Sachs predict Bitcoin could reach $150,000 by year end.
    """
    
    # Sentiment Analysis
    print("\n1. Sentiment Analysis")
    print("-" * 30)
    
    sentiment_analyzer = SentimentAnalyzer(backend="vader")
    sentiment_result = sentiment_analyzer.transform(text)
    
    print(f"Label: {sentiment_result.label.value}")
    print(f"Compound Score: {sentiment_result.score:.3f}")
    print(f"Positive: {sentiment_result.positive_score:.3f}")
    print(f"Negative: {sentiment_result.negative_score:.3f}")
    print(f"Neutral: {sentiment_result.neutral_score:.3f}")
    
    # Intent Extraction
    print("\n2. Intent Extraction")
    print("-" * 30)
    
    intent_extractor = IntentExtractor(use_transformers=False)
    intent_result = intent_extractor.transform(text)
    
    print(f"Primary Intent: {intent_result.primary_intent.value}")
    print(f"Confidence: {intent_result.confidence:.3f}")
    print(f"Secondary Intents: {intent_result.secondary_intents}")
    
    # Keyword Extraction
    print("\n3. Keyword Extraction")
    print("-" * 30)
    
    keyword_extractor = KeywordExtractor(method="tfidf", top_n=10)
    keyword_result = keyword_extractor.transform(text)
    
    print(f"Keywords: {keyword_result.keywords}")
    print(f"Top scores: {dict(list(keyword_result.keyword_scores.items())[:5])}")


def example_4_ml_features():
    """
    Example 4: Extract ML-ready numerical features
    
    Useful for training ML models on transformed data
    """
    print("\n" + "="*60)
    print("Example 4: Extract ML Features")
    print("="*60)
    
    from DataProcessing.text_transformers import TextTransformationPipeline
    
    # Sample articles
    articles = [
        {"id": 1, "headline": "Bitcoin hits new high", "content": "Bitcoin surged to record levels today..."},
        {"id": 2, "headline": "Market crash fears", "content": "Investors are worried about a potential crash..."},
        {"id": 3, "headline": "New crypto regulations", "content": "The SEC announced new rules for crypto exchanges..."},
    ]
    
    pipeline = TextTransformationPipeline()
    
    # Get ML-ready features
    features_df = pipeline.get_ml_features(articles)
    
    print("\nML Features DataFrame:")
    print(features_df.to_string())
    
    # Show feature columns
    print(f"\nFeature columns: {list(features_df.columns)}")
    
    return features_df


def example_5_database_integration():
    """
    Example 5: Transform and save to PostgreSQL
    
    Demonstrates the full flow with database persistence
    """
    print("\n" + "="*60)
    print("Example 5: Database Integration")
    print("="*60)
    
    from DataProcessing.pipeline import DataPipeline, PipelineConfig, PipelineStage
    from Storage.pgConn import PgConn
    from Storage import PostgresSQL_table_queries
    
    # Initialize database
    db_conn = PgConn()
    db_conn.set_table(PostgresSQL_table_queries.FINANCIAL_NEWS_TRANSFORMED_TABLE_NAME)
    db_conn.init_db(PostgresSQL_table_queries.FINANCIAL_NEWS_TRANSFORMED_TABLE_QUERY)
    
    # Configure pipeline with database stage
    config = PipelineConfig(
        topics=["crypto"],
        s3_bucket=None,  # Skip S3 for this example
        enable_transformations=True,
        save_to_db=True,
        db_table_name=PostgresSQL_table_queries.FINANCIAL_NEWS_TRANSFORMED_TABLE_NAME,
        stages=[
            PipelineStage.SCRAPE,
            PipelineStage.TRANSFORM,
            PipelineStage.SAVE_DB,
        ]
    )
    
    pipeline = DataPipeline(config)
    result = pipeline.run()
    
    print(f"\nPipeline completed: {'✓' if result.success else '✗'}")
    print(f"Articles saved to DB: {result.articles_transformed}")
    
    db_conn.close_connection()
    
    return result


def example_6_custom_sentiment_backend():
    """
    Example 6: Use different sentiment analysis backends
    
    Compare VADER, TextBlob, and Transformers (if installed)
    """
    print("\n" + "="*60)
    print("Example 6: Compare Sentiment Backends")
    print("="*60)
    
    from DataProcessing.text_transformers import SentimentAnalyzer
    
    text = "Bitcoin crashed 20% today amid market panic and regulatory concerns."
    
    backends = ["vader", "textblob"]
    
    # Try to add transformers if available
    try:
        import transformers
        backends.append("transformers")
    except ImportError:
        print("Note: transformers not installed, skipping that backend")
    
    print(f"\nTest text: '{text[:60]}...'\n")
    
    for backend in backends:
        try:
            analyzer = SentimentAnalyzer(backend=backend)
            result = analyzer.transform(text)
            print(f"{backend.upper():12} | Label: {result.label.value:8} | Score: {result.score:+.3f}")
        except Exception as e:
            print(f"{backend.upper():12} | Error: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline Usage Examples")
    parser.add_argument(
        "--example", 
        type=int, 
        choices=[1, 2, 3, 4, 5, 6],
        help="Run specific example (1-6)"
    )
    args = parser.parse_args()
    
    examples = {
        1: example_1_full_pipeline,
        2: example_2_transform_existing_data,
        3: example_3_individual_transformers,
        4: example_4_ml_features,
        5: example_5_database_integration,
        6: example_6_custom_sentiment_backend,
    }
    
    if args.example:
        examples[args.example]()
    else:
        # Run examples 2, 3, 4, 6 (don't require scraping or DB)
        print("Running safe examples (no scraping or DB required)...\n")
        example_2_transform_existing_data()
        example_3_individual_transformers()
        example_4_ml_features()
        example_6_custom_sentiment_backend()

