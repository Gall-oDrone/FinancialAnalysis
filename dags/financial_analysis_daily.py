"""
Airflow DAG for daily Financial Analysis data pipeline.

Option B orchestration model:
- Airflow schedules and orchestrates scraper + ETL tasks.
- KubernetesPodOperator runs each task in the existing workload images.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


APP_NAMESPACE = "financial-analysis-dev"
SCRAPER_IMAGE = "326105557351.dkr.ecr.us-east-1.amazonaws.com/financial-analysis-scraper:latest"
ETL_IMAGE = "326105557351.dkr.ecr.us-east-1.amazonaws.com/financial-analysis-etl:latest"

COMMON_ENV_FROM = [
    k8s.V1EnvFromSource(
        config_map_ref=k8s.V1ConfigMapEnvSource(name="financial-analysis-config")
    ),
    k8s.V1EnvFromSource(
        secret_ref=k8s.V1SecretEnvSource(name="financial-analysis-secrets")
    ),
]

default_args = {
    "owner": "financial-analysis",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="financial_analysis_daily",
    description="Daily orchestration for scraping and ETL transforms",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    tags=["financial-analysis", "scraping", "etl", "daily"],
) as dag:
    scrape_news = KubernetesPodOperator(
        task_id="scrape_news",
        name="scrape-news",
        namespace=APP_NAMESPACE,
        image=SCRAPER_IMAGE,
        cmds=["python"],
        arguments=["WebScraping/src/collectors/news_collector_example.py"],
        env_from=COMMON_ENV_FROM,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    scrape_stocks = KubernetesPodOperator(
        task_id="scrape_stocks",
        name="scrape-stocks",
        namespace=APP_NAMESPACE,
        image=SCRAPER_IMAGE,
        cmds=["python", "-c"],
        arguments=[
            (
                "from WebScraping.src.scrapers.WebScraper import StocksScrapper; "
                "s = StocksScrapper(debug=False, keepBrowserOpen=False); "
                "s.startScrapping()"
            )
        ],
        env_from=COMMON_ENV_FROM,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    transform_news = KubernetesPodOperator(
        task_id="transform_news",
        name="transform-news",
        namespace=APP_NAMESPACE,
        image=ETL_IMAGE,
        cmds=["python", "-m", "pipelines.etl_cli"],
        arguments=["transform-news", "--date", "{{ ds }}"],
        env_from=COMMON_ENV_FROM,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    transform_stocks = KubernetesPodOperator(
        task_id="transform_stocks",
        name="transform-stocks",
        namespace=APP_NAMESPACE,
        image=ETL_IMAGE,
        cmds=["python", "-m", "pipelines.etl_cli"],
        arguments=["transform-stocks", "--since", "{{ ds }}", "--until", "{{ ds }}"],
        env_from=COMMON_ENV_FROM,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    scrape_news >> transform_news
    scrape_stocks >> transform_stocks
