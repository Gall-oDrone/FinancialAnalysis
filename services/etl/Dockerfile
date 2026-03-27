# Financial Analysis — ETL / transforms / GenAI export (no browser)
# Build from repository root: docker build -f services/etl/Dockerfile .

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/src

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 postgresql-client build-essential gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml ./
COPY src ./src
COPY Storage ./Storage
COPY config ./config

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -e ".[ml]"

# NLP assets (optional; may fail offline — keep image useful for non-NLP commands)
RUN python -m spacy download en_core_web_sm 2>/dev/null || true \
    && python -m nltk.downloader punkt stopwords vader_lexicon 2>/dev/null || true

RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /app/logs /app/data \
    && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "-m", "pipelines.etl_cli"]
CMD ["--help"]
