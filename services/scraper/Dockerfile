# Financial Analysis — web scraping microservice (Selenium + Chromium)
# Build from repository root: docker build -f services/scraper/Dockerfile .

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/WebScraping/src \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 postgresql-client \
    chromium chromium-driver \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libdbus-1-3 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libpango-1.0-0 libcairo2 fonts-liberation wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY services/scraper/requirements.txt /tmp/requirements-scraper.txt
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r /tmp/requirements-scraper.txt

COPY config ./config
COPY WebScraping ./WebScraping
COPY Storage ./Storage
COPY utils ./utils
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
    && mkdir -p /app/logs /app/data \
    && useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "WebScraping/src/collectors/news_collector_example.py"]
