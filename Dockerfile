# =============================================================================
# Financial Analysis - Dockerfile
# =============================================================================
# Multi-stage build for Selenium + Python web scraping application
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Base image with Python and Chrome
# -----------------------------------------------------------------------------
FROM python:3.11-slim as base

# Prevent Python from writing bytecode and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies including Chrome/Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials
    build-essential \
    gcc \
    # PostgreSQL client
    libpq-dev \
    postgresql-client \
    # Chrome dependencies
    chromium \
    chromium-driver \
    # Additional dependencies for Chrome
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    # Fonts
    fonts-liberation \
    fonts-noto-color-emoji \
    # Utilities
    wget \
    curl \
    gnupg \
    ca-certificates \
    # Clean up
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Set Chrome environment variables
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    DISPLAY=:99

# Verify Chrome installation
RUN chromium --version && chromedriver --version

# -----------------------------------------------------------------------------
# Stage 2: Dependencies installation
# -----------------------------------------------------------------------------
FROM base as dependencies

# Copy dependency files
COPY requirements.txt pyproject.toml ./

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt \
    && pip install jupyter nbconvert ipykernel papermill

# -----------------------------------------------------------------------------
# Stage 3: Production image
# -----------------------------------------------------------------------------
FROM dependencies as production

# Create non-root user for security
RUN groupadd --gid 1000 appgroup \
    && useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser

# Create necessary directories
RUN mkdir -p /app/logs /app/data \
    && chown -R appuser:appgroup /app

# Copy application code
COPY --chown=appuser:appgroup . .

# Copy and set up entrypoint script
COPY --chown=appuser:appgroup docker/entrypoint.sh /entrypoint.sh
COPY --chown=appuser:appgroup docker/healthcheck.py /healthcheck.py
RUN sed -i 's/\r$//' /entrypoint.sh \
    && chmod +x /entrypoint.sh /healthcheck.py

# Install the package
RUN pip install --no-cache-dir .

# Switch to non-root user
USER appuser

# Expose port (if needed for future API)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python /healthcheck.py || exit 1

# Set entrypoint and default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-c", "print('Financial Analysis container ready. Override CMD to run your script.')"]

# -----------------------------------------------------------------------------
# Stage 4: Development image (optional)
# -----------------------------------------------------------------------------
FROM production as development

# Switch back to root to install dev dependencies
USER root

# Install development dependencies
RUN pip install -e ".[dev]"

# Install additional development tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    vim \
    less \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to non-root user
USER appuser

# Override CMD for development
CMD ["bash"]
