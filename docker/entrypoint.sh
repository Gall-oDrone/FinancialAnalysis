#!/bin/bash
set -e

# =============================================================================
# Financial Analysis - Docker Entrypoint Script
# =============================================================================

echo "========================================"
echo "Financial Analysis Container Starting"
echo "========================================"

# -----------------------------------------------------------------------------
# Wait for PostgreSQL to be ready
# -----------------------------------------------------------------------------
wait_for_postgres() {
    echo "Waiting for PostgreSQL to be ready..."
    
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if pg_isready -h "${PGDBHOST:-postgres}" -p "${PGDBPORT:-5432}" -U "${PGDBUSER:-financial_user}" > /dev/null 2>&1; then
            echo "PostgreSQL is ready!"
            return 0
        fi
        
        echo "Attempt $attempt/$max_attempts: PostgreSQL not ready yet, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: PostgreSQL did not become ready in time"
    return 1
}

# -----------------------------------------------------------------------------
# Create logs directory if not exists
# -----------------------------------------------------------------------------
setup_directories() {
    echo "Setting up directories..."
    mkdir -p /app/logs
    mkdir -p /app/data
    echo "Directories created."
}

# -----------------------------------------------------------------------------
# Verify Chrome/Chromium installation
# -----------------------------------------------------------------------------
verify_chrome() {
    echo "Verifying Chrome installation..."
    
    if command -v chromium &> /dev/null; then
        CHROME_VERSION=$(chromium --version)
        echo "Chrome found: $CHROME_VERSION"
    elif command -v chromium-browser &> /dev/null; then
        CHROME_VERSION=$(chromium-browser --version)
        echo "Chrome found: $CHROME_VERSION"
    elif command -v google-chrome &> /dev/null; then
        CHROME_VERSION=$(google-chrome --version)
        echo "Chrome found: $CHROME_VERSION"
    else
        echo "WARNING: Chrome/Chromium not found in expected locations"
    fi
    
    if command -v chromedriver &> /dev/null; then
        DRIVER_VERSION=$(chromedriver --version)
        echo "ChromeDriver found: $DRIVER_VERSION"
    else
        echo "WARNING: ChromeDriver not found"
    fi
}

# -----------------------------------------------------------------------------
# Display environment info
# -----------------------------------------------------------------------------
show_environment() {
    echo "----------------------------------------"
    echo "Environment Configuration:"
    echo "----------------------------------------"
    echo "ENVIRONMENT: ${ENVIRONMENT:-development}"
    echo "DEBUG: ${DEBUG:-False}"
    echo "PGDBHOST: ${PGDBHOST:-postgres}"
    echo "PGDBPORT: ${PGDBPORT:-5432}"
    echo "PGDBNAME: ${PGDBNAME:-financial_db}"
    echo "SCRAPING_HEADLESS: ${SCRAPING_HEADLESS:-True}"
    echo "LOG_LEVEL: ${LOG_LEVEL:-INFO}"
    echo "----------------------------------------"
}

# -----------------------------------------------------------------------------
# Main execution
# -----------------------------------------------------------------------------
main() {
    setup_directories
    show_environment
    verify_chrome
    
    # Wait for database if not in test mode
    if [ "${SKIP_DB_WAIT:-false}" != "true" ]; then
        wait_for_postgres
    fi
    
    echo "========================================"
    echo "Initialization complete. Starting app..."
    echo "========================================"
    
    # Execute the command passed to docker run
    exec "$@"
}

# Run main function
main "$@"
