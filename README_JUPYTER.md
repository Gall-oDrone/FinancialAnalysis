# Running Jupyter Notebook in Docker Container

## Quick Start

### Option 1: Using the helper script (Recommended)

**Windows PowerShell:**
```powershell
docker-compose run --rm -p 8888:8888 -v "${PWD}/WebScraping:/app/WebScraping" -v "${PWD}/Storage:/app/Storage" -v "${PWD}/logs:/app/logs" -v "${PWD}/data:/app/data" -e PYTHONPATH=/app/Storage scraper bash -c "cd /app && pip install jupyter -q && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/app --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'"
```

**With Token Authentication (More Secure):**
```powershell
$token = -join ((48..57) + (65..90) + (97..122) | Get-Random -Count 32 | ForEach-Object {[char]$_})
docker-compose run --rm -p 8888:8888 -v "${PWD}/WebScraping:/app/WebScraping" -v "${PWD}/Storage:/app/Storage" -v "${PWD}/logs:/app/logs" -v "${PWD}/data:/app/data" -e PYTHONPATH=/app/Storage scraper bash -c "cd /app && pip install jupyter -q && echo 'Access Jupyter at: http://localhost:8888/?token=$token' && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/app --NotebookApp.token='$token'"
```

### Option 2: Manual Command

1. Start Jupyter server:
```powershell
docker-compose run --rm `
  -p 8888:8888 `
  -v "${PWD}/WebScraping:/app/WebScraping" `
  -v "${PWD}/Storage:/app/Storage" `
  -v "${PWD}/logs:/app/logs" `
  -v "${PWD}/data:/app/data" `
  -e PYTHONPATH=/app/Storage `
  scraper bash -c "cd /app && pip install jupyter -q && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --notebook-dir=/app --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'"
```

2. Open your browser and navigate to: `http://localhost:8888`

## Configuration Details

- **Port**: 8888 (mapped from container to host)
- **Notebook Directory**: `/app` (container root)
- **Python Path**: Automatically configured for:
  - `/app/WebScraping/src` (for YahooFinanceHTMLElements)
  - `/app/Storage` (for pgConn)
  - `/app` (root directory)

## Important Notes

1. **Port Forwarding**: The `-p 8888:8888` maps port 8888 from the container to your host machine
2. **Volume Mounts**: All necessary directories are mounted so changes persist
3. **Python Path**: PYTHONPATH is set so imports work correctly
4. **Security**: The no-token version is convenient but less secure. Use token version for production

## Accessing Your Notebooks

Once Jupyter is running, you can:
- Navigate to `http://localhost:8888` in your browser
- Open `WebScraping/notebooks/NewsCollector-Staging.ipynb`
- All imports will work because PYTHONPATH is configured
- Database connections will work (PostgreSQL is in the same network)

## Stopping Jupyter

Press `Ctrl+C` in the terminal where Jupyter is running, or stop the container.

