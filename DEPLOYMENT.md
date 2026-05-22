# Deployment Guide

This guide provides instructions for deploying the Financial Analysis Toolkit in production environments.

## Prerequisites

- Python 3.8 or higher
- PostgreSQL database
- AWS Account (for S3 storage, optional)
- Chrome/Firefox browser (for web scraping)
- Server with sufficient resources for your workload

## Environment Setup

### 1. Server Configuration

#### System Dependencies

For Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install -y python3.9 python3-pip python3-venv
sudo apt-get install -y postgresql-client
sudo apt-get install -y chromium-browser chromium-chromedriver
```

For CentOS/RHEL:
```bash
sudo yum install -y python39 python39-pip
sudo yum install -y postgresql
sudo yum install -y chromium chromium-headless
```

### 2. Application Installation

```bash
# Clone the repository
git clone <repository-url>
cd financial_analysis

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -e .

# Or install with production-only dependencies
pip install --no-dev -e .
```

### 3. Environment Variables

Create a `.env` file or set environment variables:

```bash
# Copy example file
cp env.example .env

# Edit with your production values
nano .env
```

For production, consider using:
- Environment variables set in your deployment system
- Secret management services (AWS Secrets Manager, HashiCorp Vault, etc.)
- Configuration management tools (Ansible, Puppet, etc.)

### 4. Database Setup

#### PostgreSQL Configuration

1. **Create database:**
   ```sql
   CREATE DATABASE financial_db;
   CREATE USER financial_user WITH PASSWORD 'secure_password';
   GRANT ALL PRIVILEGES ON DATABASE financial_db TO financial_user;
   ```

2. **Initialize tables:**
   ```python
   from Storage import PgConn
   from Storage import PostgresSQL_table_queries
   
   pg_conn = PgConn(
       tablename="stocks",
       dbname="financial_db",
       user="financial_user"
   )
   pg_conn.init_db(PostgresSQL_table_queries.STOCKS_TABLE_QUERY)
   ```

### 5. AWS S3 Setup (Optional)

1. **Create S3 bucket:**
   ```bash
   aws s3 mb s3://your-financial-data-bucket
   ```

2. **Configure IAM permissions:**
   - Create IAM user with S3 read/write permissions
   - Configure AWS credentials in `.env` file

3. **Set bucket policies** for appropriate access control

## Deployment Options

### Option 1: Standalone Server

#### Using systemd (Linux)

Create service file `/etc/systemd/system/financial-analysis.service`:

```ini
[Unit]
Description=Financial Analysis Toolkit
After=network.target postgresql.service

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/financial_analysis
Environment="PATH=/path/to/financial_analysis/venv/bin"
ExecStart=/path/to/financial_analysis/venv/bin/python -m your_module
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable financial-analysis
sudo systemctl start financial-analysis
sudo systemctl status financial-analysis
```

#### Using Supervisor

Create config file `/etc/supervisor/conf.d/financial-analysis.conf`:

```ini
[program:financial-analysis]
command=/path/to/financial_analysis/venv/bin/python -m your_module
directory=/path/to/financial_analysis
user=your-user
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/financial-analysis/app.log
```

Reload and start:
```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start financial-analysis
```

### Option 2: Docker Deployment

#### Create Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY pyproject.toml .
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV CHROMIUM_FLAGS="--no-sandbox --headless --disable-gpu"

# Run application
CMD ["python", "-m", "your_module"]
```

#### Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  app:
    build: .
    environment:
      - PGDBHOST=postgres
      - PGDBNAME=financial_db
      - PGDBUSER=financial_user
      - PGDBPASS=${DB_PASSWORD}
    depends_on:
      - postgres
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data

  postgres:
    image: postgres:15
    environment:
      - POSTGRES_DB=financial_db
      - POSTGRES_USER=financial_user
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Deploy:
```bash
docker-compose up -d
```

### Option 3: Cloud Deployment

#### AWS EC2 / Elastic Beanstalk

1. **Prepare deployment package:**
   ```bash
   zip -r deployment.zip . -x "*.git*" "*__pycache__*" "*.pyc" "*.ipynb*"
   ```

2. **Deploy using EB CLI:**
   ```bash
   eb init
   eb create production
   eb deploy
   ```

#### Google Cloud Platform

1. **Use Cloud Run:**
   ```bash
   gcloud run deploy financial-analysis \
     --source . \
     --platform managed \
     --region us-central1
   ```

#### Heroku

1. **Create Procfile:**
   ```
   web: python -m your_module
   worker: python -m your_module --worker
   ```

2. **Deploy:**
   ```bash
   heroku create your-app-name
   heroku config:set PGDBHOST=...
   git push heroku main
   ```

## Monitoring and Logging

### Log Management

Logs are written to:
- Console (stdout/stderr)
- File: `logs/app.log` (rotating, configurable)

For production, consider:
- Centralized logging (ELK stack, Splunk, CloudWatch)
- Log aggregation services
- Log rotation and retention policies

### Health Checks

Create a health check endpoint or script:

```python
def health_check():
    """Check system health."""
    checks = {
        "database": check_database(),
        "aws": check_aws_connection(),
        "disk_space": check_disk_space(),
    }
    return all(checks.values())
```

### Monitoring

Consider monitoring:
- Application metrics (CPU, memory, response times)
- Database connection pool status
- Error rates and exceptions
- Scheduled job execution status
- Resource utilization

Tools:
- Prometheus + Grafana
- DataDog
- New Relic
- AWS CloudWatch
- Application Insights

## Security Considerations

1. **Secrets Management:**
   - Never commit `.env` files
   - Use secret management services
   - Rotate credentials regularly

2. **Database Security:**
   - Use strong passwords
   - Enable SSL/TLS connections
   - Restrict network access
   - Regular backups

3. **Application Security:**
   - Keep dependencies updated
   - Run security scans (safety, bandit)
   - Use HTTPS for all external communications
   - Implement rate limiting

4. **AWS Security:**
   - Use IAM roles instead of access keys when possible
   - Enable MFA
   - Follow least privilege principle
   - Enable CloudTrail logging

## Backup and Recovery

1. **Database Backups:**
   ```bash
   # Manual backup
   pg_dump -U financial_user financial_db > backup_$(date +%Y%m%d).sql
   
   # Automated backups (cron)
   0 2 * * * /path/to/backup-script.sh
   ```

2. **Application Data Backups:**
   - Backup S3 buckets regularly
   - Snapshot application volumes
   - Document recovery procedures

## Scaling

For high-load scenarios:

1. **Horizontal Scaling:**
   - Deploy multiple instances
   - Use load balancer
   - Implement session/state management

2. **Database Scaling:**
   - Read replicas for read-heavy workloads
   - Connection pooling (pgBouncer)
   - Query optimization

3. **Caching:**
   - Redis for frequently accessed data
   - Application-level caching
   - CDN for static assets

## Maintenance

1. **Updates:**
   - Test updates in staging environment
   - Create rollback plan
   - Schedule maintenance windows
   - Monitor after deployment

2. **Dependency Updates:**
   ```bash
   pip list --outdated
   pip install --upgrade package-name
   ```

3. **Database Maintenance:**
   - Regular VACUUM and ANALYZE
   - Monitor table sizes
   - Index optimization

## Troubleshooting

Common issues and solutions:

1. **Database Connection Issues:**
   - Check credentials and network connectivity
   - Verify PostgreSQL service status
   - Check firewall rules

2. **Selenium/Chrome Issues:**
   - Ensure Chrome/Chromium is installed
   - Check headless mode configuration
   - Verify driver compatibility

3. **Permission Issues:**
   - Check file/directory permissions
   - Verify user has necessary access
   - Check SELinux/AppArmor policies

## Support

For deployment issues:
- Check logs: `tail -f logs/app.log`
- Review error messages
- Consult documentation
- Open an issue on GitHub

