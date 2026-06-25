# PostgreSQL initialization

Scripts in this directory run **once** when the `postgres` volume is first created (`docker/init-db` is mounted to `/docker-entrypoint-initdb.d`).

**Single source of truth:** `01-init.sql`

To reset the database:

```bash
docker compose down
docker volume rm financial_postgres_data
docker compose up -d postgres
```

## Backup and restore (S3)

Before tearing down an EC2 environment, snapshot the live database to S3:

```bash
./scripts/backup_postgres_to_s3.sh
./scripts/list_postgres_backups.sh
```

On a fresh machine (after `docker compose up -d postgres`):

```bash
./scripts/restore_postgres_from_s3.sh
```

Configure in `.env`:

- `AWS_BACKUP_BUCKET` (or `AWS_DEFAULT_BUCKET`)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- Optional: `POSTGRES_BACKUP_S3_PREFIX` (default: `postgres-backups/financial_db`)

Schema should stay aligned with `Storage/PostgresSQL_table_queries.py`.
