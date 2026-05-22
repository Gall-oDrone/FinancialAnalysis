# PostgreSQL initialization

Scripts in this directory run **once** when the `postgres` volume is first created (`docker/init-db` is mounted to `/docker-entrypoint-initdb.d`).

**Single source of truth:** `01-init.sql`

To reset the database:

```bash
docker compose down
docker volume rm financial_postgres_data
docker compose up -d postgres
```

Schema should stay aligned with `Storage/PostgresSQL_table_queries.py`.
