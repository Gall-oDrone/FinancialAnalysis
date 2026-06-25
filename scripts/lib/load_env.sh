# Shared helpers for bash scripts. Source from repo scripts:
#   ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
#   # shellcheck disable=SC1091
#   source "$ROOT/scripts/lib/load_env.sh"
#   load_env_file "$ROOT/.env"

load_env_file() {
  local env_file="${1:-}"
  [[ -n "$env_file" && -f "$env_file" ]] || return 0

  local keys=(
    AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
    AWS_DEFAULT_BUCKET AWS_BACKUP_BUCKET POSTGRES_BACKUP_S3_PREFIX
    POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD
    PGDBNAME PGDBUSER PGDBPASS
  )

  local key line val
  for key in "${keys[@]}"; do
    line="$(grep -E "^${key}=" "$env_file" | tail -1 || true)"
    [[ -n "$line" ]] || continue
    val="${line#*=}"
    val="${val%$'\r'}"
    if [[ "$val" == \"*\" && "$val" == *\" ]]; then
      val="${val:1:${#val}-2}"
    fi
    export "${key}=${val}"
  done
}
