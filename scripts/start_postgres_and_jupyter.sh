#!/usr/bin/env bash
# =============================================================================
# Start the Postgres container and the Jupyter notebook server (Docker).
#
# Usage:
#   ./scripts/start_postgres_and_jupyter.sh            # postgres + jupyter
#   ./scripts/start_postgres_and_jupyter.sh --rebuild  # force rebuild of the scraper image
#   ./scripts/start_postgres_and_jupyter.sh --no-build # never build, fail if image is missing
#   ./scripts/start_postgres_and_jupyter.sh --pgadmin  # also start pgAdmin (port 5050)
#
# Steps (each is checked; on failure the script prints the likely root cause):
#   1. Pre-flight: docker daemon, compose CLI, buildx version, port 8888 free
#   2. docker compose up -d postgres  (waits for the healthcheck)
#   3. Build the scraper image (Jupyter runs inside it)
#   4. ./docker/start_jupyter.sh      (foreground; Ctrl+C stops it)
#
# Press Ctrl+C to stop Jupyter. Postgres keeps running; stop it with:
#   docker compose down
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

JUPYTER_PORT="${JUPYTER_PORT:-8888}"
POSTGRES_WAIT_SECONDS="${POSTGRES_WAIT_SECONDS:-90}"
MIN_BUILDX="0.17.0"

REBUILD=false
NO_BUILD=false
WITH_PGADMIN=false
for arg in "$@"; do
  case "$arg" in
    --rebuild)  REBUILD=true ;;
    --no-build) NO_BUILD=true ;;
    --pgadmin)  WITH_PGADMIN=true ;;
    -h|--help)  sed -n '2,19p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "Unknown option: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# -----------------------------------------------------------------------------
# Output helpers
# -----------------------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_BLU=$'\033[34m'; C_OFF=$'\033[0m'
else
  C_RED=""; C_GRN=""; C_YEL=""; C_BLU=""; C_OFF=""
fi
step()  { echo; echo "${C_BLU}==> $*${C_OFF}"; }
ok()    { echo "${C_GRN}  OK${C_OFF}  $*"; }
warn()  { echo "${C_YEL}  WARN${C_OFF} $*" >&2; }
fail() {
  # fail "<what failed>" "<likely cause / how to fix>" ...
  local what="$1"; shift
  echo >&2
  echo "${C_RED}ERROR: ${what}${C_OFF}" >&2
  if (($#)); then
    echo "  Possible root causes / fixes:" >&2
    local hint
    for hint in "$@"; do echo "    - ${hint}" >&2; done
  fi
  exit 1
}

# Print the last lines of a container's logs to help diagnose failures.
show_logs() {
  local name="$1" lines="${2:-40}"
  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    echo "  --- last ${lines} log lines from ${name} ---" >&2
    docker logs --tail "$lines" "$name" >&2 2>&1 || true
    echo "  --------------------------------------------" >&2
  fi
}

version_ge() { # version_ge A B -> true if A >= B
  [[ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" == "$2" ]]
}

# -----------------------------------------------------------------------------
# 1. Pre-flight checks
# -----------------------------------------------------------------------------
step "Pre-flight checks"

command -v docker >/dev/null 2>&1 \
  || fail "docker CLI not found" \
       "Install Docker: https://docs.docker.com/engine/install/"

if ! docker info >/dev/null 2>&1; then
  fail "Cannot talk to the Docker daemon" \
    "Daemon not running: sudo systemctl start docker" \
    "Permission denied on /var/run/docker.sock: sudo usermod -aG docker \$USER, then log out/in" \
    "Check: docker info"
fi
ok "Docker daemon reachable ($(docker version --format '{{.Server.Version}}' 2>/dev/null || echo '?'))"

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose)
else
  fail "Neither 'docker compose' (plugin) nor 'docker-compose' is installed" \
    "Install the compose plugin: https://docs.docker.com/compose/install/" \
    "Or the standalone binary to /usr/local/bin/docker-compose"
fi
ok "Compose: ${DC[*]} ($("${DC[@]}" version --short 2>/dev/null || echo '?'))"

# Compose v2.3x+/v5 builds through buildx and requires buildx >= 0.17.0.
BUILDX_VER="$(docker buildx version 2>/dev/null | sed -nE 's/.*v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/p' | head -n1 || true)"
if [[ -z "$BUILDX_VER" ]]; then
  warn "docker buildx plugin not found; image build may fail."
elif ! version_ge "$BUILDX_VER" "$MIN_BUILDX"; then
  warn "docker buildx ${BUILDX_VER} is older than ${MIN_BUILDX}; compose build will fail with 'compose build requires buildx ${MIN_BUILDX} or later'."
  echo "       Fix (user-local, no sudo):" >&2
  echo "         mkdir -p ~/.docker/cli-plugins && \\" >&2
  echo "         curl -fsSL -o ~/.docker/cli-plugins/docker-buildx \\" >&2
  echo "           https://github.com/docker/buildx/releases/latest/download/buildx-v0.38.0.linux-amd64 && \\" >&2
  echo "         chmod +x ~/.docker/cli-plugins/docker-buildx" >&2
else
  ok "buildx ${BUILDX_VER}"
fi

[[ -f "$ROOT/docker-compose.yml" ]] \
  || fail "docker-compose.yml not found in ${ROOT}" "Run this script from a checkout of the FinancialAnalysis repo"
[[ -x "$ROOT/docker/start_jupyter.sh" ]] || chmod +x "$ROOT/docker/start_jupyter.sh" 2>/dev/null \
  || fail "docker/start_jupyter.sh is missing or not executable" "chmod +x docker/start_jupyter.sh"

if [[ -f "$ROOT/.env" ]]; then
  ok ".env found"
else
  warn ".env not found. Using compose defaults (financial_db / financial_user). AWS-backed notebook cells will fail."
  echo "       Create it with: cp env.example .env" >&2
fi

# Port 8888 must be free; a stale Jupyter container is the usual culprit.
PORT_USER="$(docker ps --filter "publish=${JUPYTER_PORT}" --format '{{.Names}}' | head -n1 || true)"
if [[ -n "$PORT_USER" ]]; then
  fail "Port ${JUPYTER_PORT} is already published by container '${PORT_USER}'" \
    "A previous Jupyter session is still running. Stop it: docker stop ${PORT_USER}" \
    "Or open the existing server instead of starting a new one"
fi
if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${JUPYTER_PORT}$"; then
  fail "Port ${JUPYTER_PORT} is in use by a non-Docker process" \
    "Find it: sudo ss -ltnp | grep :${JUPYTER_PORT}" \
    "The IDE CloudFormation template starts a host 'jupyter lab' on 8888 (pid in /tmp/jupyter.pid); kill it if you want the Docker one"
fi
ok "Port ${JUPYTER_PORT} is free"

if ! docker image inspect postgres:16-alpine >/dev/null 2>&1; then
  if ! timeout 5 curl -fsSI https://registry-1.docker.io/v2/ >/dev/null 2>&1 \
     && ! timeout 5 curl -sI https://registry-1.docker.io/v2/ 2>/dev/null | grep -q 'HTTP/'; then
    warn "Docker Hub seems unreachable; image pulls may fail (no internet / proxy / security group)."
  fi
fi

# -----------------------------------------------------------------------------
# 2. Start Postgres (and optionally pgAdmin)
# -----------------------------------------------------------------------------
SERVICES=(postgres)
$WITH_PGADMIN && SERVICES+=(pgadmin)

step "Starting ${SERVICES[*]}"
if ! "${DC[@]}" up -d "${SERVICES[@]}"; then
  show_logs financial_postgres
  fail "'${DC[*]} up -d ${SERVICES[*]}' failed" \
    "Image pull failed: check internet access / Docker Hub rate limits (docker login)" \
    "Port 5432 already in use by a host Postgres: sudo ss -ltnp | grep :5432" \
    "Corrupt volume: ${DC[*]} down -v  (DESTROYS local DB data) then retry" \
    "Disk full: df -h /var/lib/docker"
fi

echo "  Waiting for Postgres healthcheck (max ${POSTGRES_WAIT_SECONDS}s)..."
deadline=$((SECONDS + POSTGRES_WAIT_SECONDS))
PG_STATUS=""
while (( SECONDS < deadline )); do
  PG_STATUS="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' financial_postgres 2>/dev/null || echo missing)"
  case "$PG_STATUS" in
    healthy) break ;;
    exited|dead|missing)
      show_logs financial_postgres
      fail "Postgres container is '${PG_STATUS}'" \
        "Password/user mismatch with an existing volume: the volume was initialised with different POSTGRES_USER/POSTGRES_PASSWORD than .env now has. Either restore the old values or run '${DC[*]} down -v' (DESTROYS data)" \
        "Init SQL error in docker/init-db/*.sql (see logs above)" \
        "Permission problem on the data volume" ;;
  esac
  sleep 2
done
if [[ "$PG_STATUS" != "healthy" ]]; then
  show_logs financial_postgres
  fail "Postgres did not become healthy within ${POSTGRES_WAIT_SECONDS}s (status: ${PG_STATUS})" \
    "Slow first start (initdb + init scripts): re-run with POSTGRES_WAIT_SECONDS=180" \
    "pg_isready failing: POSTGRES_USER/POSTGRES_DB in .env differ from the values the volume was created with" \
    "Inspect: docker logs financial_postgres"
fi
ok "Postgres is healthy (port 5432, db ${POSTGRES_DB:-financial_db})"
$WITH_PGADMIN && ok "pgAdmin on http://localhost:5050"

# -----------------------------------------------------------------------------
# 3. Build the scraper image (Jupyter runs inside it)
# -----------------------------------------------------------------------------
step "Scraper image (Jupyter runtime)"
if $NO_BUILD; then
  docker image inspect financialanalysis-scraper >/dev/null 2>&1 \
    || fail "--no-build given but image 'financialanalysis-scraper' does not exist" \
         "Run without --no-build, or: ${DC[*]} build scraper"
  ok "Using existing image (--no-build)"
elif $REBUILD || ! docker image inspect financialanalysis-scraper >/dev/null 2>&1; then
  $REBUILD && echo "  --rebuild requested" || echo "  Image not found locally; building (first build takes several minutes)..."
  BUILD_LOG="$(mktemp -t scraper-build.XXXXXX.log)"
  if ! "${DC[@]}" build scraper 2>&1 | tee "$BUILD_LOG"; then
    echo >&2
    if grep -q "requires buildx" "$BUILD_LOG"; then
      fail "Image build failed: compose requires a newer buildx (have ${BUILDX_VER:-none}, need >= ${MIN_BUILDX})" \
        "Install a newer buildx into ~/.docker/cli-plugins (see the WARN above for the exact commands)" \
        "Full log: ${BUILD_LOG}"
    elif grep -qiE "no space left on device" "$BUILD_LOG"; then
      fail "Image build failed: disk full" "docker system prune -af  (removes unused images)" "df -h /var/lib/docker" "Full log: ${BUILD_LOG}"
    elif grep -qiE "Could not resolve host|TLS handshake timeout|dial tcp.*i/o timeout|Temporary failure in name resolution|dl.google.com" "$BUILD_LOG"; then
      fail "Image build failed: network error while downloading (apt, pip, or Google Chrome .deb)" \
        "Check internet / proxy / security group egress from this instance" "Retry; transient mirror failures are common" "Full log: ${BUILD_LOG}"
    elif grep -qiE "ERROR: (Could not find a version|No matching distribution|ResolutionImpossible)" "$BUILD_LOG"; then
      fail "Image build failed: pip could not resolve services/scraper/requirements.txt" \
        "A pin in requirements.txt is unavailable for python:3.11; see the pip error above" "Full log: ${BUILD_LOG}"
    else
      fail "Image build failed (see output above)" "Full log: ${BUILD_LOG}" "Retry with: ${DC[*]} build --no-cache scraper"
    fi
  fi
  rm -f "$BUILD_LOG"
  ok "Image financialanalysis-scraper built"
else
  ok "Image financialanalysis-scraper already exists (use --rebuild after changing Dockerfile/requirements)"
fi

# -----------------------------------------------------------------------------
# 4. Start Jupyter (foreground)
# -----------------------------------------------------------------------------
step "Starting Jupyter via docker/start_jupyter.sh"

# Public URL: on the IDE EC2 instance, CloudFront routes /jupyter/* to port 8888.
# Discover the distribution whose origin is this instance; fall back to localhost.
PUBLIC_URL=""
if command -v aws >/dev/null 2>&1; then
  IMDS_TOKEN="$(curl -s -X PUT --max-time 1 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)"
  if [[ -n "$IMDS_TOKEN" ]]; then
    PUB_DNS="$(curl -s --max-time 1 -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" http://169.254.169.254/latest/meta-data/public-hostname 2>/dev/null || true)"
    if [[ -n "$PUB_DNS" ]]; then
      CF_DOMAIN="$(timeout 10 aws cloudfront list-distributions \
        --query "DistributionList.Items[?Origins.Items[?DomainName=='${PUB_DNS}']].DomainName | [0]" \
        --output text 2>/dev/null || true)"
      [[ -n "$CF_DOMAIN" && "$CF_DOMAIN" != "None" ]] && PUBLIC_URL="https://${CF_DOMAIN}/jupyter/tree"
    fi
  fi
fi

echo "  Local URL : http://localhost:${JUPYTER_PORT}/jupyter/tree"
if [[ -n "$PUBLIC_URL" ]]; then
  echo "  Public URL: ${PUBLIC_URL}"
else
  echo "  Public URL: (not detected) If behind the IDE CloudFront distribution use https://<distribution>.cloudfront.net/jupyter/tree"
fi
echo "  Notebook  : .../jupyter/notebooks/notebooks/ingestion/DataIngestion-Text.ipynb"
echo
echo "  If the browser shows a white page or 500 on static JS: the tornado<6.5 pin in docker/start_jupyter.sh is not applied."
echo "  If Jupyter exits right after 'Waiting for PostgreSQL': the scraper container cannot reach 'postgres' on financial_network."
echo "  Ctrl+C stops Jupyter. Postgres keeps running (stop with: ${DC[*]} down)."
echo

set +e
"$ROOT/docker/start_jupyter.sh"
RC=$?
set -e

# 130 = Ctrl+C (SIGINT), the normal way to stop Jupyter.
if [[ $RC -eq 0 || $RC -eq 130 ]]; then
  echo
  ok "Jupyter stopped."
  exit 0
fi

echo >&2
fail "Jupyter exited with code ${RC}" \
  "Port ${JUPYTER_PORT} taken between the pre-flight check and start: docker ps --filter publish=${JUPYTER_PORT}" \
  "pip install of notebook/tornado failed inside the container (network) - scroll up for pip errors" \
  "Mounted ./logs not writable by uid 1000 in the container: chmod a+rwx logs" \
  "Database not reachable from container: docker exec financial_postgres pg_isready -U ${POSTGRES_USER:-financial_user}" \
  "Image stale/broken: re-run with --rebuild"
