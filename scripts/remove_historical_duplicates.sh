#!/usr/bin/env bash
# Wrapper so the Python script can load psycopg2 on macOS when the binary was built against
# a libssl path that no longer exists (e.g. Postgres.app). Setting DYLD_LIBRARY_PATH before
# starting Python makes the linker find Homebrew's openssl@1.1.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
if [[ "$OSTYPE" == darwin* ]] && [[ -d /usr/local/opt/openssl@1.1/lib ]]; then
  export DYLD_LIBRARY_PATH="/usr/local/opt/openssl@1.1/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi
exec python3 "$SCRIPT_DIR/remove_historical_duplicates.py" "$@"
