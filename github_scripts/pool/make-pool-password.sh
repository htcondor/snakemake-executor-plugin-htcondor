#!/bin/bash
# Generate a random pool-password file used by all three containers to
# authenticate to each other (PASSWORD auth). The file just needs identical
# bytes in every container — docker-compose mounts this one file into all of
# them. Regenerate freely; it is git-ignored and only used for the local/CI
# throwaway pool.
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
out="$dir/pool_password"

# 64 hex chars: plain text (avoids any binary-handling quirks) and no pipeline
# that could trip `set -o pipefail` with a SIGPIPE.
openssl rand -hex 32 > "$out"
chmod 0600 "$out"
echo "Wrote $out"
