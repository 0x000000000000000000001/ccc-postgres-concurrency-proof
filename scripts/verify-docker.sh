#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
  git checkout src/lib.rs || true
  docker compose down -v
}
trap cleanup EXIT

docker compose up -d

until docker compose exec -T postgres pg_isready -U postgres -d concurrency_proof; do
  sleep 1
done

echo "Patching database URL for docker network..."
sed -i '' 's/localhost:54329/postgres:5432/' src/lib.rs

echo "Running cargo tests via Docker..."
docker run --rm \
  --network core_default \
  -v "$PWD":/usr/src/myapp \
  -w /usr/src/myapp \
  rust:latest \
  bash -c "cargo test --all-targets --all-features -- --nocapture"
