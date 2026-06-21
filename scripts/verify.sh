#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
  docker compose down -v
}
trap cleanup EXIT

docker compose up -d

until docker compose exec -T postgres pg_isready -U postgres -d concurrency_proof; do
  sleep 1
done

cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features -- --nocapture
