#!/bin/bash
set -e

sudo apt-get update
sudo apt-get install -y git curl build-essential libssl-dev pkg-config

# Install Docker
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    sudo usermod -aG docker admin
fi

# Install Docker Compose
sudo apt-get install -y docker-compose-plugin || true

# Install Rust
if ! command -v cargo &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi

source $HOME/.cargo/env

# Clone Repo
if [ ! -d "ccc-postgres-concurrency-proof" ]; then
    git clone https://github.com/0x000000000000000000001/ccc-postgres-concurrency-proof.git
    cd ccc-postgres-concurrency-proof
    git checkout tmp
else
    cd ccc-postgres-concurrency-proof
    git fetch
    git checkout tmp
    git pull
fi

# Start DB
sudo docker compose up -d

# Wait for DB to be ready
echo "Waiting for DB..."
sleep 15

# Increase host file descriptor limit for sqlx connection pools
sudo prlimit --pid $$ --nofile=500000:500000 || echo "Failed to set ulimit"

# Fix ephemeral port exhaustion for massive scaling (>28k connections)
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_tw_reuse=1

export DATABASE_URL=postgres://postgres:postgres@localhost:6432/concurrency_proof
export LOCK_DATABASE_URL=postgres://postgres:postgres@localhost:6433/concurrency_proof

echo "Ready! You can now run the benchmark with:"
echo "cargo run --release --bin benchmark"
