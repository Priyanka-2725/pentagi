#!/usr/bin/env bash
set -e

MODE="${1:-base}"

# Find backend directory
if [ -d "/app/backend" ]; then
    cd /app/backend
elif [ -d "backend" ]; then
    cd backend
fi

if [ "$MODE" = "base" ]; then
    echo "Running base regression tests..."
    go test -v ./pkg/server/models/...
elif [ "$MODE" = "new" ]; then
    echo "Running new flow concurrency tests..."
    go test -v ./pkg/controller -run 'TestFlowConcurrency'
else
    echo "Unknown mode: $MODE"
    echo "Usage: ./test.sh [base|new]"
    exit 1
fi
