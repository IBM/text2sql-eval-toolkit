#!/usr/bin/env bash
set -euo pipefail

# Runtime options
HOST="127.0.0.1"
PORT="8000"
OPEN_BROWSER="true"
DEV_MODE="false"
UI_PORT="5173"

usage() {
  cat <<EOF
Usage: ./scripts/run_dashboard.sh [--host HOST] [--port PORT] [--ui-port PORT] [--dev] [--no-open-browser]

Options:
  --host HOST         Hostname/IP to bind (default: 127.0.0.1)
  --port PORT         Backend port to bind (default: 8000)
  --ui-port PORT      Frontend dev port for --dev mode (default: 5173)
  --dev               Start in development mode with hot reload (backend + Vite)
  --no-open-browser   Do not auto-open browser
  -h, --help          Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="${2:-}"
      shift 2
      ;;
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    --ui-port)
      UI_PORT="${2:-}"
      shift 2
      ;;
    --dev)
      DEV_MODE="true"
      shift
      ;;
    --no-open-browser)
      OPEN_BROWSER="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[dashboard] ERROR: Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

# Resolve repo root (this script lives in scripts/)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

echo "[dashboard] Repo root: $ROOT_DIR"

# 1) Ensure uv is available
if ! command -v uv >/dev/null 2>&1; then
  echo "[dashboard] ERROR: uv is required but not found on PATH."
  echo "[dashboard] Install uv first: https://docs.astral.sh/uv/"
  exit 1
fi

echo "[dashboard] Using uv: $(command -v uv)"

# 2) Install toolkit with dashboard extra (uv-managed environment)
echo "[dashboard] Installing text2sql-eval-toolkit with [dashboard] extra via uv"
uv pip install -e ".[dashboard]"

# 3) Install frontend dependencies and build if needed
if [ ! -d "dashboard" ]; then
  echo "[dashboard] ERROR: dashboard/ directory not found"
  exit 1
fi

if [ ! -d "dashboard/node_modules" ]; then
  echo "[dashboard] Installing dashboard frontend dependencies"
  (cd dashboard && npm install)
fi

# 4) Set data root (defaults to repo data/ unless already set)
export TEXT2SQL_DATA_ROOT="${TEXT2SQL_DATA_ROOT:-"$ROOT_DIR/data"}"
echo "[dashboard] Using TEXT2SQL_DATA_ROOT=$TEXT2SQL_DATA_ROOT"

# 5) Start services
if [[ "$DEV_MODE" == "true" ]]; then
  echo "[dashboard] Starting in DEV mode (hot reload enabled)"
  echo "[dashboard] Backend:  http://$HOST:$PORT (uvicorn --reload)"
  echo "[dashboard] Frontend: http://$HOST:$UI_PORT (Vite dev server)"

  # Keep background backend in sync with current shell lifecycle.
  cleanup() {
    if [[ -n "${FRONTEND_PID:-}" ]]; then
      kill "$FRONTEND_PID" 2>/dev/null || true
    fi
    if [[ -n "${BACKEND_PID:-}" ]]; then
      # Kill backend launcher plus any direct children (reloader/server).
      pkill -P "$BACKEND_PID" 2>/dev/null || true
      kill "$BACKEND_PID" 2>/dev/null || true
    fi
    # Extra safeguard for lingering uvicorn reload children on same host/port.
    pkill -f "uvicorn text2sql_eval_toolkit.ui.server:app --host $HOST --port $PORT --reload" 2>/dev/null || true
  }
  trap cleanup EXIT INT TERM

  uv run uvicorn text2sql_eval_toolkit.ui.server:app --host "$HOST" --port "$PORT" --reload &
  BACKEND_PID=$!

  if [[ "$OPEN_BROWSER" == "true" ]]; then
    # Open the Vite URL in dev mode.
    (
      sleep 2
      if command -v open >/dev/null 2>&1; then
        open "http://$HOST:$UI_PORT"
      elif command -v xdg-open >/dev/null 2>&1; then
        xdg-open "http://$HOST:$UI_PORT"
      fi
    ) &
  fi

  # Run Vite in dashboard/ so index.html is served correctly.
  (
    cd dashboard
    export VITE_API_BASE_URL="http://$HOST:$PORT"
    npm run dev -- --host "$HOST" --port "$UI_PORT" --strictPort
  ) &
  FRONTEND_PID=$!

  # Wait for frontend process; trap handles teardown on Ctrl+C / exit.
  wait "$FRONTEND_PID"
else
  echo "[dashboard] Building dashboard frontend"
  (cd dashboard && npm run build)
  echo "[dashboard] Starting server at http://$HOST:$PORT"
  if [[ "$OPEN_BROWSER" == "true" ]]; then
    exec uv run text2sql-eval-dashboard --host "$HOST" --port "$PORT" --open-browser
  else
    exec uv run text2sql-eval-dashboard --host "$HOST" --port "$PORT"
  fi
fi

