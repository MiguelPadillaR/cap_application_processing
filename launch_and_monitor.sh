#!/usr/bin/env bash
# Lanza el pipeline CAP reanudando donde iba y monitoriza progreso + DNS.
# Uso: ./launch_and_monitor.sh [--rate-limit N] [--workers N]
set -euo pipefail
cd "$(dirname "$0")"

# 1) Verificar que el parche de DNS (Session compartida) sigue en la lib
if [ ! -f .venv/lib/python3.13/site-packages/sigpac_tools/_http.py ]; then
  echo 'ERROR: falta sigpac_tools/_http.py (parche de DNS). No lanzar.' >&2
  exit 1
fi

# 2) Si ya hay un cap-process en marcha, no lanzar otro
if pgrep -af 'cap-process' >/dev/null 2>&1; then
  echo 'ERROR: ya hay un cap-process en marcha:' >&2
  pgrep -af 'cap-process' >&2
  exit 1
fi

LOG="pipeline.log"
echo '=== Estado antes de lanzar ==='
wc -l data/sigpac_cache.jsonl 2>/dev/null || echo 'sin caché aún'
echo
echo '=== Lanzando: setsid cap-process --years 2023 2024 ==='
setsid .venv/bin/cap-process --years 2023 2024 "$@" > "$LOG" 2>&1 < /dev/null &
PID=$!
echo "PID: $PID  (log: $LOG)"
echo 'El proceso está en setsid: una caída de SSH no lo mata.'
echo
echo '=== Monitor (progreso + DNS). Ctrl-C solo para el monitor, NO para el pipeline. ==='
while kill -0 "$PID" 2>/dev/null; do
  # progreso: última línea de "Prefetch progress" del log
  PROG=$(grep -oE 'Prefetch progress.*' "$LOG" 2>/dev/null | tail -1 | sed -E 's/.*year=([0-9]+) done=([0-9]+) pending=([0-9]+) errors=([0-9]+) rate=([^ ]+) eta=([^ ]+) .*/\1: \2|\3 (err \4, \5, eta \6)/' || true)
  # caché en disco
  CACHED=$(wc -l < data/sigpac_cache.jsonl 2>/dev/null || echo 0)
  # misses DNS acumulados (ruido de fondo incluido)
  MISSES=$(resolvectl statistics 2>/dev/null | grep -oP 'Cache Misses:\s*\K\d+' || echo '?')
  echo "$(date +%H:%M:%S)  caché=$CACHED  misses_dns=$MISSES  ${PROG:-arrancando...}"
  sleep 10
done
echo
echo '=== Pipeline terminado (PID $PID) ==='
tail -5 "$LOG"
