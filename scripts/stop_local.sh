#!/usr/bin/env bash
# Termina os processos arrancados por run_local.sh / start_node.sh.
# Uso: scripts/stop_local.sh [ID ...]   (sem argumentos: todos, incluindo o bootstrapper)
# Os nós recebem "sair" (terminação limpa, com LEAVE); se não terminarem em 3 s, são mortos.
cd "$(dirname "$0")/.."

if [ $# -gt 0 ]; then
    IDS="$*"
else
    IDS=$(ls logs/*.pid 2>/dev/null | xargs -rn1 basename | sed 's/\.pid$//' | grep -v '^bootstrapper$')
fi

for id in $IDS; do
    pidfile="logs/$id.pid"
    [ -f "$pidfile" ] || { echo "$id: sem PID"; continue; }
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
        timeout 1 sh -c "echo sair > logs/$id.cmd" 2>/dev/null || true
    fi
done
for id in $IDS; do
    pidfile="logs/$id.pid"
    [ -f "$pidfile" ] || continue
    pid=$(cat "$pidfile")
    for _ in $(seq 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    if kill -0 "$pid" 2>/dev/null; then kill -9 "$pid"; echo "$id: morto à força"; else echo "$id: terminado"; fi
    rm -f "$pidfile" "logs/$id.cmd"
done

if [ $# -eq 0 ] && [ -f logs/bootstrapper.pid ]; then
    pid=$(cat logs/bootstrapper.pid)
    kill "$pid" 2>/dev/null
    for _ in $(seq 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    if kill -0 "$pid" 2>/dev/null; then kill -9 "$pid"; echo "bootstrapper: morto à força"; else echo "bootstrapper: terminado"; fi
    rm -f logs/bootstrapper.pid
fi
exit 0
