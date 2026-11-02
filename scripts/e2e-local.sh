#!/bin/bash
# Проверка передачи целиком на одной машине: две службы, сопряжение, потери, обрыв службы получателя.
# Использование: e2e-local.sh <рабочая папка> [доля потерь] [предел МБ/с]
set -u
S="$1"; LOSS="${2:-0.02}"; RATE="${3:-0}"
B="$(cd "$(dirname "$0")/.." && pwd)/target/release/deckdrop"
pkill -f "^$B daemon" 2>/dev/null; sleep 0.5
rm -rf "$S"; mkdir -p "$S"/{a,b,src/game/sub,dst}
echo "== готовлю данные"
dd if=/dev/urandom of="$S/src/game/big.bin" bs=1m count=800 2>/dev/null
for i in $(seq 1 300); do head -c $((RANDOM*3)) /dev/urandom > "$S/src/game/sub/f$i.dat"; done
: > "$S/src/game/empty.txt"; ln -s sub/f1.dat "$S/src/game/link"; touch -t 202001020304 "$S/src/game/empty.txt"
a() { DECKDROP_HOME="$S/a" DECKDROP_PORT=52791 DECKDROP_NAME=Mac DECKDROP_PEERS=127.0.0.1:52792 "$B" "$@"; }
z() { DECKDROP_HOME="$S/b" DECKDROP_PORT=52792 DECKDROP_NAME=Deck DECKDROP_PEERS=127.0.0.1:52791 DECKDROP_SIM_LOSS="$LOSS" DECKDROP_SIM_RATE="$RATE" "$B" "$@"; }
a daemon > "$S/a.log" 2>&1 & PA=$!
z daemon > "$S/b.log" 2>&1 & PB=$!
trap 'kill $PA $PB 2>/dev/null' EXIT
sleep 3
echo "== устройства"; a status
echo "== сопряжение"
a pair Deck > "$S/pair.txt" 2>&1 & PP=$!
sleep 1
CODE=$(grep -oE '[0-9]{6}' "$S/pair.txt")
echo "код $CODE"
[ -n "$CODE" ] || { echo "НЕТ КОДА"; cat "$S/pair.txt"; cat "$S"/*.log; exit 1; }
z pair-enter "$CODE"; wait $PP; cat "$S/pair.txt"
sleep 3
echo "== свободное место видно с другой стороны"; a status | head -8
echo "== отправка"
a send Deck "$S/src/game" --to "$S/dst" > "$S/job.txt"; cat "$S/job.txt"
for i in $(seq 1 40); do
  O=$(z status --json | python3 -c 'import json,sys;o=json.load(sys.stdin)["offers"];print(o[0]["offer"] if o else "")')
  [ -n "$O" ] && break; sleep 0.25
done
[ -n "$O" ] || { echo "НЕТ ПРЕДЛОЖЕНИЯ"; z status; a status; exit 1; }
echo "предложение $O"; z accept "$O" --to "$S/dst"
sleep 4; a status | tail -1
echo "== обрыв: убиваю службу получателя"
kill -9 $PB; wait $PB 2>/dev/null
sleep 3; a status | tail -1
echo "== запускаю получателя снова"
z daemon > "$S/b2.log" 2>&1 & PB=$!
T0=$(date +%s)
while :; do
  ST=$(a status --json | python3 -c 'import json,sys;j=json.load(sys.stdin)["jobs"][0];print(j["state"],j["phase"],round(j["bytes"]/1e6),"МБ",round(j["speed"]/1e6,1),"МБ/с повторов",j["retx"],j["message"])')
  echo "$ST"
  case "$ST" in done*|failed*|cancelled*|declined*) break;; esac
  [ $(( $(date +%s) - T0 )) -gt 120 ] && { echo "ТАЙМАУТ"; break; }
  sleep 1
done
echo "== сверка независимо от приложения"
H1=$(cd "$S/src" && find game -type f -print0 | sort -z | xargs -0 shasum -a 256 | shasum -a 256)
H2=$(cd "$S/dst" && find game -type f -print0 | sort -z | xargs -0 shasum -a 256 | shasum -a 256)
echo "$H1"; echo "$H2"; [ "$H1" = "$H2" ] && echo "СОВПАЛО" || echo "НЕ СОВПАЛО"
ls -la "$S/dst/game" | head -8; ls -A "$S/dst"
a status --json | python3 -c 'import json,sys;j=json.load(sys.stdin)["jobs"][0];print("корень",j["root"][:32]);print("в среднем",round(j["avg_speed"]/1e6,1),"МБ/с")'
cat "$S/a.log" "$S/b.log" "$S/b2.log" | tail -5
