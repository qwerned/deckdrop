#!/bin/bash
# Долгая тишина от соседа: служба должна пересоздать служебный сокет и снова его увидеть.
# Использование: e2e-heal.sh <рабочая папка>
set -u
S="$1"
B="$(cd "$(dirname "$0")/.." && pwd)/target/release/deckdrop"
pkill -f "^$B daemon" 2>/dev/null; sleep 0.5
rm -rf "$S"; mkdir -p "$S"/{a,b,src,dst}
head -c 50000000 /dev/urandom > "$S/src/f.bin"
a() { DECKDROP_HOME="$S/a" DECKDROP_PORT=52791 DECKDROP_NAME=Mac DECKDROP_PEERS=127.0.0.1:52792 "$B" "$@"; }
z() { DECKDROP_HOME="$S/b" DECKDROP_PORT=52792 DECKDROP_NAME=Deck DECKDROP_PEERS=127.0.0.1:52791 "$B" "$@"; }
DECKDROP_HOME="$S/a" DECKDROP_PORT=52791 DECKDROP_NAME=Mac DECKDROP_PEERS=127.0.0.1:52792 "$B" daemon 2>/dev/null &
DECKDROP_HOME="$S/b" DECKDROP_PORT=52792 DECKDROP_NAME=Deck DECKDROP_PEERS=127.0.0.1:52791 "$B" daemon 2>/dev/null & PZ=$!
trap 'kill -CONT $PZ 2>/dev/null; pkill -f "^$B daemon" 2>/dev/null' EXIT
sleep 3
a pair Deck > "$S/pair.txt" 2>&1 & PP=$!
sleep 1
z pair-enter "$(grep -oE '[0-9]{6}' "$S/pair.txt")"; wait $PP
sleep 3
echo "== до заморозки: $(a status | grep Устройство)"
echo "== замораживаю соседа на 40 с"; kill -STOP $PZ; sleep 40; kill -CONT $PZ
for i in $(seq 1 10); do sleep 1; ST=$(a status | grep Устройство); case "$ST" in *"не в сети"*) ;; *) break;; esac; done
echo "== после: $ST"
echo "== журнал службы:"; cat "$S/a/deckdrop.log"
a send Deck "$S/src/f.bin" --to "$S/dst" --wait --json > /dev/null & PS=$!
for i in $(seq 1 20); do O=$(z status --json | python3 -c 'import json,sys;o=json.load(sys.stdin)["offers"];print(o[0]["offer"] if o else "")'); [ -n "$O" ] && break; sleep 0.5; done
z accept "$O" --to "$S/dst" >/dev/null; wait $PS
cmp "$S/src/f.bin" "$S/dst/f.bin" && echo "ФАЙЛ СОВПАЛ" || echo "ФАЙЛ НЕ СОВПАЛ"
