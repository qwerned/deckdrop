#!/bin/bash
# Обновление на месте между двумя локальными службами.
# Использование: e2e-inplace.sh <рабочая папка>
set -u
S="$1"
B="$(cd "$(dirname "$0")/.." && pwd)/target/release/deckdrop"
pkill -f "^$B daemon" 2>/dev/null; sleep 0.5
rm -rf "$S"; mkdir -p "$S"/{a,b,src/game/Content,dst/game/Content}
echo "== источник: новая версия"
dd if=/dev/urandom of="$S/src/game/Content/big.ucas" bs=1m count=300 2>/dev/null
echo first-new > "$S/src/game/first.txt"
for i in 1 2 3; do head -c $((i*50000)) /dev/urandom > "$S/src/game/s$i.dat"; done
echo "== назначение: старая версия короче, с порчей посередине и лишним файлом"
head -c 262144000 "$S/src/game/Content/big.ucas" > "$S/dst/game/Content/big.ucas"
for o in $(seq 10 3 190); do printf XXXX | dd of="$S/dst/game/Content/big.ucas" bs=1 seek=$((o*1048576)) conv=notrunc 2>/dev/null; done
echo "first-old-and-longer-than-the-new-one" > "$S/dst/game/first.txt"
echo keep > "$S/dst/game/vkd3d-proton.cache.write"
a() { DECKDROP_HOME="$S/a" DECKDROP_PORT=52791 DECKDROP_NAME=Mac DECKDROP_PEERS=127.0.0.1:52792 "$B" "$@"; }
z() { DECKDROP_HOME="$S/b" DECKDROP_PORT=52792 DECKDROP_NAME=Deck DECKDROP_PEERS=127.0.0.1:52791 "$B" "$@"; }
a daemon 2>&1 | sed 's/^/[a] /' &
z daemon 2>&1 | sed 's/^/[z] /' &
trap 'pkill -f "^$B daemon" 2>/dev/null' EXIT
sleep 3
echo "== сопряжение"
a pair Deck > "$S/pair.txt" 2>&1 & PP=$!
sleep 1
CODE=$(grep -oE '[0-9]{6}' "$S/pair.txt")
[ -n "$CODE" ] || { echo "НЕТ КОДА"; cat "$S/pair.txt"; exit 1; }
z pair-enter "$CODE"; wait $PP
sleep 3
echo "== отправка на месте"
a send Deck "$S/src/game" --to "$S/dst" --in-place
O=""
for i in $(seq 1 40); do
  O=$(z status --json | python3 -c 'import json,sys;o=json.load(sys.stdin)["offers"];print(o[0]["offer"] if o else "")')
  [ -n "$O" ] && break; sleep 0.25
done
[ -n "$O" ] || { echo "НЕТ ПРЕДЛОЖЕНИЯ"; a status; z status; exit 1; }
echo "предложение $O ждёт решения, принимаю"; z accept "$O" --to "$S/dst"
T0=$(date +%s)
while :; do
  ST=$(a status --json | python3 -c 'import json,sys;j=json.load(sys.stdin)["jobs"][0];print(j["state"],j["phase"],round(j["bytes"]/1e6),"МБ повторов",j["retx"],j["message"])')
  echo "$ST"
  case "$ST" in done*|failed*|cancelled*|declined*) break;; esac
  [ $(( $(date +%s) - T0 )) -gt 120 ] && { echo "ТАЙМАУТ"; break; }
  sleep 0.5
done
a status --json | python3 -c 'import json,sys;print(json.dumps(json.load(sys.stdin)["jobs"][0],ensure_ascii=False)[:900])'
echo "== ждём: изменилось 61 блок + хвост 52 МБ, всего около 230 МБ"
echo "== сверка независимо от приложения"
H1=$(cd "$S/src" && find game -type f -print0 | sort -z | xargs -0 shasum -a 256)
H2=$(cd "$S/dst" && find game -type f ! -name 'vkd3d-proton.cache*' -print0 | sort -z | xargs -0 shasum -a 256)
[ "$H1" = "$H2" ] && echo "СОВПАЛО" || { echo "НЕ СОВПАЛО"; diff <(echo "$H1") <(echo "$H2"); }
[ -f "$S/dst/game/vkd3d-proton.cache.write" ] && echo "лишний файл остался" || echo "ЛИШНИЙ ФАЙЛ УДАЛЁН"
echo "содержимое назначения:"; ls -A "$S/dst"
echo "журнал убран:"; ls "$S/b/inplace" 2>/dev/null | wc -l
