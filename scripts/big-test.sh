#!/bin/bash
# Главный тест: папка с файлом 5 ГБ и мелкими файлами. Пауза и обрывы на ходу, независимая сверка.
# big-test.sh <рабочая папка>
cd "$(dirname "$0")/.."
W="$1"; SRC="$W/big/dd40"; M=target/release/deckdrop; DECK=${DECK:?укажите DECK=deck@адрес}; D='$HOME/.local/bin/deckdrop'
mkdir -p "$SRC/sub"
if [ ! -f "$SRC/game.bin" ]; then
  echo "== создаю файл 5 ГБ"; dd if=/dev/urandom of="$SRC/game.bin" bs=1m count=4800 status=progress 2>&1
  echo "== мелкие файлы"; for i in $(seq 1 500); do head -c $((RANDOM*4)) /dev/urandom > "$SRC/sub/f$i.dat"; done
  touch -t 202105060708 "$SRC/sub/f1.dat"
fi
echo "== хеш источника"; (cd "$W/big" && find dd40 -type f -print0 | sort -z | xargs -0 shasum -a 256 | shasum -a 256 | cut -d' ' -f1) | tee "$W/big/src.sha"
echo "== свободно на Deck: $(ssh $DECK 'df --output=avail -B1 /home | tail -1')"
JOB=$($M send "Steam Deck" "$SRC" --to '~/Downloads/ddtest40' --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["job"])'); echo "задание $JOB"
for i in $(seq 1 40); do O=$(ssh $DECK "$D status --json" | python3 -c 'import json,sys;o=json.load(sys.stdin)["offers"];print(o[0]["offer"] if o else "")'); [ -n "$O" ] && break; sleep 0.5; done
ssh $DECK "$D accept $O --to '~/Downloads/ddtest40'"
T0=$(date +%s); E1=; E2=; E3=
while :; do
  L=$($M status --json | python3 -c '
import json,sys
j=[x for x in json.load(sys.stdin)["jobs"] if x["id"]=="'$JOB'"][0]
print(j["state"],j["phase"],int(j["bytes"]/1e6),"%.1f"%(j["speed"]/1e6),"%.1f"%(j["avg_speed"]/1e6),"предел %.0f"%(j["rate"]/1e6),"rtt %.0f"%(j["rtt_us"]/1e3),"повторы",j["retx"],"из",j["sent_pkts"],"заново",j["repaired"],"сверка %.1f"%(j["phase_done"]/1e9),j["root"][:16],j["message"])')
  echo "$(( $(date +%s)-T0 ))с $L"
  set -- $L; MB=$3
  if [ -z "$E1" ] && [ "${MB:-0}" -gt 800 ]; then E1=1; echo ">>> пауза 15 с"; $M pause "$JOB"; sleep 15; $M resume "$JOB"; echo ">>> продолжаю"; fi
  if [ -z "$E2" ] && [ "${MB:-0}" -gt 2000 ]; then E2=1; echo ">>> kill -9 службы на Deck (автоперезапуск systemd)"; ssh $DECK 'pkill -9 -x deckdrop'; fi
  if [ -z "$E3" ] && [ "${MB:-0}" -gt 3500 ]; then E3=1; echo ">>> заморозка службы на Deck на 20 с, связь молчит"; ssh $DECK 'pkill -STOP -x deckdrop; sleep 20; pkill -CONT -x deckdrop'; echo ">>> разморожена"; fi
  case "$L" in done*|failed*|cancelled*|declined*) break;; esac
  sleep 5
done
echo "== независимая сверка на Deck"
ssh $DECK 'cd ~/Downloads/ddtest40 && find dd40 -type f -print0 | sort -z | xargs -0 sha256sum | awk "{print \$1\"  \"\$2}" | sha256sum | cut -d" " -f1; stat -c "%y %n" dd40/sub/f1.dat; ls -A ~/Downloads/ddtest40; df -h /home | tail -1'
echo "== итог источника $(cat "$W/big/src.sha")"
(cd "$W/big" && find dd40 -type f -print0 | sort -z | xargs -0 shasum -a 256 | awk '{print $1"  "$2}' | shasum -a 256 | cut -d' ' -f1)
echo ГОТОВО
