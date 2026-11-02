#!/bin/bash
DECK=${DECK:?укажите DECK=deck@адрес}
# Сравнение частоты обратной связи на реальном канале.
cd "$(dirname "$0")/.."
W="$1"
for cfg in "5 10" "20 50" "50 100" "100 200"; do
  read -r F B <<< "$cfg"
  ssh $DECK "printf '[Service]\nEnvironment=DECKDROP_FB_MS=$F\nEnvironment=DECKDROP_BEAT_MS=$B\n' > ~/.config/systemd/user/deckdrop.service.d/t.conf; systemctl --user daemon-reload; systemctl --user restart deckdrop"
  pkill -f 'release/deckdrop daemon'; sleep 0.5
  DECKDROP_FB_MS=$F DECKDROP_BEAT_MS=$B nohup target/release/deckdrop daemon </dev/null >/dev/null 2>&1 &
  sleep 5
  rm -rf "$W/back/probe"
  echo "=== обратная связь $F мс, сигнал $B мс"
  scripts/wifi-test.sh to-deck "$W/src/probe" '~/Downloads/ddtest' 2>&1 | grep -E 'done|failed' | cut -c1-120
  scripts/wifi-test.sh to-mac /home/deck/Downloads/ddtest/probe "$W/back" 2>&1 | grep -E 'done|failed' | cut -c1-120
  ssh $DECK 'rm -rf ~/Downloads/ddtest'
done
