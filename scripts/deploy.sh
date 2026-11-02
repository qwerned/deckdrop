#!/bin/bash
# Сборка и установка службы на Мак и Deck.
set -e
cd "$(dirname "$0")/.."
echo "== сборка для Мака"; cargo build --release -p deckdrop 2>&1
echo "== сборка для Deck"; docker run --rm -v "$PWD":/src -v deckdrop-cargo:/usr/local/cargo/registry -w /src -e CARGO_TARGET_DIR=/src/target/docker deckdrop-cross cargo build --release -p deckdrop --target x86_64-unknown-linux-gnu 2>&1
DECK=${DECK:?укажите DECK=deck@адрес}
echo "== установка на Deck"; ssh "$DECK" 'systemctl --user stop deckdrop' ; scp -q target/docker/x86_64-unknown-linux-gnu/release/deckdrop "$DECK":.local/bin/deckdrop; ssh "$DECK" 'systemctl --user start deckdrop'
echo "== перезапуск на Маке"; pkill -f '/deckdrop$' || true; pkill -f 'deckdrop daemon' || true; sleep 1; nohup target/release/deckdrop daemon </dev/null >/dev/null 2>&1 &
sleep 5
