"""Тонкая прослойка: панель Decky ↔ служба DeckDrop. Передачей занимается служба."""
import asyncio
import json
import os
import subprocess

import decky

HOME = decky.DECKY_USER_HOME
SOCK = os.path.join(HOME, ".local/share/deckdrop/ctl.sock")
BIN = os.path.join(HOME, ".local/bin/deckdrop")


async def call(req):
    reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(SOCK), 2)
    try:
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 5)
        return json.loads(line)
    finally:
        writer.close()


class Plugin:
    async def command(self, req):
        try:
            return await call(req)
        except (OSError, asyncio.TimeoutError, ValueError) as e:
            return {"ok": False, "error": str(e), "offline": True}

    async def start_service(self):
        if not os.path.exists(BIN):
            return False
        subprocess.Popen([BIN, "daemon"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        return True

    async def _main(self):
        await Plugin.start_service(self)

    async def _unload(self):
        pass
