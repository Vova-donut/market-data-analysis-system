from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any, Optional, Tuple

from .config import TG_API_HASH, TG_API_ID, TG_CHAT_ID, TG_ENABLE, TG_PASSWORD_2FA, TG_PHONE

log = logging.getLogger("spread_watcher")

async def tg_prepare() -> Optional[Tuple[Any, Any]]:
    if not TG_ENABLE:
        return None
    if not (TG_API_ID and TG_API_HASH and TG_PHONE and TG_CHAT_ID):
        log.warning("TG_ENABLE=1 but telegram env is incomplete.")
        return None
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.errors import SessionPasswordNeededError  # type: ignore
    except Exception:
        log.warning("TG_ENABLE=1 but Telethon is not installed.")
        return None
    session_name = os.getenv("TG_SESSION_NAME", "parser")
    client = TelegramClient(session_name, int(TG_API_ID), TG_API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(TG_PHONE)
        try:
            code = await asyncio.to_thread(input, "Telegram code (from app/SMS): ")
        except Exception:
            await client.disconnect()
            return None
        try:
            await client.sign_in(TG_PHONE, code)
        except SessionPasswordNeededError:
            pw = TG_PASSWORD_2FA or await asyncio.to_thread(input, "Telegram 2FA password: ")
            await client.sign_in(password=pw)
    try:
        chat_ref: str | int = TG_CHAT_ID.strip()
        if re.fullmatch(r"-?\d+", str(chat_ref)):
            chat_ref = int(str(chat_ref))
        try:
            await client.get_dialogs(limit=200)
        except Exception:
            pass
        chat_entity = await client.get_input_entity(chat_ref)
    except Exception:
        log.exception("Failed to resolve TARGET_CHAT_VLAD=%s", TG_CHAT_ID)
        await client.disconnect()
        return None
    return client, chat_entity


async def tg_sender(q: "asyncio.Queue[str]", client_chat: Optional[Tuple[Any, Any]]) -> None:
    if not client_chat:
        while True:
            _ = await q.get()
    client, chat_entity = client_chat
    try:
        while True:
            try:
                text = await q.get()
                await client.send_message(chat_entity, text)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("TG send error")
                await asyncio.sleep(0.5)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


