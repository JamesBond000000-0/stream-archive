"""Log in the Telegram user account for live restreams.

Run this once, in an interactive terminal:

    stream-archive-setup-telegram-live

Telethon asks for the phone number, the login code, and the two-step
password when the account has one. The session file then holds the
login, and the daemon never prompts. The account must own or manage
each Telegram channel it restreams to. Keep the session file private:
it is a full login for that account.
"""

import asyncio
import sys

from stream_archive.config import get_config
from stream_archive.telegram_streamer import telegram_live_session_path


async def _login() -> None:
    from telethon import TelegramClient  # type: ignore[import-untyped]

    try:
        config = get_config()
    except (FileNotFoundError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    live = config.telegram_live
    if live.api_id <= 0 or not live.api_hash.strip():
        print(
            "ERROR: telegram_live.api_id and telegram_live.api_hash are not set. "
            "Set them in config.json (or its env vars) first. Get both from https://my.telegram.org/apps.",
            file=sys.stderr,
        )
        sys.exit(1)
    session = telegram_live_session_path(config)
    print("Starting Telegram user login...")
    print(f"Session file: {session}")
    print("Telethon now asks for the phone number, the login code, and the two-step password if set.")
    client = TelegramClient(str(session), live.api_id, live.api_hash)
    try:
        # Telethon prompts on stdin for the phone, the code, and the
        # password. No value passes through this module.
        await client.start()
        me = await client.get_me()
        name = getattr(me, "username", None) or getattr(me, "first_name", None) or getattr(me, "id", "?")
        print()
        print(f"Logged in as {name}.")
        print("Telegram live login complete. Set telegram_live.enabled to true and restart the app.")
    finally:
        await client.disconnect()


def main() -> None:
    try:
        asyncio.run(_login())
    except KeyboardInterrupt:
        print("\nERROR: login stopped. Run the command again to finish it.", file=sys.stderr)
        sys.exit(1)
    except EOFError:
        print("\nERROR: no input received. Run the command in an interactive terminal.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
