"""Telegram live restream: start channel live streams over MTProto.

A user account runs this module. Bots cannot create a channel, start a
live stream, or read its RTMP address: those MTProto calls are user-only.
The account logs in once with ``stream-archive-setup-telegram-live`` and
the session file holds the login after that. The daemon path never
prompts: a session that is not logged in fails with a clear error.

One Telegram channel can hold one live stream at a time. Each monitored
channel maps to its own Telegram channel in ``channel_telegram_peers``.
When a channel has no target yet and ``auto_create_channels`` is on, the
app creates a private channel for it and stores its peer id at once.
"""

from __future__ import annotations

import asyncio
import io
import logging
import secrets
import tempfile
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from stream_archive.config import AppConfig, RecordingClaim, apply_config_change, bare_name, channel_url
from stream_archive.recorder.common import sanitize_metadata_text

logger = logging.getLogger(__name__)

#: Profile images come from the platforms' CDNs. Anything larger is not
#: a channel icon, so the download stops there.
_MAX_ICON_BYTES = 8 * 1024 * 1024

_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_JPEG_MAGIC = b"\xff\xd8\xff"


#: Seconds a finished segment stays on record as a recording claim.
#: The forward wait is 10 minutes; the record must outlive every
#: competing wait, or the last wait standing picks an ambiguous file.
_CLAIM_WINDOW_S = 1800


def _duration_tolerance(expected_s: float) -> float:
    """How far a recording's duration may sit from its stream segment.

    The server recording starts a little after the stream and stops
    with the call, so it runs shorter than the segment. Long segments
    drift more, hence the percentage.
    """
    return max(60.0, expected_s * 0.10)


def _message_duration(msg: Any) -> float | None:
    """Duration in seconds of a message's media, from its document attributes."""
    doc = getattr(msg, "document", None)
    if doc is None:
        return None
    for attr in getattr(doc, "attributes", None) or []:
        duration = getattr(attr, "duration", None)
        if isinstance(duration, int | float):
            return float(duration)
    return None


def _recording_fingerprint(size: int, duration: float) -> str:
    """Identity of one recording file: its byte size and whole-second duration."""
    return f"{size}:{round(duration)}"


async def _download_image(url: str) -> bytes | None:
    """Fetch one image. None on any failure: icons are best effort."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as http:
            resp = await http.get(url)
            resp.raise_for_status()
            data = resp.content
    except Exception:
        logger.warning("[telegram_live] Icon download failed: %s", url, exc_info=True)
        return None
    if not data or len(data) > _MAX_ICON_BYTES:
        logger.warning("[telegram_live] Icon download has a bad size (%d bytes): %s", len(data), url)
        return None
    return data


async def _as_png_or_jpeg(data: bytes) -> bytes | None:
    """Image bytes in a format Telegram accepts as a channel photo.

    Telegram profile photos take JPEG or PNG. Twitch serves PNG, Kick
    serves WebP, so anything else goes through ffmpeg once. None when
    the bytes are not a convertible image.
    """
    if data.startswith(_PNG_MAGIC) or data.startswith(_JPEG_MAGIC):
        return data
    try:
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "icon.src"
            dst = Path(tmp) / "icon.png"
            src.write_bytes(data)
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(src),
                "-frames:v",
                "1",
                str(dst),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
            if proc.returncode != 0 or not dst.exists():
                return None
            return dst.read_bytes()
    except Exception:
        logger.warning("[telegram_live] Icon conversion failed", exc_info=True)
        return None


#: Message that names the fix when the user session is missing or dead.
SETUP_HINT = "Run 'stream-archive-setup-telegram-live' first to log in the Telegram user account."


def telegram_live_session_path(config: AppConfig) -> Path:
    """Session file path of the Telegram live client, resolved against the workdir."""
    p = Path(config.telegram_live.session)
    if not p.is_absolute():
        p = config.workdir / p
    return p


def marked_peer_id(raw_id: int) -> int:
    """Marked peer id of a channel: raw 123 becomes -100123."""
    return -int(f"100{raw_id}")


def join_rtmp_url(url: str, key: str) -> str:
    """Full RTMP address from the server URL and the stream key.

    Telegram returns the two parts apart, like OBS shows them. The URL
    can end with or without a slash, and it can already hold the key.
    """
    if key and key in url:
        return url
    if url.endswith("/"):
        return f"{url}{key}"
    return f"{url}/{key}"


def _call_from_updates(result: Any) -> Any:
    """Group call object inside a create result, or None.

    A create answers an Updates object. One of its updates carries the
    new call with its id and access hash. The end call needs both.
    """
    updates = getattr(result, "updates", None)
    if updates is None and isinstance(result, list):
        updates = result
    for update in updates or []:
        call = getattr(update, "call", None)
        if call is not None and getattr(call, "id", None) is not None:
            return call
    return None


def _channel_from_result(result: Any) -> Any:
    """First chat inside a create-channel result, or None."""
    chats = getattr(result, "chats", None) or []
    return chats[0] if chats else None


class TelegramLiveStreamer:
    """MTProto client for Telegram live streams, logged in as a user."""

    def __init__(
        self,
        config: AppConfig,
        client_factory: Callable[..., Any] | None = None,
        profile_image_resolver: Callable[[str], Awaitable[str | None]] | None = None,
    ) -> None:
        self._config = config
        self._client_factory = client_factory
        self._profile_image_resolver = profile_image_resolver
        self._client: Any = None
        self._lock = asyncio.Lock()
        #: Serializes recording picks, so two finishing streams cannot
        #: claim the same Saved Messages file in the same round.
        self._forward_lock = asyncio.Lock()
        #: Serializes channel creates, so two first-time channels cannot
        #: race the config save or hit the create rate limit together.
        self._create_lock = asyncio.Lock()

    def set_profile_image_resolver(self, resolver: Callable[[str], Awaitable[str | None]] | None) -> None:
        """Set the channel -> profile image URL lookup used for channel photos.

        The scheduler builds the platform APIs after the streamer, so it
        hands the resolver over once they exist.
        """
        self._profile_image_resolver = resolver

    @property
    def enabled(self) -> bool:
        """True when Telegram live is on and holds its credentials."""
        live = self._config.telegram_live
        return bool(live.enabled and live.api_id > 0 and live.api_hash.strip())

    @property
    def connected(self) -> bool:
        """True when the client holds a live session."""
        client = self._client
        if client is None:
            return False
        is_connected = getattr(client, "is_connected", None)
        if callable(is_connected):
            try:
                return bool(is_connected())
            except Exception:
                return False
        return True

    async def connect(self) -> Any:
        """Return the logged-in client. Raise when the session is not logged in.

        This path never prompts for a code. The setup command owns the
        interactive login, so a daemon run with a dead session fails
        fast with the setup hint instead of blocking on stdin.
        """
        if not self.enabled:
            msg = "Telegram live is not enabled. Set telegram_live.enabled and its api credentials."
            raise RuntimeError(msg)
        if self._client is not None and self.connected:
            return self._client
        async with self._lock:
            if self._client is not None and self.connected:
                return self._client
            if self._client is not None:
                old, self._client = self._client, None
                try:
                    await old.disconnect()
                except Exception:
                    logger.warning("[telegram_live] Dead session disconnect failed", exc_info=True)
            factory = self._client_factory
            if factory is None:
                from telethon import TelegramClient  # type: ignore[import-untyped]

                factory = TelegramClient
            live = self._config.telegram_live
            client = factory(telegram_live_session_path(self._config), live.api_id, live.api_hash)
            try:
                await client.connect()
                authorized = bool(await client.is_user_authorized())
            except Exception as e:
                try:
                    await client.disconnect()
                except Exception:
                    logger.warning("[telegram_live] Temp client disconnect failed", exc_info=True)
                msg = f"Telegram live login failed: {e}. {SETUP_HINT}"
                raise RuntimeError(msg) from e
            if not authorized:
                await client.disconnect()
                msg = f"Telegram user session is not logged in. {SETUP_HINT}"
                raise RuntimeError(msg)
            self._client = client
            logger.info("[telegram_live] Connected")
            return client

    async def disconnect(self) -> None:
        """Close the session. Never raises: shutdown must finish."""
        async with self._lock:
            client, self._client = self._client, None
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    logger.warning("[telegram_live] Disconnect failed", exc_info=True)

    async def ensure_destination(self, channel: str) -> int:
        """Peer id of the Telegram channel for ``channel``.

        A stored mapping wins. Without one, auto-create builds a private
        channel, stores its peer id in the config at once, and returns
        it. The store goes through ``apply_config_change``, so the peer
        id lands on disk before the first stream starts.
        """
        peer = self._config.channel_telegram_peers.get(channel)
        if peer:
            return peer
        if not self._config.telegram_live.auto_create_channels:
            msg = (
                f"No Telegram channel is mapped for {channel}. "
                "Add it to channel_telegram_peers or turn on telegram_live.auto_create_channels."
            )
            raise RuntimeError(msg)
        async with self._create_lock:
            # A second start of the same channel can land here while the
            # first one creates. Read the mapping again inside the lock.
            peer = self._config.channel_telegram_peers.get(channel)
            if peer:
                return peer
            client = await self.connect()
            from telethon.tl.functions.channels import CreateChannelRequest  # type: ignore[import-untyped]

            title = sanitize_metadata_text(f"{bare_name(channel)} restream", limit=128)
            about = f"Live restream of {channel_url(channel)} by StreamArchive"
            try:
                result = await client(CreateChannelRequest(broadcast=True, title=title, about=about))
            except Exception as e:
                msg = f"Telegram channel create failed for {channel}: {e}"
                raise RuntimeError(msg) from e
            chat = _channel_from_result(result)
            raw_id = getattr(chat, "id", None)
            if not isinstance(raw_id, int):
                msg = f"Telegram channel create for {channel} returned no channel id: {result!r}"
                raise RuntimeError(msg)
            peer = marked_peer_id(raw_id)

            def store(cfg: AppConfig) -> None:
                cfg.channel_telegram_peers[channel] = peer

            apply_config_change(self._config, store)
            logger.info("[telegram_live] Created Telegram channel %s for %s", peer, channel)
            return peer

    async def _apply_channel_icon(self, channel: str, peer_id: int) -> None:
        """Set the Telegram channel photo from the broadcaster's icon. Never raises.

        The resolver answers the current profile image URL of the
        source channel. The URL last applied is stored in the config,
        so the photo is set once per icon: at the first stream, and
        again only when the broadcaster changes their icon.
        """
        resolver = self._profile_image_resolver
        if resolver is None:
            return
        try:
            url = await resolver(channel)
        except Exception:
            logger.warning("[telegram_live] Icon lookup failed for %s", channel, exc_info=True)
            return
        if not url or self._config.channel_telegram_icon_urls.get(channel) == url:
            return
        data = await _download_image(url)
        if data is None:
            return
        data = await _as_png_or_jpeg(data)
        if data is None:
            logger.warning("[telegram_live] Icon for %s is not a usable image", channel)
            return
        try:
            client = await self.connect()
            from telethon.tl.functions.channels import EditPhotoRequest
            from telethon.tl.types import InputChatUploadedPhoto  # type: ignore[import-untyped]

            input_peer = await client.get_input_entity(peer_id)
            uploaded = await client.upload_file(io.BytesIO(data), file_name="icon.png")
            await client(EditPhotoRequest(channel=input_peer, photo=InputChatUploadedPhoto(file=uploaded)))
        except Exception:
            logger.warning("[telegram_live] Icon set failed for %s", channel, exc_info=True)
            return

        def store(cfg: AppConfig) -> None:
            cfg.channel_telegram_icon_urls[channel] = url

        apply_config_change(self._config, store)
        logger.info("[telegram_live] Set channel photo for %s from %s", channel, url)

    async def post_chat_log(self, channel: str, file_path: str, caption: str) -> bool:
        """Post one finished chat log file to the channel. False when skipped.

        Skips when the channel has no mapped Telegram channel: a chat
        log alone never creates one. Raises on a send failure, like
        end_stream: the caller logs it.
        """
        peer_id = self._config.channel_telegram_peers.get(channel)
        if not peer_id:
            return False
        client = await self.connect()
        input_peer = await client.get_input_entity(peer_id)
        await client.send_file(input_peer, file=file_path, caption=sanitize_metadata_text(caption, limit=1024))
        logger.info("[telegram_live] Posted chat log for %s to Telegram channel %s", channel, peer_id)
        return True

    async def create_stream(self, author: str, title: str, channel: str, game: str) -> dict[str, Any]:
        """Start a live stream for one monitored channel.

        Returns the RTMP address, the call ids the end call needs, and a
        member link. The title follows the YouTube form: author - title.
        """
        client = await self.connect()
        peer_id = await self.ensure_destination(channel)
        await self._apply_channel_icon(channel, peer_id)
        from telethon.tl.functions.phone import (  # type: ignore[import-untyped]
            CreateGroupCallRequest,
            GetGroupCallStreamRtmpUrlRequest,
            ToggleGroupCallRecordRequest,
        )
        from telethon.tl.types import InputGroupCall

        input_peer = await client.get_input_entity(peer_id)
        rtmp = await client(GetGroupCallStreamRtmpUrlRequest(peer=input_peer, revoke=False))
        url = getattr(rtmp, "url", None)
        key = getattr(rtmp, "key", None)
        if not isinstance(url, str) or not isinstance(key, str) or not url or not key:
            msg = f"Telegram returned no RTMP address for {channel}: {rtmp!r}"
            raise RuntimeError(msg)
        live_title = f"{author} - {title}"
        if self._config.telegram_live.title_timestamp:
            stamp = datetime.now(ZoneInfo(self._config.timezone)).strftime("(%Y-%m-%d %H:%M %Z)")
            live_title = f"{live_title} {stamp}"
        live_title = sanitize_metadata_text(live_title, limit=200)
        result = await client(
            CreateGroupCallRequest(peer=input_peer, rtmp_stream=True, random_id=secrets.randbits(31), title=live_title)
        )
        call = _call_from_updates(result)
        call_id = getattr(call, "id", None)
        access_hash = getattr(call, "access_hash", None)
        if not isinstance(call_id, int) or not isinstance(access_hash, int):
            msg = f"Telegram live create for {channel} returned no call id: {result!r}"
            raise RuntimeError(msg)
        record_baseline_id: int | None = None
        if self._config.telegram_live.record:
            # Best effort: a recording failure must not kill the stream.
            # The file lands in Saved Messages when the stream ends.
            try:
                await client(
                    ToggleGroupCallRecordRequest(
                        call=InputGroupCall(id=call_id, access_hash=access_hash),
                        start=True,
                        video=True,
                        video_portrait=False,
                    )
                )
                # The latest Saved Messages id now: the recording file
                # is the first media message past it after the end.
                record_baseline_id = await self._saved_messages_latest_id(client)
            except Exception:
                logger.warning("[telegram_live] Server recording start failed for %s", channel, exc_info=True)
        raw_id = str(peer_id).removeprefix("-100")
        logger.info("[telegram_live] Live started for %s on Telegram channel %s", channel, peer_id)
        info: dict[str, Any] = {
            "peer_id": peer_id,
            "call_id": call_id,
            "access_hash": access_hash,
            "rtmp_url": join_rtmp_url(url, key),
            "telegram_url": f"https://t.me/c/{raw_id}",
        }
        if record_baseline_id is not None:
            info["record_baseline_id"] = record_baseline_id
        return info

    @staticmethod
    async def _saved_messages_latest_id(client: Any) -> int | None:
        """Id of the newest Saved Messages message, or None on failure."""
        try:
            msgs = await client.get_messages("me", limit=1)
        except Exception:
            logger.warning("[telegram_live] Saved Messages read failed", exc_info=True)
            return None
        if not msgs:
            return 0
        latest = getattr(msgs[0], "id", None)
        return latest if isinstance(latest, int) else None

    async def forward_recording(
        self,
        channel: str,
        baseline_id: int,
        *,
        expected_duration_s: float | None = None,
        ended_at: float | None = None,
        timeout_s: float = 600.0,
        poll_s: float = 20.0,
    ) -> dict[str, Any] | None:
        """Forward this stream's recording from Saved Messages to the channel.

        Saved Messages has no shareable link, so the forwarded post is
        the archive: it lives in the channel and has a channel link.

        A recording message carries no channel name, no title, and no
        call id: the file is always record.mp4 (plus a record.ogg
        audio twin). The only signals are its duration, its size, and
        when it landed, so the pick is strict:

        - video files only, never the audio twin,
        - newer than baseline_id and landed no earlier than the
          stream's end (a small slack covers processing order),
        - duration within max(60 s, 10%) of the stream segment,
        - not forwarded before: Telegram can deliver one recording
          several times, and the (size, duration) fingerprint stored
          in telegram_forwarded_recording_keys is what tells the
          copies apart from new recordings,
        - not ambiguous: a candidate that also matches a recent
          segment of another channel goes to neither. A wrong archive
          is worse than a missing one. Segments stay on record for
          30 minutes in telegram_recording_claims, so the guard also
          holds after a competing wait times out and across restarts.

        Polls until a candidate qualifies or the timeout passes; None
        on timeout or when the channel has no mapped peer.
        """
        peer_id = self._config.channel_telegram_peers.get(channel)
        if not peer_id:
            return None
        client = await self.connect()
        input_peer = await client.get_input_entity(peer_id)
        if expected_duration_s is not None and ended_at is not None:
            self._register_recording_claim(channel, expected_duration_s, ended_at)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while True:
            async with self._forward_lock:
                msgs = await client.get_messages("me", limit=30)
                pick = self._pick_recording(msgs, channel, baseline_id, expected_duration_s, ended_at)
                if pick is not None:
                    msg, fingerprint = pick
                    sent = await client.forward_messages(input_peer, messages=msg.id, from_peer="me")
                    self._store_forwarded_key(fingerprint)
                    first = sent[0] if isinstance(sent, list) else sent
                    post_id = getattr(first, "id", None)
                    raw_id = str(peer_id).removeprefix("-100")
                    url = (
                        f"https://t.me/c/{raw_id}/{post_id}" if isinstance(post_id, int) else f"https://t.me/c/{raw_id}"
                    )
                    logger.info(
                        "[telegram_live] Forwarded the recording of %s to Telegram channel %s", channel, peer_id
                    )
                    return {"message_id": post_id, "telegram_url": url}
            if loop.time() >= deadline:
                logger.warning("[telegram_live] No recording matched in Saved Messages for %s", channel)
                return None
            await asyncio.sleep(poll_s)

    def _register_recording_claim(self, channel: str, duration_s: float, ended_at: float) -> None:
        """Record one finished segment in the config and prune old claims.

        The record outlives this wait and the process. A mass restart
        ends many segments at once, and the next process must still
        know that a near-equal segment of another channel exists.
        """

        def store(cfg: AppConfig) -> None:
            cutoff = ended_at - _CLAIM_WINDOW_S
            claims = [c for c in cfg.telegram_recording_claims if c.ended_at >= cutoff]
            claims.append(RecordingClaim(channel=channel, duration_s=duration_s, ended_at=round(ended_at)))
            cfg.telegram_recording_claims = claims[-50:]

        apply_config_change(self._config, store)

    def _pick_recording(
        self,
        msgs: Any,
        channel: str,
        baseline_id: int,
        expected_duration_s: float | None,
        ended_at: float | None,
    ) -> tuple[Any, str] | None:
        """Best claimable recording message in one Saved Messages page, or None.

        Returns the message and its (size, duration) fingerprint. The
        rules are the ones in forward_recording's docstring.
        """
        consumed = set(self._config.telegram_forwarded_recording_keys)
        best: tuple[float, int, Any, str] | None = None
        for msg in msgs or []:
            msg_id = getattr(msg, "id", None)
            if not isinstance(msg_id, int) or msg_id <= baseline_id:
                continue
            doc = getattr(msg, "document", None)
            mime = getattr(doc, "mime_type", None) if doc is not None else None
            if not isinstance(mime, str) or not mime.startswith("video/"):
                continue
            size = getattr(doc, "size", None)
            duration = _message_duration(msg)
            if not isinstance(size, int) or duration is None:
                continue
            fingerprint = _recording_fingerprint(size, duration)
            if fingerprint in consumed:
                continue
            if ended_at is not None:
                landed = getattr(msg, "date", None)
                landed_ts = landed.timestamp() if landed is not None else None
                if landed_ts is not None and landed_ts < ended_at - 60:
                    continue
            score = 0.0
            if expected_duration_s is not None:
                if abs(duration - expected_duration_s) > _duration_tolerance(expected_duration_s):
                    continue
                ambiguous = any(
                    claim.channel != channel
                    and abs(duration - claim.duration_s) <= _duration_tolerance(claim.duration_s)
                    and (ended_at is None or abs(claim.ended_at - ended_at) <= _CLAIM_WINDOW_S)
                    for claim in self._config.telegram_recording_claims
                )
                if ambiguous:
                    continue
                score = abs(duration - expected_duration_s)
            # Closest duration wins; a tie goes to the earliest
            # delivery, which is the original, not a later copy.
            if best is None or (score, msg_id) < (best[0], best[1]):
                best = (score, msg_id, msg, fingerprint)
        if best is None:
            return None
        return best[2], best[3]

    def _store_forwarded_key(self, fingerprint: str) -> None:
        """Record one forwarded recording fingerprint in the config."""

        def store(cfg: AppConfig) -> None:
            keys = cfg.telegram_forwarded_recording_keys
            if fingerprint not in keys:
                keys.append(fingerprint)
                del keys[:-200]

        apply_config_change(self._config, store)

    async def end_stream(self, telegram_info: dict[str, Any]) -> None:
        """End one live stream. Raises on failure: the caller logs it."""
        client = await self.connect()
        from telethon.tl.functions.phone import DiscardGroupCallRequest
        from telethon.tl.types import InputGroupCall

        call = InputGroupCall(id=int(telegram_info["call_id"]), access_hash=int(telegram_info["access_hash"]))
        await client(DiscardGroupCallRequest(call=call))
        logger.info("[telegram_live] Live ended on Telegram channel %s", telegram_info.get("peer_id"))

    async def close(self) -> None:
        await self.disconnect()
