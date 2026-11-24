"""Telegram live output: feed one capture to a Telegram channel.

The flow mirrors the YouTube output with the Telegram parts removed:
no daily create budget, no hold or reuse, no split chain, and no status
poll. Telegram ends a live stream when its call ends, and it keeps no
archive. ``telegram_both`` also writes the disk file in the same read
loop, so the local copy is the archive of that mode.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from stream_archive.config import TELEGRAM_MODES, AppConfig, channel_url
from stream_archive.recorder.common import sanitize_metadata_text
from stream_archive.recorder.types import Recording

if TYPE_CHECKING:
    from stream_archive.notifier import Notifier
    from stream_archive.telegram_streamer import TelegramLiveStreamer

logger = logging.getLogger(__name__)


class TelegramOutputMixin:
    _config: AppConfig
    _telegram: TelegramLiveStreamer | None
    _notifier: Notifier | None
    _recordings: dict[str, Recording]
    # Set by sibling mixins and Recorder (core.py). Exact call shapes so
    # a signature drift fails type checks instead of failing at runtime.
    _track: Callable[[str, Coroutine[Any, Any, Any]], asyncio.Task[Any]]
    _record_disk: Callable[[str, str, Any], Coroutine[Any, Any, None]]
    _open_feed: Callable[[Any], Coroutine[Any, Any, Any]]
    _read_ffmpeg_stderr: Callable[[str, Any], Coroutine[Any, Any, list[str]]]
    _channel_dir: Callable[[str], str]
    _terminate: Callable[..., Coroutine[Any, Any, None]]

    def _require_telegram(self, channel: str) -> TelegramLiveStreamer:
        """Return the Telegram live streamer. Raise when none is configured."""
        telegram = self._telegram
        if telegram is None:
            msg = f"Telegram live streamer is not configured for {channel}"
            raise RuntimeError(msg)
        return telegram

    async def _end_telegram_stream(self, channel: str, telegram_info: dict[str, Any] | None) -> None:
        """End the Telegram live stream of one recording. Never raises."""
        if not telegram_info:
            return
        try:
            await self._require_telegram(channel).end_stream(telegram_info)
        except Exception as e:
            logger.error("[recorder] [telegram] Error ending live stream for %s: %s", channel, e)

    async def _post_chat_log_file(self, channel: str, entry: Recording, chat_path: str) -> None:
        """Post the finished chat log to the Telegram channel. Never raises.

        Runs at finalize, after the chat writer closed the file. Only
        recordings that actually restreamed to Telegram qualify: the
        caller passes the entry after its telegram_info was used to end
        the stream.
        """
        if not self._config.telegram_live.post_chat_log:
            return
        if entry.get("mode") not in TELEGRAM_MODES:
            return
        telegram = self._telegram
        if telegram is None:
            return
        caption = f"Chat log: {sanitize_metadata_text(entry.get('title') or 'Unknown')}"
        try:
            await telegram.post_chat_log(channel, chat_path, caption)
        except Exception:
            logger.error("[recorder] [telegram] Chat log post failed for %s", channel, exc_info=True)

    async def _stream_telegram(
        self,
        channel: str,
        author: str,
        title: str,
        game: str,
        stream: Any,
        filepath: str | None,
        notify: bool = True,
        telegram_notify: bool = True,
    ) -> None:
        entry = self._recordings.get(channel)
        if entry is None:
            return
        try:
            telegram_info = await self._require_telegram(channel).create_stream(author, title, channel, game)
        except Exception as e:
            logger.error("[recorder] [telegram] Failed to start live stream for %s: %s", channel, e)
            entry = self._recordings.get(channel)
            if filepath is not None and entry is not None:
                # telegram_both keeps a local copy: fall back to a plain
                # disk recording, like the YouTube rate-limit path.
                logger.info("[recorder] Telegram start failed - falling back to disk recording for %s", channel)
                disk_task = self._track(channel, self._record_disk(channel, filepath, stream))
                entry["tasks"].append(disk_task)
                if self._notifier and notify:
                    try:
                        await self._notifier.notify(
                            f"\u26a0\ufe0f Telegram live start failed for {channel}: {e}. Recording to disk instead."
                        )
                        await self._notifier.notify_live(channel, title, game, channel_url(channel))
                    except Exception:
                        logger.error("[recorder] live notification failed for %s", channel, exc_info=True)
                return
            raise
        entry = self._recordings.get(channel)
        if entry is None:
            # The recording vanished while the live stream was created,
            # so nothing can feed it. End it here.
            await self._end_telegram_stream(channel, telegram_info)
            return
        entry["telegram_info"] = telegram_info
        if self._notifier and telegram_notify:
            try:
                await self._notifier.notify_live(
                    channel, title, game, channel_url(channel), telegram_url=telegram_info["telegram_url"]
                )
            except Exception:
                logger.error("[recorder] live notification failed for %s", channel, exc_info=True)
        await self._capture_and_send(channel, title, stream, filepath)

    async def _capture_and_send(
        self,
        channel: str,
        title: str | None,
        stream: Any,
        filepath: str | None,
    ) -> None:
        """Copy the source into the disk file and the Telegram ingest.

        A broken ingest drops the feeder: with a disk file the copy
        goes on to the end, without one the capture fails. A clean
        source EOF ends the capture normally.
        """
        try:
            fd = await self._open_feed(stream)
        except Exception as e:
            logger.error("[recorder] [telegram] %s stream open failed: %s", channel, e)
            msg = f"[telegram] {channel} stream interrupted"
            raise RuntimeError(msg) from e

        loop = asyncio.get_running_loop()
        file_handle: Any = None
        feeder: Any = None
        stderr_task: asyncio.Task[Any] | None = None
        ingest_error: str | None = None
        clean = False
        try:
            if filepath:
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                file_handle = open(filepath, "wb")  # noqa: SIM115 - closed in the finally below
            entry = self._recordings.get(channel)
            if entry is None:
                return
            feeder = await self._spawn_telegram_feeder(channel, entry.get("telegram_info"))
            if feeder is None:
                msg = f"[telegram] {channel} ffmpeg could not start"
                raise RuntimeError(msg)
            stderr_task = asyncio.create_task(self._read_ffmpeg_stderr(channel, feeder))
            while True:
                try:
                    data = await loop.run_in_executor(None, fd.read, 65536)
                except Exception as e:
                    logger.error("[recorder] [telegram] %s read error: %s", channel, e)
                    break
                if not data:
                    clean = True
                    break
                if file_handle is not None:
                    await loop.run_in_executor(None, file_handle.write, data)
                if feeder is not None:
                    stdin = getattr(feeder, "stdin", None)
                    if stdin is None:
                        ingest_error = "Telegram ingest closed"
                        feeder = None
                    else:
                        try:
                            stdin.write(data)
                            await stdin.drain()
                        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
                            ingest_error = f"Telegram ingest failed: {e}"
                            logger.warning("[recorder] [telegram] %s %s", channel, ingest_error)
                            feeder = None
                        except Exception as e:
                            ingest_error = f"Telegram ingest failed: {e}"
                            logger.error("[recorder] [telegram] %s %s", channel, ingest_error)
                            feeder = None
                if feeder is None and file_handle is None:
                    break
        finally:
            if stderr_task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await asyncio.gather(stderr_task, return_exceptions=True)
            if feeder is not None:
                await self._terminate(feeder)
            if file_handle is not None:
                with contextlib.suppress(Exception):
                    await loop.run_in_executor(None, file_handle.close)
            with contextlib.suppress(Exception):
                await loop.run_in_executor(None, fd.close)
            logger.info("[recorder] [telegram] %s ffmpeg stopped", channel)

        if ingest_error is not None:
            await self._notify_telegram_failure(channel, title, ingest_error, filepath is not None)
        if not clean:
            msg = f"[telegram] {channel} stream interrupted"
            raise RuntimeError(msg)

    async def _spawn_telegram_feeder(self, channel: str, telegram_info: dict[str, Any] | None) -> Any:
        """Start the ffmpeg that feeds one Telegram ingest. None when it cannot start."""
        rtmp_url = (telegram_info or {}).get("rtmp_url")
        if not rtmp_url:
            return None
        ffmpeg_cmd = [
            "ffmpeg",
            "-loglevel",
            "warning",
            "-fflags",
            "+genpts+igndts",
            "-re",
            "-i",
            "pipe:0",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            "160k",
            "-max_muxing_queue_size",
            "1024",
            "-f",
            "flv",
            "-flvflags",
            "no_duration_filesize",
            rtmp_url,
        ]
        logger.info("[recorder] [telegram] Starting ffmpeg for %s", channel)
        try:
            return await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            logger.error("[recorder] [telegram] ffmpeg spawn failed for %s: %s", channel, e)
            return None

    async def _notify_telegram_failure(self, channel: str, title: str | None, reason: str, has_disk: bool) -> None:
        """Tell the operator that the Telegram restream died. Never raises."""
        if self._notifier is None:
            return
        lines = [
            f"\u26a0\ufe0f Telegram restream failed for {channel}.",
            f"Stream: {sanitize_metadata_text(title or 'Unknown')}",
            f"Source: {channel_url(channel)}",
            f"Cause: {sanitize_metadata_text(reason)}",
        ]
        if has_disk:
            lines.append("The disk recording continues.")
        else:
            lines.append("There is no local backup in telegram mode.")
        try:
            await self._notifier.notify("\n".join(lines))
        except Exception:
            logger.error("[recorder] telegram-failure notification failed for %s", channel, exc_info=True)

    def telegram_active_count(self) -> int:
        """Active recordings whose mode uses a Telegram restream (for the uplink cap)."""
        return sum(1 for e in self._recordings.values() if e.get("mode") in ("telegram", "telegram_both"))
