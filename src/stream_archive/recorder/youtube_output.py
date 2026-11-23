from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from collections.abc import Callable, Coroutine
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from stream_archive import disk
from stream_archive.config import (
    AppConfig,
    channel_url,
)
from stream_archive.health import clear_degraded, set_degraded
from stream_archive.recorder.common import RECORDED_BY_MARKER, sanitize_filename, sanitize_metadata_text
from stream_archive.recorder.types import HoldState, Recording

if TYPE_CHECKING:
    from stream_archive.notifier import Notifier
    from stream_archive.youtube_streamer import YouTubeStreamer

logger = logging.getLogger(__name__)


_QUICK_END_S = 120

_BACKOFF_BASE_S = 120

_BACKOFF_MAX_S = 1800

# Rolling 24-hour budget of broadcast creations. Guards YouTube's daily
# limit on new broadcast creations. A split chain spends one create for
# each part, so the budget covers about two full days of 11-hour parts.
_YOUTUBE_DAILY_BUDGET = 30

_YOUTUBE_BUDGET_WINDOW_S = 86400

# Pre-encoded 1920x1080@60 "Reconnecting..." interstitial (animated dots,
# 3s loop) fed into a held broadcast with `-c copy` - no runtime encoding.
_RECONNECT_CLIP = Path(__file__).resolve().parent.parent / "assets" / "reconnect_clip.mp4"

#: Delay between broadcast status polls during a restream, in seconds. A
#: takedown shows as ``revoked`` while the ingest stays open, so ffmpeg
#: never errors. One ``liveBroadcasts.list`` costs one API unit: one call
#: every two minutes per restream is 720 units a day, which leaves the
#: shared daily pool for the calls that matter (broadcast creates). The
#: alert goes to a human, so minute-scale latency costs nothing.
_STATUS_POLL_S = 120.0

#: Delay between orphan sweeps, in seconds. One sweep costs one
#: ``liveBroadcasts.list`` unit plus one ``liveStreams.list`` unit per
#: candidate, so ten minutes keeps the cost near zero.
_ORPHAN_SWEEP_INTERVAL_S = 600.0

#: How long the old broadcast keeps receiving the feed after the next one
#: starts. Both ingests hold the live feed in this window, so the new link
#: is already playing before the old one ends, and the old viewers see no
#: gap. YouTube wants a few seconds of ingest before it flips the new
#: broadcast to live, hence the margin.
_HANDOVER_OVERLAP_S = 20.0

#: Substrings that mark an ffmpeg stderr line as an output failure. The
#: caller consults them only when the capture already failed (broken pipe
#: or a bad ffmpeg exit), so a passing capture with noisy warnings never
#: alerts.
_YOUTUBE_OUTPUT_HINTS = (
    "failed",
    "failure",
    "error",
    "broken pipe",
    "connection reset",
    "connection refused",
    "timed out",
    "timeout",
    "server",
    "denied",
    "forbidden",
    "rejected",
    "closed",
    "terminated",
    "interrupted",
    "av_interleaved_write_frame",
    "error writing",
    "failed to open",
    "rtmp",
    "403",
    "401",
)


def youtube_output_hint(returncode: int | None, pipe_clean: bool, stderr_lines: list[str]) -> str | None:
    """First output-failure hint in ``stderr_lines``, or None.

    Only a failed capture asks: a clean source EOF with a zero ffmpeg exit
    never alerts, whatever the warnings say. The lines are already
    redacted, so the result is safe in a Telegram message.
    """
    if pipe_clean and returncode in (0, None):
        return None
    for line in reversed(stderr_lines):
        if any(hint in line.lower() for hint in _YOUTUBE_OUTPUT_HINTS):
            return line[:200]
    return None


class YoutubeOutputMixin:
    _config: AppConfig
    _youtube: YouTubeStreamer | None
    _notifier: Notifier | None
    _recordings: dict[str, Recording]
    _held: dict[str, HoldState]
    _orphan_first_seen: dict[str, float]
    _quick_ends: dict[str, int]
    _backoff_until: dict[str, float]
    _youtube_starts: list[float]
    _youtube_auth_alerted: bool
    _youtube_budget_lock: asyncio.Lock
    # Set by sibling mixins and Recorder (core.py). Exact call shapes so
    # a signature drift fails type checks instead of failing at runtime.
    _track: Callable[[str, Coroutine[Any, Any, Any]], asyncio.Task[Any]]
    _record_disk: Callable[[str, str, Any], Coroutine[Any, Any, None]]
    _open_feed: Callable[[Any], Coroutine[Any, Any, Any]]
    _spawn_bg: Callable[[Coroutine[Any, Any, Any]], asyncio.Task[Any]]
    _read_ffmpeg_stderr: Callable[[str, Any], Coroutine[Any, Any, list[str]]]
    _channel_dir: Callable[[str], str]

    def _note_youtube_end(self, channel: str, entry: Recording) -> None:
        """Apply quick-end backoff after a short recording.

        A stable recording clears the backoff instead.
        """
        started = entry.get("started_at")
        lifetime = time.monotonic() - started if started else None
        if lifetime is not None and lifetime < _QUICK_END_S:
            n = self._quick_ends.get(channel, 0) + 1
            self._quick_ends[channel] = n
            wait = min(_BACKOFF_BASE_S * (2 ** (n - 1)), _BACKOFF_MAX_S)
            self._backoff_until[channel] = time.monotonic() + wait
            logger.warning(
                "[recorder] [%s] Recording ended after %.0fs - backing off restarts for %ds",
                channel,
                lifetime,
                wait,
            )
        else:
            self._quick_ends.pop(channel, None)
            self._backoff_until.pop(channel, None)

    def youtube_restart_blocked_reason(self, channel: str) -> str | None:
        """Return why a youtube-mode recording for this channel must wait, or None.

        Checks the per-channel quick-end backoff and the global rolling
        24-hour broadcast budget. All re-streams share one YouTube channel.
        """
        mode = self._config.channel_output_modes.get(channel, self._config.output_mode)
        if mode not in ("youtube", "both"):
            return None
        if self._held.get(channel):
            return None  # held broadcast is reused, so no create and no quota cost
        now = time.monotonic()
        backoff = self._backoff_until.get(channel, 0.0)
        if backoff > now:
            return f"restarting in {backoff - now:.0f}s (short recording, YouTube quota guard)"
        return self._youtube_budget_blocked_reason()

    def _youtube_budget_blocked_reason(self) -> str | None:
        """Return why the rolling 24-hour budget blocks a create now, or None."""
        now_wall = time.time()
        self._youtube_starts = [t for t in self._youtube_starts if t > now_wall - _YOUTUBE_BUDGET_WINDOW_S]
        if _YOUTUBE_DAILY_BUDGET <= 0:
            # A zero budget blocks every create. Guard it here: the line
            # below would index an empty list.
            return "YouTube daily broadcast limit reached (0/0 in the last 24h)"
        if len(self._youtube_starts) >= _YOUTUBE_DAILY_BUDGET:
            wait = self._youtube_starts[0] + _YOUTUBE_BUDGET_WINDOW_S - now_wall
            return (
                f"YouTube daily broadcast limit reached "
                f"({len(self._youtube_starts)}/{_YOUTUBE_DAILY_BUDGET} in the last 24h), "
                f"next slot in {wait / 60:.0f} min"
            )
        return None

    def _require_streamer(self, channel: str) -> YouTubeStreamer:
        """Return the YouTube streamer. Raise when none is configured."""
        youtube = self._youtube
        if youtube is None:
            msg = f"YouTube streamer is not configured for {channel}"
            raise RuntimeError(msg)
        return youtube

    def _require_budget_slot(self) -> None:
        """Raise when the rolling 24-hour budget blocks a new broadcast.

        A caller holds `_youtube_budget_lock`, so the check and the record
        of the matching slot cannot interleave with another start.
        """
        blocked = self._youtube_budget_blocked_reason()
        if blocked is not None:
            msg = blocked
            raise RuntimeError(msg)

    async def _terminate(self, proc: asyncio.subprocess.Process | None, timeout: float = 10.0) -> None:
        """Stop a child process: terminate, then kill when it does not exit.

        Never raises. A process that already exited is a no-op.
        """
        if proc is None or proc.returncode is not None:
            return
        try:
            # terminate() raises ProcessLookupError when the child already
            # exited, so it belongs inside the guard.
            proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except TimeoutError, ProcessLookupError:
            try:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass

    async def _end_broadcast(self, channel: str, broadcast_id: str) -> None:
        """Move a YouTube broadcast to the complete state. Never raises."""
        try:
            await self._require_streamer(channel).end_stream(broadcast_id)
        except Exception as e:
            logger.error("[recorder] [youtube] Error ending broadcast for %s: %s", channel, e)

    def _hold_seconds(self, channel: str) -> float:
        """Return the hold delay in seconds after the source stops (0 ends now)."""
        return self._config.channel_youtube_hold_seconds.get(channel, self._config.youtube.hold_seconds)

    async def _start_keepalive(self, rtmp_url: str) -> asyncio.subprocess.Process | None:
        """Loop the bundled reconnect clip into the RTMP URL with `-c copy`.

        The feed keeps the broadcast alive during the hold. Without it,
        YouTube auto-ends a broadcast about 90 s after the encoder goes
        silent. Returns None when ffmpeg cannot spawn. The hold then
        proceeds without a keep-alive.
        """
        cmd = [
            "ffmpeg",
            "-loglevel",
            "warning",
            "-re",
            "-stream_loop",
            "-1",
            "-i",
            str(_RECONNECT_CLIP),
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-f",
            "flv",
            "-flvflags",
            "no_duration_filesize",
            rtmp_url,
        ]
        proc: asyncio.subprocess.Process | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except asyncio.CancelledError:
            # The await was cancelled, so no handle exists yet. A child that
            # was already spawned cannot be reaped here: make it traceable.
            logger.warning("[recorder] [youtube] keep-alive spawn cancelled; an ffmpeg may be orphaned")
            raise
        except Exception as e:
            logger.warning("[recorder] [youtube] keep-alive spawn failed (hold without keep-alive): %s", e)
            return None
        else:
            return proc

    async def _stop_keepalive(self, proc: asyncio.subprocess.Process | None) -> None:
        """Stop the keep-alive feed. Safe to call more than once."""
        await self._terminate(proc)

    async def _release_broadcast(self, channel: str, youtube_info: dict[str, Any] | None, entry: Recording) -> None:
        """End the broadcast now, or hold it open for the configured delay."""
        if youtube_info is None:
            return
        delay = self._hold_seconds(channel)
        if delay <= 0 or (entry.get("reused") and entry.get("failed")):
            # Zero delay means the feature is off. A reuse that failed left a
            # dead broadcast, so holding it would only loop.
            await self._end_broadcast(channel, youtube_info["broadcast_id"])
            return
        old = self._held.pop(channel, None)
        if old is not None:
            # A replaced hold loses its timer, and nothing else still knows
            # its broadcast. End it here, or it stays live on YouTube.
            end_task = old.get("end_task")
            if end_task is not None:
                end_task.cancel()
            await self._end_broadcast(channel, old["youtube_info"]["broadcast_id"])
        hold: HoldState = {"youtube_info": youtube_info, "end_task": None, "keepalive": None}
        self._held[channel] = hold
        hold["end_task"] = asyncio.create_task(self._hold_then_end(channel, delay, hold))
        logger.info(
            "[recorder] [youtube] %s broadcast %s held for %.0fs (streamer may return)",
            channel,
            youtube_info["broadcast_id"],
            delay,
        )

    async def _hold_then_end(self, channel: str, delay: float, hold: HoldState) -> None:
        """Feed the held broadcast for the delay, or end early if the feed dies.

        This task ends its own broadcast. A cancellation only drops the hold
        and stops the keep-alive: the canceller took the hold over (reuse), or
        it ends the broadcast itself (close).
        """
        keepalive: asyncio.subprocess.Process | None = None
        sleep_task: asyncio.Task[Any] | None = None
        ka_task: asyncio.Task[Any] | None = None
        try:
            keepalive = await self._start_keepalive(hold["youtube_info"]["rtmp_url"])
            hold["keepalive"] = keepalive
            sleep_task = asyncio.create_task(asyncio.sleep(delay))
            ka_task = asyncio.create_task(keepalive.wait()) if keepalive is not None else None
            done, _ = await asyncio.wait(
                [t for t in (sleep_task, ka_task) if t is not None],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if ka_task is not None and ka_task in done:
                # The keep-alive died early, so the broadcast is gone. End it now.
                if self._held.get(channel) is hold:
                    self._held.pop(channel, None)
                await self._stop_keepalive(keepalive)
                logger.warning("[recorder] [youtube] %s keep-alive feed stopped early, ending broadcast", channel)
                sleep_task.cancel()  # the hold is over; do not leave the timer pending
                await asyncio.gather(sleep_task, return_exceptions=True)
                await self._end_broadcast(channel, hold["youtube_info"]["broadcast_id"])
                return
            if ka_task is not None:
                ka_task.cancel()  # the feed outlived the hold; stop watching it
                await asyncio.gather(ka_task, return_exceptions=True)
            if self._held.get(channel) is not hold:
                await self._stop_keepalive(keepalive)
                return  # a new stream consumed the hold while we slept
            self._held.pop(channel, None)
            await self._stop_keepalive(keepalive)
            await self._end_broadcast(channel, hold["youtube_info"]["broadcast_id"])
        except asyncio.CancelledError:
            for t in (sleep_task, ka_task):
                if t is not None:
                    t.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                # A cancelled task that nobody awaits warns at teardown.
                await asyncio.gather(*(t for t in (sleep_task, ka_task) if t is not None), return_exceptions=True)
            if self._held.get(channel) is hold:
                # Nobody took the hold over, so drop it: the next start must
                # not reuse a broadcast that no task feeds any more.
                self._held.pop(channel, None)
            await self._stop_keepalive(keepalive)
            raise

    async def _stream_youtube(
        self,
        channel: str,
        author: str,
        title: str,
        game: str,
        stream: Any,
        filepath: str | None,
        notify: bool = True,
        youtube_notify: bool = True,
    ) -> None:
        entry = self._recordings.get(channel)
        if entry is None:
            return
        held = self._held.pop(channel, None)
        if held is not None:
            end_task = held.get("end_task")
            if end_task is not None:
                end_task.cancel()
            await self._stop_keepalive(held.get("keepalive"))
            youtube_info = held["youtube_info"]
            entry["youtube_info"] = youtube_info
            entry["reused"] = True
            logger.info("[recorder] [youtube] %s reusing held broadcast %s", channel, youtube_info["broadcast_id"])
        else:
            try:
                youtube_info = await self._create_broadcast(channel, author, title, game)
            except Exception as e:
                logger.error("[recorder] [youtube] Failed to create YouTube stream: %s", e)
                if "stream-archive-setup-youtube" in str(e):
                    set_degraded("youtube_auth", "YouTube authentication is dead")
                    if not self._youtube_auth_alerted:
                        self._youtube_auth_alerted = True
                        msg = (
                            f"\u26a0\ufe0f YouTube authentication is dead.\n"
                            f"Channel: {channel}\n"
                            "Restreaming is stalled: run stream-archive-setup-youtube again."
                        )
                        if self._notifier:
                            try:
                                await self._notifier.notify(msg)
                            except Exception:
                                self._youtube_auth_alerted = False
                                logger.error("[recorder] auth notification failed for %s", channel, exc_info=True)
                    raise
                if "rate limit" in str(e).lower() or "403" in str(e) or "quota" in str(e).lower():
                    msg = (
                        f"\u26a0\ufe0f YouTube rate limit reached!\n"
                        f"Channel: {channel}\n"
                        f"Stream: {sanitize_metadata_text(title or 'Unknown')}\n"
                        f"Stream link: {channel_url(channel)}"
                    )
                    if self._notifier:
                        try:
                            await self._notifier.notify(msg)
                        except Exception:
                            logger.error("[recorder] rate-limit notification failed for %s", channel, exc_info=True)
                    entry = self._recordings.get(channel)
                    if entry is not None:
                        recording_dir = str(disk.channel_recording_dir(self._config, self._channel_dir(channel)))
                        os.makedirs(recording_dir, exist_ok=True)
                        now = datetime.now(ZoneInfo(self._config.timezone)).strftime("%d_%m_%Y-%H%M%S")
                        safe_title = sanitize_filename(f"{author} - {title}")
                        filepath = os.path.join(recording_dir, f"{safe_title}-{now}.ts")
                        entry["filepath"] = filepath
                        logger.info("[recorder] Rate limited - falling back to disk recording for %s", channel)
                        disk_task = self._track(channel, self._record_disk(channel, filepath, stream))
                        entry["tasks"].append(disk_task)
                        if self._notifier and notify:
                            try:
                                await self._notifier.notify_live(channel, title, game, channel_url(channel))
                            except Exception:
                                logger.error("[recorder] live notification failed for %s", channel, exc_info=True)
                    return
                raise
            self._youtube_auth_alerted = False
            clear_degraded("youtube_auth")
            entry = self._recordings.get(channel)
            if entry is None:
                # The recording vanished while the broadcast was created, so
                # nothing can feed it. End it here: a broadcast left live and
                # unowned costs quota and shows on the channel.
                await self._end_broadcast(channel, youtube_info["broadcast_id"])
                return
            entry["youtube_info"] = youtube_info
        entry["part_started_at"] = time.monotonic()

        if self._notifier and youtube_notify:
            try:
                await self._notifier.notify_live(
                    channel, title, game, channel_url(channel), youtube_info["youtube_url"]
                )
            except Exception:
                logger.error("[recorder] live notification failed for %s", channel, exc_info=True)
        await self._capture_and_restream(channel, author, title, game, stream, filepath)

    async def _create_broadcast(self, channel: str, author: str, title: str, game: str) -> dict[str, Any]:
        """Create one broadcast and record its budget slot.

        The lock covers the budget check, the create, and the record, so
        concurrent starts cannot all pass the check before any of them
        records its slot.
        """
        youtube = self._require_streamer(channel)
        async with self._youtube_budget_lock:
            self._require_budget_slot()
            youtube_info: dict[str, Any] = await youtube.create_stream(author, title, channel, game)
            self._youtube_starts.append(time.time())  # count fresh creates only
        return youtube_info

    def _part_seconds(self) -> float:
        """Length of one broadcast of a split restream, in seconds.

        ``0`` keeps one broadcast for the whole stream. The value comes
        from the live config, so a reload applies at the next boundary.
        """
        hours = self._config.youtube.max_broadcast_hours
        return hours * 3600.0 if hours > 0 else 0.0

    async def _capture_and_restream(
        self,
        channel: str,
        author: str,
        title: str,
        game: str,
        stream: Any,
        filepath: str | None,
    ) -> None:
        """Read the source once, and split the broadcast when the part ends.

        The read loop copies the source into the disk file and into the
        stdin of the current ffmpeg. At a split boundary the next broadcast
        is created and its ffmpeg joins the live feed. Both ingests then
        receive every chunk for a short overlap, and the old broadcast ends
        after that. The source is never re-opened, so the disk file has no
        gap, and the new link is live before the old one ends.
        """
        try:
            fd = await self._open_feed(stream)
        except Exception as e:
            logger.error("[recorder] [youtube] %s stream open failed: %s", channel, e)
            msg = f"[youtube] {channel} stream interrupted"
            raise RuntimeError(msg) from e

        abandoned = asyncio.Event()
        stop = asyncio.Event()
        # Live feeders of the current broadcast. A handover adds one and the
        # overlap task drops the old one.
        feeders: list[Any] = []
        # One stderr drain for each feeder, paired with the feeder it reads.
        stderr_drains: list[tuple[Any, asyncio.Task[Any]]] = []
        status_task: asyncio.Task[Any] | None = None
        file_handle: Any = None
        youtube_error: str | None = None
        clean = False
        stderr_lines: list[str] = []
        last_proc: Any = None
        exit_codes: dict[int, int | None] = {}
        loop = asyncio.get_running_loop()
        try:
            if filepath:
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                file_handle = open(filepath, "wb")  # noqa: SIM115 - closed in the finally below
            entry = self._recordings.get(channel)
            if entry is None:
                return
            first = await self._spawn_feeder(channel, entry.get("youtube_info"))
            if first is None:
                return
            feeders.append(first)
            stderr_drains.append((first, asyncio.create_task(self._read_ffmpeg_stderr(channel, first))))
            status_task = asyncio.create_task(
                self._watch_broadcast_status(channel, title, filepath is not None, abandoned, stop)
            )
            clean, youtube_error = await self._feed(
                channel, author, title, game, fd, file_handle, feeders, stderr_drains, abandoned
            )
        finally:
            stop.set()
            if status_task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await asyncio.gather(status_task, return_exceptions=True)
            # Snapshot the exit code first: our own terminate would replace a
            # running feeder's None with a signal code and fake a failure.
            exit_codes = {id(proc): getattr(proc, "returncode", None) for proc in feeders}
            for proc in list(feeders):
                await self._terminate(proc)
            for index, (proc, task) in enumerate(stderr_drains):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    result = (await asyncio.gather(task, return_exceptions=True))[0]
                    # The last feeder is the current part: its stderr names
                    # the cause when ffmpeg died.
                    if index == len(stderr_drains) - 1:
                        last_proc = proc
                        if isinstance(result, list):
                            stderr_lines = result
            if file_handle is not None:
                with contextlib.suppress(Exception):
                    await loop.run_in_executor(None, file_handle.close)
            with contextlib.suppress(Exception):
                await loop.run_in_executor(None, fd.close)
            logger.info("[recorder] [youtube] %s ffmpeg stopped", channel)

        reason = youtube_output_hint(exit_codes.get(id(last_proc)), clean, stderr_lines)
        if youtube_error is not None or reason is not None:
            entry = self._recordings.get(channel) or {}
            await self._notify_youtube_failure(
                channel,
                entry.get("youtube_info") or {},
                title,
                youtube_error or reason or "the ffmpeg ingest stopped",
                filepath is not None,
            )
            if not clean:
                msg = f"[youtube] {channel} stream interrupted"
                raise RuntimeError(msg)
            return
        if abandoned.is_set():
            # The takedown alert already went out. End normally: the disk
            # file finalizes, and the monitor does not restart a restream
            # of flagged content.
            logger.info("[recorder] [youtube] %s stopped feeding the revoked broadcast", channel)
            return
        if not clean:
            msg = f"[youtube] {channel} stream interrupted"
            raise RuntimeError(msg)

    async def _feed(
        self,
        channel: str,
        author: str,
        title: str,
        game: str,
        fd: Any,
        file_handle: Any,
        feeders: list[Any],
        stderr_drains: list[tuple[Any, asyncio.Task[Any]]],
        abandoned: asyncio.Event,
    ) -> tuple[bool, str | None]:
        """Copy the source into the disk file and the live ingest pipes.

        Return ``(clean, youtube_error)``. ``clean`` is True only when the
        source reached EOF. A broken ingest drops that feeder: with a disk
        file the copy goes on to the end, and ``youtube_error`` names the
        failure. A source read failure returns ``(False, None)`` and keeps
        every byte written so far.
        """
        loop = asyncio.get_running_loop()
        ingest_error: str | None = None
        entry = self._recordings.get(channel)
        part_end = self._part_end(entry)
        # The task that builds the next part. A boundary waits for it, so
        # one split cannot start a create on every loop pass.
        split_task: asyncio.Task[Any] | None = None
        while True:
            if abandoned.is_set() and feeders:
                for proc in list(feeders):
                    self._close_stdin(proc)
                feeders.clear()
            if split_task is not None and split_task.done():
                split_task = None
                entry = self._recordings.get(channel) or entry
                part_end = self._part_end(entry)
            if part_end is not None and split_task is None and time.monotonic() >= part_end and not abandoned.is_set():
                split_task = self._spawn_bg(self._start_next_part(channel, author, title, game, feeders, stderr_drains))
            try:
                data = await loop.run_in_executor(None, fd.read, 65536)
            except Exception as e:
                logger.error("[recorder] [youtube] %s read error: %s", channel, e)
                return (False, ingest_error)
            if not data:
                return (True, ingest_error)
            if file_handle is not None:
                # The archive volume can be slow. Write on the executor.
                await loop.run_in_executor(None, file_handle.write, data)
            broken: list[Any] = []
            for proc in feeders:
                stdin = getattr(proc, "stdin", None)
                if stdin is None:
                    broken.append(proc)
                    continue
                try:
                    stdin.write(data)
                    await stdin.drain()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
                    ingest_error = f"YouTube ingest failed: {e}"
                    broken.append(proc)
                    logger.warning("[recorder] [youtube] %s %s", channel, ingest_error)
                except Exception as e:
                    ingest_error = f"YouTube ingest failed: {e}"
                    broken.append(proc)
                    logger.error("[recorder] [youtube] %s %s", channel, ingest_error)
            for proc in broken:
                if proc in feeders:
                    feeders.remove(proc)
                self._close_stdin(proc)
            if not feeders and file_handle is None:
                return (False, ingest_error or "ffmpeg is not running")

    def _part_end(self, entry: Any) -> float | None:
        """Monotonic time of the next split boundary, or None when off."""
        part = self._part_seconds()
        if part <= 0 or entry is None:
            return None
        started = entry.get("part_started_at") or entry.get("started_at")
        if started is None:
            return None
        return float(started) + part

    async def _start_next_part(
        self,
        channel: str,
        author: str,
        title: str,
        game: str,
        feeders: list[Any],
        stderr_drains: list[tuple[Any, asyncio.Task[Any]]],
    ) -> Any:
        """Create the next broadcast and add its feeder to the live feed.

        Return the new process, or None when the split did not happen. A
        failed create leaves the current broadcast running: the next
        boundary tries again.
        """
        entry = self._recordings.get(channel)
        if entry is None:
            return None
        old_info = entry.get("youtube_info") or {}
        try:
            new_info = await self._create_broadcast(channel, author, title, game)
            proc = await self._spawn_feeder(channel, new_info)
        except Exception as e:
            logger.error("[recorder] [youtube] %s split failed, keeping the broadcast: %s", channel, e)
            return None
        if proc is None:
            logger.error("[recorder] [youtube] %s split could not start ffmpeg", channel)
            with contextlib.suppress(Exception):
                await self._end_broadcast(channel, new_info["broadcast_id"])
            return None
        entry["youtube_info"] = new_info
        entry["part_started_at"] = time.monotonic()
        feeders.append(proc)
        stderr_drains.append((proc, asyncio.create_task(self._read_ffmpeg_stderr(channel, proc))))
        logger.info(
            "[recorder] [youtube] %s next part started on %s (overlap %.0fs)",
            channel,
            new_info["youtube_url"],
            _HANDOVER_OVERLAP_S,
        )
        # The feeders before the new one are the parts that just ended. The
        # overlap task drops them after the window.
        old_procs = [p for p in feeders if p is not proc]
        for proc in old_procs:
            feeders.remove(proc)
        self._spawn_bg(self._schedule_part_end(channel, old_info.get("broadcast_id"), old_procs))
        if self._notifier:
            try:
                await self._notifier.notify(
                    f"\U0001f500 {channel}: long restream split into a new broadcast.\n"
                    f"New YouTube link: {new_info['youtube_url']}\n"
                    f"The previous link ends in {_HANDOVER_OVERLAP_S:.0f}s. "
                    "The recording continues without a gap."
                )
            except Exception:
                logger.error("[recorder] split notification failed for %s", channel, exc_info=True)
        return proc

    async def _schedule_part_end(self, channel: str, old_broadcast_id: str | None, old_procs: list[Any]) -> None:
        """End the old broadcast and its feeders after the overlap.

        The wait runs as a task, so the feed loop keeps copying to the new
        ingest at once. A failed end is logged and never reaches the loop.
        """
        await asyncio.sleep(_HANDOVER_OVERLAP_S)
        for proc in old_procs:
            await self._terminate(proc)
        if old_broadcast_id:
            await self._end_broadcast(channel, old_broadcast_id)
        logger.info("[recorder] [youtube] %s previous part ended (%s)", channel, old_broadcast_id)

    @staticmethod
    def _close_stdin(proc: Any) -> None:
        """Close one feeder's stdin, so its ffmpeg sees EOF and exits."""
        stdin = getattr(proc, "stdin", None)
        if stdin is not None:
            with suppress(Exception):
                stdin.close()

    async def _spawn_feeder(self, channel: str, youtube_info: dict[str, Any] | None) -> Any:
        """Start the ffmpeg that feeds one ingest. None when ffmpeg is missing."""
        rtmp_url = (youtube_info or {}).get("rtmp_url")
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
        logger.info("[recorder] [youtube] Starting ffmpeg for %s", channel)
        return await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )

    async def _notify_youtube_failure(
        self, channel: str, youtube_info: dict[str, Any], title: str | None, reason: str, has_disk: bool
    ) -> None:
        """Tell the operator that the restream died. Never raises.

        A takedown closes the RTMP connection mid-capture, so the disk
        file of ``both`` mode also stops here and the monitor restarts.
        The lines stay short and the cause stays redacted: ffmpeg can
        echo the ingest URL, which holds the stream key.
        """
        if self._notifier is None:
            return
        safe_title = sanitize_metadata_text(title or "Unknown")
        youtube_url = (youtube_info or {}).get("youtube_url") or ""
        lines = [
            f"\u26a0\ufe0f YouTube restream failed for {channel}.",
            f"Stream: {safe_title}",
            f"Source: {channel_url(channel)}",
        ]
        if youtube_url:
            lines.append(f"YouTube: {youtube_url}")
        lines.append(f"Cause: {sanitize_metadata_text(reason)}")
        if has_disk:
            lines.append("The disk file stopped here and is finalized. The monitor restarts automatically.")
        else:
            lines.append("There is no local backup in youtube mode.")
        lines.append("Check YouTube Studio for a copyright or policy notice.")
        try:
            await self._notifier.notify("\n".join(lines))
        except Exception:
            logger.error("[recorder] restream-failure notification failed for %s", channel, exc_info=True)

    async def _watch_broadcast_status(
        self,
        channel: str,
        title: str | None,
        has_disk: bool,
        abandoned: asyncio.Event,
        stop: asyncio.Event,
    ) -> None:
        """Alert once when YouTube revokes the broadcast mid-restream. Never raises.

        A takedown flips ``lifeCycleStatus`` to ``revoked`` while the RTMP
        ingest stays open, so the pipe and ffmpeg both look healthy. This
        poll catches that state and marks the broadcast abandoned, so the
        feed drops the ingest: in ``both`` mode the disk file carries on
        alone, and the recording ends normally instead of restarting a
        restream of flagged content. Other states need no alert: ``live``
        is normal, and a missing broadcast is logged at debug level until
        the capture ends.

        The broadcast id is read from the entry on every tick: a split
        replaces it, and the old id answers ``complete`` once it ends.
        """
        streamer = self._youtube
        if streamer is None:
            return
        watched: str | None = None
        try:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=_STATUS_POLL_S)
                except TimeoutError:
                    pass
                else:
                    return
                entry = self._recordings.get(channel)
                youtube_info = (entry or {}).get("youtube_info") or {}
                broadcast_id = youtube_info.get("broadcast_id")
                if not broadcast_id:
                    continue
                if broadcast_id != watched:
                    # A new part: its first poll happens on the next tick, so
                    # a fresh broadcast has time to reach ``live``.
                    watched = broadcast_id
                    continue
                try:
                    status = await streamer.get_broadcast_status(broadcast_id)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.debug("[recorder] [youtube] %s broadcast status check failed: %s", channel, e)
                    continue
                if status == "revoked":
                    abandoned.set()
                    await self._notify_youtube_takedown(channel, youtube_info, title, has_disk)
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("[recorder] broadcast watch failed for %s", channel, exc_info=True)

    def _known_broadcast_ids(self) -> set[str]:
        """Broadcast ids that a recording or a hold still owns.

        A split moves the entry to the new id at once, and a hold keeps
        its id here until it ends or a new stream takes it over. Any
        other active broadcast is not ours to keep alive.
        """
        known: set[str] = set()
        for entry in self._recordings.values():
            info = entry.get("youtube_info") or {}
            if info.get("broadcast_id"):
                known.add(info["broadcast_id"])
        for hold in self._held.values():
            info = hold.get("youtube_info") or {}
            if info.get("broadcast_id"):
                known.add(info["broadcast_id"])
        return known

    async def cleanup_orphan_broadcasts(self) -> int:
        """End broadcasts that a crash or a failed end call left live.

        A broadcast is a candidate when all of these hold: this app made
        it (the description marker), no recording or hold owns it, and
        its ingest has no data. The sweep ends a candidate only after it
        stays a candidate for ``orphan_cleanup_hours``, so a broadcast
        in setup or in a short gap is safe. The marker guards streams
        from other tools. The wait guards the rest: a held broadcast is
        owned, and its keep-alive feed reads as active in any case.
        Return the count ended. Never raises.
        """
        streamer = self._youtube
        hours = self._config.youtube.orphan_cleanup_hours
        if streamer is None or hours <= 0:
            return 0
        try:
            broadcasts = await streamer.list_active_broadcasts()
        except Exception:
            logger.error("[recorder] [youtube] orphan sweep list failed", exc_info=True)
            return 0
        known = self._known_broadcast_ids()
        now = time.monotonic()
        cutoff = hours * 3600
        seen_now: set[str] = set()
        ended = 0
        for item in broadcasts:
            try:
                broadcast_id = item.get("id")
                if not isinstance(broadcast_id, str) or broadcast_id in known:
                    continue
                snippet = item.get("snippet") or {}
                if RECORDED_BY_MARKER not in str(snippet.get("description") or ""):
                    continue
                details = item.get("contentDetails") or {}
                stream_id = details.get("boundStreamId")
                if not isinstance(stream_id, str) or not stream_id:
                    continue
                stream_status, health = await streamer.get_stream_health(stream_id)
                if stream_status != "inactive" or health != "noData":
                    continue
                seen_now.add(broadcast_id)
                first_seen = self._orphan_first_seen.setdefault(broadcast_id, now)
                if now - first_seen < cutoff:
                    continue
                await streamer.end_stream(broadcast_id)
                ended += 1
                logger.info(
                    "[recorder] [youtube] ended orphan broadcast %s (no owner, no data for over %.0f h)",
                    broadcast_id,
                    hours,
                )
                if self._notifier:
                    with contextlib.suppress(Exception):
                        await self._notifier.notify(
                            f"\U0001f9f9 Ended an orphaned YouTube broadcast: https://www.youtube.com/watch?v={broadcast_id}\n"
                            "It had no recording and no data on its ingest."
                        )
            except Exception:
                logger.error("[recorder] [youtube] orphan sweep failed for one broadcast", exc_info=True)
        # Forget candidates that left the list or found an owner, so a
        # later orphan with a long life does not inherit an old clock.
        self._orphan_first_seen = {bid: ts for bid, ts in self._orphan_first_seen.items() if bid in seen_now}
        return ended

    async def run_orphan_cleanup_loop(self) -> None:
        """Run the orphan sweep on its interval until the task is cancelled."""
        while True:
            await asyncio.sleep(_ORPHAN_SWEEP_INTERVAL_S)
            with contextlib.suppress(Exception):
                await self.cleanup_orphan_broadcasts()

    async def _notify_youtube_takedown(
        self, channel: str, youtube_info: dict[str, Any], title: str | None, has_disk: bool
    ) -> None:
        """Tell the operator that YouTube took the restream down. Never raises.

        The pipe drops the dead ingest when this goes out: in ``both`` mode
        the disk file carries on alone, in ``youtube`` mode the recording
        ends with nothing saved. Either way no new broadcast starts, so a
        flagged restream cannot loop into fresh strikes.
        """
        if self._notifier is None:
            return
        safe_title = sanitize_metadata_text(title or "Unknown")
        youtube_url = (youtube_info or {}).get("youtube_url") or ""
        lines = [
            f"\u26a0\ufe0f YouTube took down the restream for {channel}.",
            f"Stream: {safe_title}",
            f"Source: {channel_url(channel)}",
        ]
        if youtube_url:
            lines.append(f"YouTube: {youtube_url}")
        lines.append("The broadcast is revoked. The app stopped feeding it.")
        if has_disk:
            lines.append("The disk recording continues.")
        else:
            lines.append("There is no local backup in youtube mode, so the recording ended.")
        lines.append("Check YouTube Studio for the copyright or policy notice.")
        try:
            await self._notifier.notify("\n".join(lines))
        except Exception:
            logger.error("[recorder] takedown notification failed for %s", channel, exc_info=True)

    def youtube_active_count(self) -> int:
        """Active recordings whose mode uses a YouTube re-stream (for the uplink cap)."""
        return sum(1 for e in self._recordings.values() if e.get("mode") in ("youtube", "both"))
