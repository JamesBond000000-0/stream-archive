"""Archive mode: restream a channel's past broadcasts to YouTube.

The ``/archive`` command drives this module. For one Twitch channel
it lists the past broadcasts, skips the ones the Telegram ledger
already holds, and restreams each remaining VOD to YouTube in real
time. The capture reuses the recorder's YouTube output, so the split
chain, the creation budget, and the failure alerts match a live
restream. Only the source differs: an ffmpeg process reads the
rebuilt playlist at native speed and writes MPEG-TS to a pipe.

A VOD's playlist comes from the recovery layout on the Twitch CDN.
The VOD thumbnail URL names the CDN host and the storage path. The
DVR playlist under that path lists the original segments. A segment
Twitch muted for the public player appears as ``<n>-unmuted.ts``
there, while the public playlist serves ``<n>-muted.ts`` instead.
Twitch deletes the original segment of a muted range soon after the
mute, so the module probes each unmuted segment and falls back to
the muted segment where the original is gone. The archive then
always holds the full timeline, with the original audio wherever
Twitch still serves it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import subprocess
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from stream_archive.config import AppConfig
    from stream_archive.notifier import Notifier
    from stream_archive.recorder import Recorder
    from stream_archive.telegram_streamer import TelegramLiveStreamer
    from stream_archive.twitch_api import TwitchAPI

logger = logging.getLogger(__name__)

#: The thumbnail URL of a VOD names its CDN shard and storage path:
#: ``cf_vods/<shard>/<hash>_<login>_<stream id>_<start unix>/...``.
_THUMB_RE = re.compile(r"cf_vods/([^/]+)/([^/]+)/")

#: One ledger entry starts with this line. The id is the Twitch VOD id.
_LEDGER_ID_RE = re.compile(r"^vod: (\d+)\s*$", re.MULTILINE)

_DURATION_RE = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")

#: How many segment probes run at once during a playlist rebuild.
_PROBE_CONCURRENCY = 8

#: How long the service waits for the recorder to release a finished
#: capture before it reads the entry's final state anyway.
_RELEASE_TIMEOUT_S = 180.0


@dataclass(frozen=True)
class VodLocation:
    """Where one VOD lives on the Twitch CDN."""

    cdn_host: str
    base: str


def parse_vod_location(thumbnail_url: str) -> VodLocation | None:
    """Read the CDN location out of a VOD thumbnail URL.

    None means the thumbnail carries no storage path. A VOD that
    Twitch still processes has a placeholder thumbnail, so its
    archive waits for a later run.
    """
    match = _THUMB_RE.search(thumbnail_url or "")
    if match is None:
        return None
    return VodLocation(cdn_host=f"{match.group(1)}.cloudfront.net", base=match.group(2))


def dvr_playlist_url(location: VodLocation, quality: str = "chunked") -> str:
    """URL of the DVR playlist of one quality of a VOD."""
    return f"https://{location.cdn_host}/{location.base}/{quality}/index-dvr.m3u8"


def playlist_segments(playlist_text: str) -> list[str]:
    """Segment file names of a media playlist, in order."""
    return [line.strip() for line in playlist_text.splitlines() if line.strip() and not line.startswith("#")]


def build_playlist(playlist_text: str, base_url: str, unavailable_unmuted: set[str]) -> tuple[str, int]:
    """Rewrite a DVR playlist as a local file with absolute URLs.

    Each unavailable unmuted segment swaps for its muted twin, which
    the public player serves and Twitch keeps. Returns the new text
    and the swap count.
    """
    out: list[str] = []
    swapped = 0
    for raw in playlist_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            out.append(raw)
            continue
        name = line.rsplit("/", 1)[-1]
        if name in unavailable_unmuted and name.endswith("-unmuted.ts"):
            name = name[: -len("-unmuted.ts")] + "-muted.ts"
            swapped += 1
        out.append(f"{base_url}/{name}")
    return "\n".join(out) + "\n", swapped


def format_ledger_entry(*, vod_id: str, login: str, title: str, created_at: str, youtube_urls: list[str]) -> str:
    """One ledger message for a finished VOD.

    The first line is the machine-readable marker the reader scans
    for. The rest is for the human who opens the channel.
    """
    lines = [f"vod: {vod_id}", f"channel: {login}", f"title: {title}", f"date: {created_at}"]
    if len(youtube_urls) == 1:
        lines.append(f"youtube: {youtube_urls[0]}")
    else:
        lines.extend(f"youtube part {index}: {url}" for index, url in enumerate(youtube_urls, 1))
    return "\n".join(lines)


def parse_ledger_ids(texts: Iterable[str]) -> set[str]:
    """VOD ids a set of ledger message texts marks as finished."""
    ids: set[str] = set()
    for text in texts:
        ids.update(_LEDGER_ID_RE.findall(text))
    return ids


def parse_helix_duration(text: str) -> float:
    """Seconds in a Helix duration string such as ``3h35m39s``."""
    match = _DURATION_RE.fullmatch((text or "").strip())
    if match is None:
        return 0.0
    hours, minutes, seconds = (int(group) if group else 0 for group in match.groups())
    return hours * 3600 + minutes * 60 + seconds


class _PlaylistReader:
    """File-like reader over one ffmpeg process.

    The recorder's feed loop uses only ``read`` and ``close``. Close
    ends the process: a capture that stops early must not leave an
    ffmpeg behind.
    """

    def __init__(self, proc: subprocess.Popen[bytes], log_file: Any) -> None:
        self._proc = proc
        self._log_file = log_file

    def read(self, size: int) -> bytes:
        stdout = self._proc.stdout
        if stdout is None:
            return b""
        return stdout.read(size)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            if self._proc.stdout is not None:
                self._proc.stdout.close()
        if self._proc.poll() is None:
            self._proc.terminate()
            with contextlib.suppress(Exception):
                self._proc.wait(timeout=5)
            if self._proc.poll() is None:
                with contextlib.suppress(Exception):
                    self._proc.kill()
        with contextlib.suppress(Exception):
            self._log_file.close()


class PlaylistStream:
    """A stream-shaped source that opens ffmpeg on a playlist file.

    ffmpeg reads the HLS playlist at native speed (``-re``), copies
    the tracks, and writes MPEG-TS to stdout. The recorder opens the
    stream on an executor thread, so ``open`` may block while the
    process starts.
    """

    def __init__(self, playlist_path: Path, log_path: Path) -> None:
        self._playlist_path = playlist_path
        self._log_path = log_path

    def open(self) -> _PlaylistReader:
        log_file = self._log_path.open("ab")
        try:
            proc = subprocess.Popen(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "warning",
                    # The playlist is a local file, so ffmpeg starts
                    # with a file-only protocol whitelist. The segment
                    # URLs inside are https, so name them here or
                    # every segment is refused.
                    "-protocol_whitelist",
                    "file,http,https,tcp,tls,crypto,data",
                    "-re",
                    "-i",
                    str(self._playlist_path),
                    "-c",
                    "copy",
                    "-f",
                    "mpegts",
                    "pipe:1",
                ],
                stdout=subprocess.PIPE,
                stderr=log_file,
            )
        except BaseException:
            log_file.close()
            raise
        return _PlaylistReader(proc, log_file)


#: Builds the stream object for one rebuilt playlist. The default is
#: PlaylistStream. Tests pass a fake that needs no ffmpeg.
StreamFactory = Callable[[Path, Path], Any]


class ArchiveService:
    """Runs archive jobs, one at a time, behind the /archive command."""

    def __init__(
        self,
        config: AppConfig,
        twitch_api: TwitchAPI,
        recorder: Recorder,
        telegram_streamer: TelegramLiveStreamer | None,
        notifier: Notifier | None,
        http: httpx.AsyncClient,
        stream_factory: StreamFactory | None = None,
    ) -> None:
        self._config = config
        self._twitch = twitch_api
        self._recorder = recorder
        self._telegram = telegram_streamer
        self._notifier = notifier
        self._http = http
        self._stream_factory: StreamFactory = stream_factory or (lambda playlist, log: PlaylistStream(playlist, log))
        self._task: asyncio.Task[None] | None = None
        self._run_login: str | None = None
        self._current: str | None = None

    @property
    def running(self) -> bool:
        """True while an archive run is in flight."""
        return self._task is not None and not self._task.done()

    def status(self) -> str:
        """One line on the current run, for the /archive reply."""
        if not self.running:
            return "No archive run is going."
        return f"Archive run for {self._run_login} is going: {self._current or 'starting'}."

    async def start(self, username: str) -> str:
        """Start a run for one Twitch channel. Returns the reply text."""
        login = username.strip().lstrip("@").lower()
        if not login:
            return "Usage: /archive <twitch username>"
        if not self._config.archive.enabled:
            return "Archive mode is off. Set archive.enabled to true in the config first."
        if self._telegram is None or not self._telegram.enabled:
            return "Archive mode needs its Telegram ledger, so telegram_live must be on and logged in first."
        if self.running:
            return f"An archive run is already going for {self._run_login}. {self.status()}"
        self._run_login = login
        self._current = None
        self._task = asyncio.create_task(self._run(login))
        return f"Archive run started for {login}. I report each VOD as it finishes."

    async def _notify(self, text: str) -> None:
        if self._notifier is None:
            return
        with contextlib.suppress(Exception):
            await self._notifier.notify(text)

    async def _run(self, login: str) -> None:
        telegram = self._telegram
        if telegram is None:
            return  # start() already refused this run
        try:
            ids = await self._twitch.resolve_user_ids([login])
            user_id = ids.get(login)
            if user_id is None:
                await self._notify(f"\u274c Archive: Twitch user {login} was not found.")
                return
            done = parse_ledger_ids(await telegram.read_ledger_texts())
            videos = await self._twitch.list_videos(user_id)
            # Oldest first: the YouTube channel then reads in broadcast
            # order, and a capped run takes the oldest gap first.
            pending = [vod for vod in reversed(videos) if str(vod.get("id")) not in done]
            cap = self._config.archive.max_vods_per_run
            if cap > 0:
                pending = pending[:cap]
            total_s = sum(parse_helix_duration(str(vod.get("duration") or "")) for vod in pending)
            await self._notify(
                f"\U0001f3ac Archive {login}: {len(videos)} VODs on Twitch, {len(done)} in the ledger, "
                f"{len(pending)} to archive (about {total_s / 3600:.1f} hours of streaming)."
            )
            archived = 0
            failed = 0
            for vod in pending:
                if await self._archive_one(login, vod):
                    archived += 1
                else:
                    failed += 1
            await self._notify(
                f"\U0001f3c1 Archive {login} done: {archived} archived, {failed} failed. "
                "Failed VODs stay out of the ledger, so the next run retries them."
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[archive] run for %s failed: %s", login, e, exc_info=True)
            await self._notify(f"\u274c Archive run for {login} stopped early: {e}")
        finally:
            self._current = None

    async def _archive_one(self, login: str, vod: dict[str, Any]) -> bool:
        """Archive one VOD. True only when the ledger entry is written."""
        telegram = self._telegram
        if telegram is None:
            return False
        vod_id = str(vod.get("id") or "")
        title = str(vod.get("title") or "Untitled")
        created_at = str(vod.get("created_at") or "")
        expected_s = parse_helix_duration(str(vod.get("duration") or ""))
        self._current = f"{login} VOD {vod_id} ({title})"
        key = f"archive:{vod_id}"
        playlist_path: Path | None = None
        try:
            location = parse_vod_location(str(vod.get("thumbnail_url") or ""))
            if location is None:
                logger.warning("[archive] VOD %s has no CDN path in its thumbnail; skipping for now", vod_id)
                await self._notify(
                    f"\u23ed\ufe0f Archive {login}: VOD {vod_id} ({title}) has no storage path yet. "
                    "Twitch may still process it. A later run retries it."
                )
                return False
            built = await self._build_playlist(location, vod_id)
            if built is None:
                await self._notify(
                    f"\u274c Archive {login}: VOD {vod_id} ({title}) playlist did not load. It may be gone."
                )
                return False
            playlist_path, swapped = built
            stream = self._stream_factory(playlist_path, playlist_path.with_suffix(".ffmpeg.log"))
            part_seconds = self._config.archive.split_hours * 3600.0
            started = await self._recorder.start_archive(
                key, login, title, "Unknown", stream, part_seconds=part_seconds
            )
            if started is None:
                await self._notify(f"\u274c Archive {login}: VOD {vod_id} ({title}) capture did not start.")
                return False
            task, entry = started
            began = time.monotonic()
            with contextlib.suppress(Exception):
                await task
            await self._wait_released(key)
            elapsed = time.monotonic() - began
            parts = list(entry.get("youtube_parts") or [])
            failed = bool(entry.get("failed")) or not parts
            if not failed and expected_s > 0 and elapsed < expected_s * 0.9:
                # A clean EOF far before the VOD's length means the
                # feed broke early (a segment vanished mid-run). The
                # YouTube archive would be a stub, so keep the VOD
                # out of the ledger and let a later run redo it.
                logger.warning(
                    "[archive] VOD %s ended after %.0fs of %.0fs; treating it as failed", vod_id, elapsed, expected_s
                )
                failed = True
            if failed:
                await self._notify(
                    f"\u26a0\ufe0f Archive {login}: VOD {vod_id} ({title}) did not finish clean. "
                    "It stays out of the ledger, so the next run retries it."
                )
                return False
            await telegram.post_ledger_entry(
                format_ledger_entry(vod_id=vod_id, login=login, title=title, created_at=created_at, youtube_urls=parts)
            )
            note = f" {swapped} muted segments kept their public audio." if swapped else ""
            await self._notify(
                f"\u2705 Archived {login}: {title} ({created_at[:10]})\n{parts[0]}"
                + (f"\n{len(parts)} parts in total." if len(parts) > 1 else "")
                + note
            )
        except Exception as e:
            logger.error("[archive] VOD %s failed: %s", vod_id, e, exc_info=True)
            await self._notify(f"\u274c Archive {login}: VOD {vod_id} ({title}) failed: {e}")
            return False
        else:
            return True
        finally:
            if playlist_path is not None:
                with contextlib.suppress(OSError):
                    playlist_path.unlink()
                with contextlib.suppress(OSError):
                    playlist_path.with_suffix(".ffmpeg.log").unlink()

    async def _wait_released(self, key: str) -> None:
        """Wait until the recorder releases a finished capture's entry.

        The capture task ends first. The finalize that ends the
        broadcast and settles the entry runs right after it, and the
        parts list is final only then.
        """
        deadline = time.monotonic() + _RELEASE_TIMEOUT_S
        while time.monotonic() < deadline:
            if not any(info.get("channel") == key for info in self._recorder.recording_info()):
                return
            await asyncio.sleep(2)

    async def _build_playlist(self, location: VodLocation, vod_id: str) -> tuple[Path, int] | None:
        """Fetch the DVR playlist, probe it, and write the local copy.

        Returns the file path and the muted-fallback swap count.
        None means the playlist did not load.
        """
        quality = "chunked"
        try:
            resp = await self._http.get(dvr_playlist_url(location, quality), timeout=30)
        except httpx.HTTPError as e:
            logger.warning("[archive] playlist fetch failed for VOD %s: %s", vod_id, e)
            return None
        if resp.status_code != 200:
            logger.warning("[archive] playlist for VOD %s answered %d", vod_id, resp.status_code)
            return None
        segments = playlist_segments(resp.text)
        if not segments:
            logger.warning("[archive] playlist for VOD %s lists no segments", vod_id)
            return None
        unmuted = [name for name in segments if name.endswith("-unmuted.ts")]
        unavailable: set[str] = set()
        if unmuted:
            unavailable = await self._probe_unavailable(location, quality, unmuted)
        base_url = f"https://{location.cdn_host}/{location.base}/{quality}"
        body, swapped = build_playlist(resp.text, base_url, unavailable)
        tmp_dir = self._config.workdir / "archive"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        path = tmp_dir / f"{vod_id}.m3u8"
        path.write_text(body, encoding="utf-8")
        if swapped:
            logger.info(
                "[archive] VOD %s: %d of %d unmuted segments are gone; the muted twins take their place",
                vod_id,
                swapped,
                len(unmuted),
            )
        return path, swapped

    async def _probe_unavailable(self, location: VodLocation, quality: str, names: list[str]) -> set[str]:
        """Names from ``names`` that the CDN does not serve any more.

        A one-byte range read is the probe. Any failure counts as
        gone: the muted twin is the safe fallback, and a probe that
        failed on a network error still leaves a complete archive.
        """
        semaphore = asyncio.Semaphore(_PROBE_CONCURRENCY)

        async def probe(name: str) -> bool:
            url = f"https://{location.cdn_host}/{location.base}/{quality}/{name}"
            async with semaphore:
                try:
                    resp = await self._http.get(url, headers={"Range": "bytes=0-0"}, timeout=30)
                except httpx.HTTPError:
                    return False
                return resp.status_code in (200, 206)

        results = await asyncio.gather(*(probe(name) for name in names))
        return {name for name, available in zip(names, results, strict=True) if not available}
