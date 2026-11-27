"""Archive mode: playlist recovery, the ledger, and the run logic.

The pure helpers are tested on the shapes a live check against the
real Twitch CDN produced. The service runs against fakes at the
Twitch, Telegram, and recorder boundaries plus an httpx mock
transport for the CDN, so no network or account takes part. The
ledger methods run against a fake at the Telethon boundary, like
the other Telegram streamer tests.
"""

import asyncio
import types

import httpx
import pytest
from conftest import make_config as valid_config
from conftest import read_file

from stream_archive.archive import (
    ArchiveService,
    build_playlist,
    format_ledger_entry,
    parse_helix_duration,
    parse_ledger_ids,
    parse_vod_location,
)
from stream_archive.recorder import Recorder
from stream_archive.telegram import TelegramController
from stream_archive.telegram_streamer import TelegramLiveStreamer, marked_peer_id

THUMB = (
    "https://static-cdn.jtvnw.net/cf_vods/d3vd9lfkzbru3h/"
    "9b5f2164cdef782a1096_twitchreo_316776008178_1795692555//thumb/thumb0-320x180.jpg"
)

PLAYLIST = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXTINF:10.0,
0-unmuted.ts
#EXTINF:10.0,
1-unmuted.ts
#EXTINF:10.0,
2.ts
#EXT-X-ENDLIST
"""


def make_config(tmp_path, **overrides):
    """Build a valid config bound to tmp_path."""
    cfg = valid_config(**overrides)
    cfg._workdir = tmp_path
    cfg._config_path = tmp_path / "config.json"
    return cfg


# ---------------------------------------------------------------------------
# Playlist recovery helpers
# ---------------------------------------------------------------------------


def test_parse_vod_location_reads_cdn_and_base():
    location = parse_vod_location(THUMB)
    assert location is not None
    assert location.cdn_host == "d3vd9lfkzbru3h.cloudfront.net"
    assert location.base == "9b5f2164cdef782a1096_twitchreo_316776008178_1795692555"


@pytest.mark.parametrize(
    "url",
    [
        # A VOD Twitch still processes has this placeholder thumbnail.
        "https://vod-secure.twitch.tv/_404/404_processing_320x180.png",
        "",
        "https://example.com/no/cf_vods-here.png",
    ],
)
def test_parse_vod_location_rejects_urls_without_a_storage_path(url):
    assert parse_vod_location(url) is None


def test_build_playlist_swaps_only_unavailable_unmuted_segments():
    text, swapped = build_playlist(PLAYLIST, "https://cdn.example/base/chunked", {"0-unmuted.ts"})
    assert swapped == 1
    lines = text.splitlines()
    # Header lines pass through untouched.
    assert lines[0] == "#EXTM3U"
    assert "#EXT-X-ENDLIST" in lines
    # Segment lines become absolute. The gone unmuted segment swaps
    # for its muted twin; the live one keeps its name.
    segments = [line for line in lines if line.startswith("https://")]
    assert segments == [
        "https://cdn.example/base/chunked/0-muted.ts",
        "https://cdn.example/base/chunked/1-unmuted.ts",
        "https://cdn.example/base/chunked/2.ts",
    ]


def test_ledger_entry_round_trip_marks_the_vod_done():
    entry = format_ledger_entry(
        vod_id="2941241699",
        login="twitchreo",
        title="Stream",
        created_at="2026-11-26T11:29:21Z",
        youtube_urls=["https://www.youtube.com/watch?v=abc"],
    )
    other = format_ledger_entry(
        vod_id="123",
        login="someone",
        title="Two parts",
        created_at="2026-11-20T00:00:00Z",
        youtube_urls=["https://www.youtube.com/watch?v=a", "https://www.youtube.com/watch?v=b"],
    )
    ids = parse_ledger_ids([entry, "a stray message\nvod: not-a-number", other])
    assert ids == {"2941241699", "123"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [("3h35m39s", 12939.0), ("12h0m0s", 43200.0), ("0h4m9s", 249.0), ("", 0.0), ("soon", 0.0)],
)
def test_parse_helix_duration(text, expected):
    assert parse_helix_duration(text) == expected


# ---------------------------------------------------------------------------
# The archive service, at its boundaries
# ---------------------------------------------------------------------------


class FakeTwitch:
    def __init__(self, videos):
        self._videos = videos

    async def resolve_user_ids(self, names):
        return {names[0]: "42"}

    async def list_videos(self, user_id):
        return list(self._videos)


class FakeLedger:
    enabled = True

    def __init__(self, texts=()):
        self.texts = list(texts)
        self.posted = []

    async def read_ledger_texts(self):
        return list(self.texts)

    async def post_ledger_entry(self, text):
        self.posted.append(text)
        self.texts.append(text)


class FakeNotifier:
    def __init__(self):
        self.messages = []

    async def notify(self, text):
        self.messages.append(text)


class FakeRecorder:
    """Stands in for the recorder's archive entry point."""

    def __init__(self, *, sleep_s=0.0, failed=False):
        self.calls = []
        self.sleep_s = sleep_s
        self.failed = failed

    async def start_archive(self, key, author, title, game, stream, *, part_seconds=0):
        self.calls.append({"key": key, "title": title, "part_seconds": part_seconds})
        entry = {"youtube_parts": ["https://www.youtube.com/watch?v=abc"], "failed": self.failed}

        async def run():
            if self.sleep_s:
                await asyncio.sleep(self.sleep_s)
            return None

        return asyncio.create_task(run()), entry

    def recording_info(self):
        return []


class FakeStreamFactory:
    """Captures the playlist file the service built, before it is deleted."""

    def __init__(self):
        self.playlists = {}

    def __call__(self, playlist_path, log_path):
        self.playlists[playlist_path.name] = playlist_path.read_text()
        return object()


def vod(vod_id, created_at, duration="0h0m1s"):
    return {
        "id": vod_id,
        "title": f"Stream {vod_id}",
        "created_at": created_at,
        "duration": duration,
        "thumbnail_url": (
            f"https://static-cdn.jtvnw.net/cf_vods/shard{vod_id}/hash_someone_111_{vod_id}//thumb/thumb0-320x180.jpg"
        ),
    }


def make_service(tmp_path, *, videos, ledger_texts=(), recorder=None, gone=("0-unmuted.ts",)):
    def handler(request):
        path = request.url.path
        if path.endswith("/index-dvr.m3u8"):
            return httpx.Response(200, text=PLAYLIST)
        name = path.rsplit("/", 1)[-1]
        if name in gone:
            return httpx.Response(403, text="gone")
        return httpx.Response(206, content=b"x")

    factory = FakeStreamFactory()
    ledger = FakeLedger(ledger_texts)
    notifier = FakeNotifier()
    rec = recorder if recorder is not None else FakeRecorder(sleep_s=1.0)
    service = ArchiveService(
        make_config(tmp_path, archive={"enabled": True}),
        FakeTwitch(videos),
        rec,
        ledger,
        notifier,
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        stream_factory=factory,
    )
    return service, ledger, notifier, factory, rec


def run_to_finish(service):
    """Start a run and wait for it, inside one event loop."""

    async def scenario():
        reply = await service.start("someone")
        await service._task
        return reply

    return asyncio.run(scenario())


def test_run_archives_only_vods_missing_from_the_ledger(tmp_path):
    # Helix lists newest first. The ledger already holds VOD 2.
    videos = [vod("3", "2026-11-26T00:00:00Z"), vod("2", "2026-11-25T00:00:00Z"), vod("1", "2026-11-24T00:00:00Z")]
    done = format_ledger_entry(
        vod_id="2",
        login="someone",
        title="Stream 2",
        created_at="2026-11-25T00:00:00Z",
        youtube_urls=["https://www.youtube.com/watch?v=old"],
    )
    service, ledger, notifier, factory, recorder = make_service(tmp_path, videos=videos, ledger_texts=[done])

    reply = run_to_finish(service)
    assert "started" in reply

    # Oldest first, and VOD 2 skipped: the ledger's whole point.
    assert [call["key"] for call in recorder.calls] == ["archive:1", "archive:3"]
    assert parse_ledger_ids(ledger.posted) == {"1", "3"}
    # The playlist of VOD 1 carries the muted fallback for the gone
    # unmuted segment and keeps the live one.
    built = factory.playlists["1.m3u8"]
    assert "/chunked/0-muted.ts" in built
    assert "/chunked/1-unmuted.ts" in built
    assert any("Archived someone" in message for message in notifier.messages)


def test_run_keeps_a_failed_vod_out_of_the_ledger(tmp_path):
    videos = [vod("7", "2026-11-24T00:00:00Z")]
    service, ledger, notifier, _, _ = make_service(
        tmp_path, videos=videos, recorder=FakeRecorder(sleep_s=1.0, failed=True)
    )

    run_to_finish(service)

    assert ledger.posted == []
    assert any("did not finish clean" in message for message in notifier.messages)


def test_run_keeps_a_cut_short_vod_out_of_the_ledger(tmp_path):
    # The capture ends at once but the VOD runs 10 minutes: the feed
    # broke early, and a stub archive must not mark the VOD done.
    videos = [vod("8", "2026-11-24T00:00:00Z", duration="0h10m0s")]
    service, ledger, _, _, _ = make_service(tmp_path, videos=videos, recorder=FakeRecorder())

    run_to_finish(service)

    assert ledger.posted == []


def test_start_refuses_when_archive_mode_is_off(tmp_path):
    service = ArchiveService(
        make_config(tmp_path),
        FakeTwitch([]),
        FakeRecorder(),
        FakeLedger(),
        FakeNotifier(),
        httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(404))),
    )
    assert "off" in asyncio.run(service.start("someone"))


def test_start_refuses_without_a_telegram_ledger(tmp_path):
    service = ArchiveService(
        make_config(tmp_path, archive={"enabled": True}),
        FakeTwitch([]),
        FakeRecorder(),
        None,
        FakeNotifier(),
        httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(404))),
    )
    assert "telegram_live" in asyncio.run(service.start("someone"))


def test_start_refuses_a_second_run(tmp_path):
    videos = [vod("9", "2026-11-24T00:00:00Z", duration="0h0m0s")]
    service, _, _, _, _ = make_service(tmp_path, videos=videos, recorder=FakeRecorder(sleep_s=0.5))

    async def scenario():
        first = await service.start("someone")
        second = await service.start("someone")
        await service._task
        return first, second

    first, second = asyncio.run(scenario())
    assert "started" in first
    assert "already going" in second


# ---------------------------------------------------------------------------
# Recorder seam: the archive flags on a capture entry
# ---------------------------------------------------------------------------


class FakeYouTubeStreamer:
    def __init__(self):
        self.ended = []

    async def end_stream(self, broadcast_id):
        self.ended.append(broadcast_id)


def test_archive_capture_ends_its_broadcast_at_once_despite_a_hold(tmp_path):
    # A hold keeps a live broadcast open for a streamer who may
    # return. An archive VOD never returns, so its broadcast ends
    # at once even with a hold configured.
    config = make_config(tmp_path, youtube={"hold_seconds": 300})
    youtube = FakeYouTubeStreamer()
    recorder = Recorder(config, youtube_streamer=youtube)
    entry = {"archive": True}

    asyncio.run(recorder._release_broadcast("archive:1", {"broadcast_id": "b1"}, entry))

    assert youtube.ended == ["b1"]
    assert "archive:1" not in recorder._held


def test_part_end_prefers_the_capture_own_split_length(tmp_path):
    config = make_config(tmp_path)  # youtube.max_broadcast_hours is 0: live split off
    recorder = Recorder(config)
    entry = {"part_seconds": 60.0, "part_started_at": 1000.0}
    assert recorder._part_end(entry) == 1060.0
    assert recorder._part_end({"part_started_at": 1000.0}) is None


# ---------------------------------------------------------------------------
# Ledger methods, at the Telethon boundary
# ---------------------------------------------------------------------------


class FakeTelethonClient:
    """Telethon stand-in for the ledger channel: create, read, post."""

    def __init__(self, *args):
        self.created = []
        self.sent = []
        self.messages = []
        self.disconnected = False

    def is_connected(self):
        return not self.disconnected

    async def connect(self):
        return None

    async def disconnect(self):
        self.disconnected = True

    async def is_user_authorized(self):
        return True

    async def __call__(self, request):
        self.created.append(request)
        chat = types.SimpleNamespace(id=777)
        return types.SimpleNamespace(chats=[chat])

    async def iter_messages(self, peer, limit=None):
        for text in self.messages:
            yield types.SimpleNamespace(message=text)

    async def send_message(self, peer, text):
        self.sent.append((peer, text))
        self.messages.insert(0, text)
        return types.SimpleNamespace(id=len(self.messages))


def make_streamer(tmp_path, **overrides):
    live = {"enabled": True, "api_id": 123, "api_hash": "hash"}
    cfg = make_config(tmp_path, telegram_live=live, **overrides)
    clients = []

    def factory(*args):
        client = FakeTelethonClient(*args)
        clients.append(client)
        return client

    return TelegramLiveStreamer(cfg, client_factory=factory), cfg, clients


def test_ledger_channel_is_created_once_and_its_peer_is_stored(tmp_path):
    streamer, cfg, clients = make_streamer(tmp_path)

    async def scenario():
        first = await streamer.ensure_ledger_channel()
        second = await streamer.ensure_ledger_channel()
        return first, second

    first, second = asyncio.run(scenario())
    assert first == second == marked_peer_id(777)
    assert len(clients[0].created) == 1
    # The peer lands in config.json at once, so a restart reuses it.
    assert read_file(tmp_path)["archive"]["ledger_peer_id"] == marked_peer_id(777)


class FakeArchiveService:
    def __init__(self):
        self.started = []

    async def start(self, username):
        self.started.append(username)
        return f"Archive run started for {username}."

    def status(self):
        return "No archive run is going."


def test_archive_command_forwards_the_username_to_the_service(tmp_path):
    service = FakeArchiveService()
    ctrl = TelegramController(
        make_config(tmp_path),
        object(),
        object(),
        object(),
        archive_service=service,
    )

    async def scenario():
        started = await ctrl.handle_archive(["SomeOne"])
        status = await ctrl.handle_archive([])
        return started, status

    started, status = asyncio.run(scenario())
    assert service.started == ["SomeOne"]
    assert started == "Archive run started for SomeOne."
    assert "No archive run is going." in status
    assert "Usage: /archive" in status


def test_ledger_read_and_post_use_the_stored_peer(tmp_path):
    streamer, cfg, clients = make_streamer(tmp_path, archive={"ledger_peer_id": marked_peer_id(555)})

    async def scenario():
        client = await streamer.connect()
        client.messages = ["vod: 42\nchannel: someone", "noise"]
        texts = await streamer.read_ledger_texts()
        await streamer.post_ledger_entry("vod: 43\nchannel: someone")
        return texts

    texts = asyncio.run(scenario())
    assert parse_ledger_ids(texts) == {"42"}
    assert clients[0].sent == [(marked_peer_id(555), "vod: 43\nchannel: someone")]
    assert clients[0].created == []
