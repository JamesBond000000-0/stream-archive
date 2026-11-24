import asyncio
import time

from conftest import make_config as valid_config

from stream_archive.recorder import Recorder
from stream_archive.recorder.common import RECORDED_BY_MARKER


def make_recorder(tmp_path, streamer, **youtube_overrides):
    """Build a Recorder on a valid config with the YouTube double attached."""
    youtube = youtube_overrides or {"orphan_cleanup_hours": 3}
    cfg = valid_config(channels=["twitch:ch"], recording_dir=str(tmp_path), youtube=youtube)
    cfg._workdir = tmp_path
    cfg._config_path = tmp_path / "config.json"
    return Recorder(cfg, youtube_streamer=streamer)


def broadcast(broadcast_id, stream_id="s1", *, marker=True):
    """One active-broadcast item in the shape the API list returns."""
    description = (
        f"Twitch stream by author\nGame: game\nOriginally streamed at: https://twitch.tv/ch\n{RECORDED_BY_MARKER}"
        if marker
        else "made by another tool"
    )
    return {
        "id": broadcast_id,
        "snippet": {"description": description},
        "contentDetails": {"boundStreamId": stream_id},
    }


class FakeStreamer:
    """YouTube double: serves canned broadcasts and records the ends."""

    def __init__(self, broadcasts, health=None):
        self.broadcasts = broadcasts
        self.health = health or {}  # stream id -> (streamStatus, health)
        self.ended = []
        self.list_calls = 0

    async def list_active_broadcasts(self):
        self.list_calls += 1
        return self.broadcasts

    async def get_stream_health(self, stream_id):
        return self.health.get(stream_id, ("inactive", "noData"))

    async def end_stream(self, broadcast_id):
        self.ended.append(broadcast_id)


def test_sweep_ends_a_long_orphan_and_keeps_the_rest(tmp_path):
    """Only the ownerless, marker-carrying, data-less broadcast ends.

    The rest guard real cases: a live recording, a held broadcast, a
    broadcast from another tool, and one whose ingest still has data.
    The first sweep only starts the clock: the end needs the full wait,
    so a broadcast in setup or in a short gap is safe.
    """
    streamer = FakeStreamer(
        [
            broadcast("orphan", "s-orphan"),
            broadcast("owned-rec", "s-rec"),
            broadcast("owned-hold", "s-hold"),
            broadcast("foreign", "s-foreign", marker=False),
            broadcast("flowing", "s-flowing"),
        ],
        health={"s-flowing": ("active", "good")},
    )
    rec = make_recorder(tmp_path, streamer)
    rec._recordings["twitch:ch"] = {"youtube_info": {"broadcast_id": "owned-rec"}}
    rec._held["twitch:ch"] = {"youtube_info": {"broadcast_id": "owned-hold"}}

    assert asyncio.run(rec.cleanup_orphan_broadcasts()) == 0
    assert streamer.ended == []
    assert set(rec._orphan_first_seen) == {"orphan"}

    rec._orphan_first_seen["orphan"] = time.monotonic() - 4 * 3600
    assert asyncio.run(rec.cleanup_orphan_broadcasts()) == 1
    assert streamer.ended == ["orphan"]


def test_sweep_does_nothing_when_disabled(tmp_path):
    """orphan_cleanup_hours of 0 turns the sweep off before any API call."""
    streamer = FakeStreamer([broadcast("orphan")])
    rec = make_recorder(tmp_path, streamer, orphan_cleanup_hours=0)

    assert asyncio.run(rec.cleanup_orphan_broadcasts()) == 0
    assert streamer.list_calls == 0
    assert streamer.ended == []


def test_sweep_forgets_a_candidate_that_finds_an_owner(tmp_path):
    """A candidate that a recording takes over loses its clock.

    Without the prune, the same broadcast could end at once if it ever
    lost its owner again, long after the first sighting.
    """
    streamer = FakeStreamer([broadcast("adopted")])
    rec = make_recorder(tmp_path, streamer)

    assert asyncio.run(rec.cleanup_orphan_broadcasts()) == 0
    assert "adopted" in rec._orphan_first_seen

    rec._recordings["twitch:ch"] = {"youtube_info": {"broadcast_id": "adopted"}}
    rec._orphan_first_seen["adopted"] = time.monotonic() - 4 * 3600
    assert asyncio.run(rec.cleanup_orphan_broadcasts()) == 0
    assert streamer.ended == []
    assert rec._orphan_first_seen == {}
