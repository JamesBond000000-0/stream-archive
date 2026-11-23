"""Telegram live streamer: RTMP join, call lifecycle, and channel auto-create.

The client is a fake at the Telethon boundary. It records the requests
it gets and answers with the shapes the real API returns, so the tests
prove the request flow without a network or an account.
"""

import asyncio
import types

import pytest
from conftest import make_config as valid_config
from conftest import read_file

from stream_archive.telegram_streamer import TelegramLiveStreamer, join_rtmp_url, marked_peer_id


def make_config(tmp_path, **overrides):
    """Build a valid config bound to tmp_path, with Telegram live on."""
    live = {"enabled": True, "api_id": 123, "api_hash": "hash"}
    live.update(overrides.pop("telegram_live", {}))
    cfg = valid_config(telegram_live=live, **overrides)
    cfg._workdir = tmp_path
    cfg._config_path = tmp_path / "config.json"
    return cfg


class FakeTelethonClient:
    """Telethon client stand-in: records requests, answers canned results."""

    def __init__(self, *args):
        self.requests = []
        self.authorized = True
        self.disconnected = False

    def is_connected(self):
        return not self.disconnected

    async def connect(self):
        return None

    async def disconnect(self):
        self.disconnected = True

    async def is_user_authorized(self):
        return self.authorized

    async def get_input_entity(self, peer_id):
        return ("input-peer", peer_id)

    async def __call__(self, request):
        self.requests.append(request)
        name = type(request).__name__
        if name == "GetGroupCallStreamRtmpUrlRequest":
            return types.SimpleNamespace(url="rtmps://dc1-1.rtmp.t.me/s/", key="KEY123")
        if name == "CreateGroupCallRequest":
            call = types.SimpleNamespace(id=42, access_hash=99)
            return types.SimpleNamespace(updates=[types.SimpleNamespace(call=call)])
        if name == "CreateChannelRequest":
            return types.SimpleNamespace(chats=[types.SimpleNamespace(id=777)])
        if name == "ToggleGroupCallRecordRequest":
            return types.SimpleNamespace()
        if name == "DiscardGroupCallRequest":
            return types.SimpleNamespace()
        msg = f"unexpected request: {name}"
        raise AssertionError(msg)


def make_streamer(tmp_path, **overrides):
    cfg = make_config(tmp_path, **overrides)
    client = FakeTelethonClient()
    streamer = TelegramLiveStreamer(cfg, client_factory=lambda *a: client)
    return streamer, client, cfg


@pytest.mark.parametrize(
    ("url", "key", "expected"),
    [
        ("rtmps://dc1-1.rtmp.t.me/s/", "KEY", "rtmps://dc1-1.rtmp.t.me/s/KEY"),
        ("rtmps://dc1-1.rtmp.t.me/s", "KEY", "rtmps://dc1-1.rtmp.t.me/s/KEY"),
        ("rtmps://dc1-1.rtmp.t.me/s/KEY", "KEY", "rtmps://dc1-1.rtmp.t.me/s/KEY"),
    ],
)
def test_join_rtmp_url_forms(url, key, expected):
    assert join_rtmp_url(url, key) == expected


def test_marked_peer_id():
    assert marked_peer_id(777) == -100777
    assert marked_peer_id(1234567890) == -1001234567890


def test_create_stream_joins_rtmp_and_returns_call_ids(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})

    async def scenario():
        return await streamer.create_stream("author", "Title", "twitch:ch", "Game")

    info = asyncio.run(scenario())
    assert info["rtmp_url"] == "rtmps://dc1-1.rtmp.t.me/s/KEY123"
    assert info["call_id"] == 42
    assert info["access_hash"] == 99
    assert info["peer_id"] == -100777
    assert info["telegram_url"] == "https://t.me/c/777"
    names = [type(r).__name__ for r in client.requests]
    assert names == ["GetGroupCallStreamRtmpUrlRequest", "CreateGroupCallRequest"]
    create = client.requests[1]
    assert create.rtmp_stream is True
    assert create.title == "author - Title"
    # The TL schema packs random_id as a signed 32-bit int; a wider value
    # fails serialization before the request ever leaves the process.
    assert -(2**31) <= create.random_id <= 2**31 - 1


def test_end_stream_discards_the_call(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path)

    async def scenario():
        await streamer.end_stream({"peer_id": -100777, "call_id": 42, "access_hash": 99})

    asyncio.run(scenario())
    assert [type(r).__name__ for r in client.requests] == ["DiscardGroupCallRequest"]
    call = client.requests[0].call
    assert call.id == 42
    assert call.access_hash == 99


def test_ensure_destination_uses_the_stored_peer(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100555})

    async def scenario():
        return await streamer.ensure_destination("twitch:ch")

    assert asyncio.run(scenario()) == -100555
    assert client.requests == []


def test_ensure_destination_auto_creates_and_stores_the_peer(tmp_path):
    """A first stream creates one private channel and saves its peer id.

    The save must land in config.json: a restart that lost it would
    create a second channel for the same source.
    """
    streamer, client, cfg = make_streamer(tmp_path)

    async def scenario():
        return await streamer.ensure_destination("twitch:ch")

    peer = asyncio.run(scenario())
    assert peer == -100777
    assert cfg.channel_telegram_peers == {"twitch:ch": -100777}
    assert read_file(tmp_path)["channel_telegram_peers"] == {"twitch:ch": -100777}
    creates = [r for r in client.requests if type(r).__name__ == "CreateChannelRequest"]
    assert len(creates) == 1
    assert creates[0].broadcast is True
    assert creates[0].title == "ch restream"


def test_ensure_destination_without_auto_create_fails(tmp_path):
    streamer, _client, _cfg = make_streamer(tmp_path, telegram_live={"auto_create_channels": False})

    async def scenario():
        return await streamer.ensure_destination("twitch:ch")

    with pytest.raises(RuntimeError, match="No Telegram channel is mapped"):
        asyncio.run(scenario())


def test_connect_rejects_a_session_that_is_not_logged_in(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path)
    client.authorized = False

    async def scenario():
        return await streamer.connect()

    with pytest.raises(RuntimeError, match="stream-archive-setup-telegram-live"):
        asyncio.run(scenario())
