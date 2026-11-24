"""Telegram live streamer: RTMP join, call lifecycle, and channel auto-create.

The client is a fake at the Telethon boundary. It records the requests
it gets and answers with the shapes the real API returns, so the tests
prove the request flow without a network or an account.
"""

import asyncio
import re
import types

import pytest
from conftest import make_config as valid_config
from conftest import read_file

import stream_archive.telegram_streamer as streamer_module
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
        self.sent_files = []
        self.uploads = []
        self.saved_messages = []
        self.forwarded = []
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

    async def upload_file(self, file, file_name=None):
        data = file.read()
        self.uploads.append((file_name, data))
        return ("uploaded", file_name)

    async def send_file(self, entity, file=None, caption=None):
        self.sent_files.append((entity, file, caption))
        return types.SimpleNamespace(id=1)

    async def get_messages(self, entity, limit=None):
        msgs = sorted(self.saved_messages, key=lambda m: m.id, reverse=True)
        return msgs[:limit] if limit else msgs

    async def forward_messages(self, entity, messages=None, from_peer=None):
        self.forwarded.append((entity, messages, from_peer))
        return types.SimpleNamespace(id=9001)

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
        if name == "EditPhotoRequest":
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
    streamer, client, _cfg = make_streamer(
        tmp_path, channel_telegram_peers={"ch": -100777}, telegram_live={"title_timestamp": False}
    )

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


def test_create_stream_title_carries_the_start_timestamp(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})

    async def scenario():
        return await streamer.create_stream("author", "Title", "twitch:ch", "Game")

    asyncio.run(scenario())
    create = [r for r in client.requests if type(r).__name__ == "CreateGroupCallRequest"][0]
    assert re.fullmatch(r"author - Title \(\d{4}-\d{2}-\d{2} \d{2}:\d{2} [A-Z]+\)", create.title)


_PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"fake-png-payload"


def _icon_streamer(tmp_path, monkeypatch, url="https://cdn.example/icon.png", **overrides):
    """Streamer with a resolver and a download that answer one PNG icon."""
    streamer, client, cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777}, **overrides)

    async def resolve(channel):
        assert channel == "twitch:ch"
        return url

    async def download(got_url):
        assert got_url == url
        return _PNG_BYTES

    streamer.set_profile_image_resolver(resolve)
    monkeypatch.setattr(streamer_module, "_download_image", download)
    return streamer, client, cfg


def test_create_stream_sets_the_channel_icon_once(tmp_path, monkeypatch):
    streamer, client, cfg = _icon_streamer(tmp_path, monkeypatch)

    async def scenario():
        await streamer.create_stream("author", "Title", "twitch:ch", "Game")
        await streamer.create_stream("author", "Title", "twitch:ch", "Game")

    asyncio.run(scenario())
    edits = [r for r in client.requests if type(r).__name__ == "EditPhotoRequest"]
    assert len(edits) == 1
    assert edits[0].channel == ("input-peer", -100777)
    assert edits[0].photo.file == ("uploaded", "icon.png")
    assert client.uploads == [("icon.png", _PNG_BYTES)]
    assert cfg.channel_telegram_icon_urls == {"twitch:ch": "https://cdn.example/icon.png"}
    assert read_file(tmp_path)["channel_telegram_icon_urls"] == {"twitch:ch": "https://cdn.example/icon.png"}


def test_create_stream_skips_the_icon_when_the_url_is_unchanged(tmp_path, monkeypatch):
    streamer, client, _cfg = _icon_streamer(
        tmp_path, monkeypatch, channel_telegram_icon_urls={"ch": "https://cdn.example/icon.png"}
    )

    async def download(url):  # must not run: the stored URL already matches
        msg = "download called for an unchanged icon"
        raise AssertionError(msg)

    monkeypatch.setattr(streamer_module, "_download_image", download)

    async def scenario():
        return await streamer.create_stream("author", "Title", "twitch:ch", "Game")

    asyncio.run(scenario())
    assert [r for r in client.requests if type(r).__name__ == "EditPhotoRequest"] == []


def test_create_stream_survives_a_failing_icon_lookup(tmp_path, monkeypatch):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})

    async def resolve(channel):
        msg = "platform down"
        raise RuntimeError(msg)

    streamer.set_profile_image_resolver(resolve)

    async def scenario():
        return await streamer.create_stream("author", "Title", "twitch:ch", "Game")

    info = asyncio.run(scenario())
    assert info["call_id"] == 42
    assert [r for r in client.requests if type(r).__name__ == "EditPhotoRequest"] == []


def test_as_png_or_jpeg_passes_png_and_rejects_garbage(tmp_path):
    assert asyncio.run(streamer_module._as_png_or_jpeg(_PNG_BYTES)) == _PNG_BYTES
    assert asyncio.run(streamer_module._as_png_or_jpeg(b"not an image at all")) is None


def test_post_chat_log_sends_the_file_to_the_mapped_peer(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})

    async def scenario():
        return await streamer.post_chat_log("twitch:ch", "/data/chat.chat.json", "Chat log: Title")

    assert asyncio.run(scenario()) is True
    assert client.sent_files == [(("input-peer", -100777), "/data/chat.chat.json", "Chat log: Title")]


def test_create_stream_with_record_stores_the_saved_messages_baseline(tmp_path):
    streamer, client, _cfg = make_streamer(
        tmp_path, channel_telegram_peers={"ch": -100777}, telegram_live={"record": True}
    )
    client.saved_messages = [types.SimpleNamespace(id=7, media=None)]

    async def scenario():
        return await streamer.create_stream("author", "Title", "twitch:ch", "Game")

    info = asyncio.run(scenario())
    assert info["record_baseline_id"] == 7


def _saved_recording(msg_id, *, duration=100.0, size=1000, mime="video/mp4", landed=None):
    """One Saved Messages message in the shape a recording lands in."""
    doc = types.SimpleNamespace(mime_type=mime, size=size, attributes=[types.SimpleNamespace(duration=duration)])
    return types.SimpleNamespace(id=msg_id, media=object(), document=doc, date=landed)


def test_forward_recording_forwards_the_matching_video(tmp_path):
    streamer, client, cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})
    client.saved_messages = [
        _saved_recording(8, mime="audio/ogg", size=900),  # the audio twin: never forwarded
        _saved_recording(9, duration=100.0, size=1000),
        _saved_recording(10, duration=400.0, size=2000),  # another stream's recording
    ]

    async def scenario():
        return await streamer.forward_recording("twitch:ch", 7, expected_duration_s=100.0, timeout_s=1, poll_s=0.01)

    result = asyncio.run(scenario())
    assert result == {"message_id": 9001, "telegram_url": "https://t.me/c/777/9001"}
    assert client.forwarded == [(("input-peer", -100777), 9, "me")]
    assert cfg.telegram_forwarded_recording_keys == ["1000:100"]
    assert read_file(tmp_path)["telegram_forwarded_recording_keys"] == ["1000:100"]


def test_forward_recording_times_out_without_a_recording(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})
    client.saved_messages = [types.SimpleNamespace(id=8, media=None)]

    async def scenario():
        return await streamer.forward_recording("twitch:ch", 7, timeout_s=0, poll_s=0.01)

    assert asyncio.run(scenario()) is None
    assert client.forwarded == []


def test_forward_recording_rejects_a_duration_mismatch(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})
    client.saved_messages = [_saved_recording(9, duration=900.0, size=5000)]

    async def scenario():
        return await streamer.forward_recording("twitch:ch", 7, expected_duration_s=100.0, timeout_s=0, poll_s=0.01)

    assert asyncio.run(scenario()) is None
    assert client.forwarded == []


def test_forward_recording_skips_an_already_forwarded_file(tmp_path):
    streamer, client, _cfg = make_streamer(
        tmp_path, channel_telegram_peers={"ch": -100777}, telegram_forwarded_recording_keys=["1000:100"]
    )
    # A second delivery of the same recording: same size and duration.
    client.saved_messages = [_saved_recording(9, duration=100.0, size=1000)]

    async def scenario():
        return await streamer.forward_recording("twitch:ch", 7, expected_duration_s=100.0, timeout_s=0, poll_s=0.01)

    assert asyncio.run(scenario()) is None
    assert client.forwarded == []


def test_forward_recording_ignores_files_that_landed_before_the_end(tmp_path):
    from datetime import UTC, datetime, timedelta

    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"ch": -100777})
    ended = datetime(2026, 11, 24, 5, 0, tzinfo=UTC)
    client.saved_messages = [_saved_recording(9, duration=100.0, landed=ended - timedelta(hours=1))]

    async def scenario():
        return await streamer.forward_recording(
            "twitch:ch", 7, expected_duration_s=100.0, ended_at=ended.timestamp(), timeout_s=0, poll_s=0.01
        )

    assert asyncio.run(scenario()) is None
    assert client.forwarded == []


def test_forward_recording_leaves_an_ambiguous_file_to_neither_channel(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path, channel_telegram_peers={"a": -100777, "b": -100888})
    client.saved_messages = [_saved_recording(9, duration=100.0, size=1000)]
    # Another channel is already waiting for a recording of the same
    # length: the one file on hand matches both claims, so it is
    # ambiguous and must go to neither.
    streamer._forward_claims["twitch:b"] = (102.0, None)

    async def scenario():
        return await streamer.forward_recording("twitch:a", 7, expected_duration_s=100.0, timeout_s=0, poll_s=0.01)

    assert asyncio.run(scenario()) is None
    assert client.forwarded == []


def test_post_chat_log_skips_an_unmapped_channel(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path)

    async def scenario():
        return await streamer.post_chat_log("twitch:ch", "/data/chat.chat.json", "Chat log: Title")

    assert asyncio.run(scenario()) is False
    assert client.sent_files == []


def test_connect_rejects_a_session_that_is_not_logged_in(tmp_path):
    streamer, client, _cfg = make_streamer(tmp_path)
    client.authorized = False

    async def scenario():
        return await streamer.connect()

    with pytest.raises(RuntimeError, match="stream-archive-setup-telegram-live"):
        asyncio.run(scenario())
