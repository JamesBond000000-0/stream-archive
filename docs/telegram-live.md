# Telegram live restream

The app can send a live stream to a Telegram channel. The modes are:

- `telegram`: Telegram only. Telegram keeps no archive, so nothing stays after the stream ends.
- `telegram_both`: Telegram plus a disk file. The disk file is the archive.

Set the mode per channel with `channel_output_modes`, or for all channels with `output_mode`.

## Why a user account

Bots cannot start a Telegram live stream. The MTProto calls for a channel, a live stream, and its RTMP address are user-only. So this feature logs in a Telegram **user account**, not the bot.

The account must own or manage each target channel. One Telegram channel can hold one live stream at a time. Two sources that are live at once need two Telegram channels.

## Setup

1. Open https://my.telegram.org/apps and create an app. Note the api id and the api hash.
2. Set them in `config.json`. Use env placeholders for the secrets:

   ```json
   "telegram_live": {
       "enabled": true,
       "api_id": "${TELEGRAM_API_ID}",
       "api_hash": "${TELEGRAM_API_HASH}",
       "session": "telegram_live.session",
       "auto_create_channels": true,
       "record": false,
       "title_timestamp": true,
       "post_chat_log": true,
       "forward_recording": true
   }
   ```

3. Log the account in, in an interactive terminal:

   ```sh
   stream-archive-setup-telegram-live
   ```

   Telethon asks for the phone number, the login code, and the two-step password when the account has one. The session file holds the login after that. Keep the file private: it is a full login for that account.
4. Restart the app and set a channel to `telegram` or `telegram_both`.

## Target channels

`channel_telegram_peers` maps a monitored channel to a Telegram channel peer id (marked, `-100...`):

```json
"channel_telegram_peers": {"twitch:example": -1001234567890}
```

With `telegram_live.auto_create_channels` on (the default), a channel with no entry gets a new **private** Telegram channel at its first stream. The app stores the peer id in `config.json` at once, so a restart reuses the same channel. The channel title is `<name> restream`, and its description links the source.

## Channel photo, stream title, chat log

At each stream start the app sets the Telegram channel photo to the broadcaster's profile image (Twitch `profile_image_url`, Kick `profile_picture`). The applied URL is stored in `channel_telegram_icon_urls`, so the photo is set once and again only when the broadcaster changes their icon. This also covers channels that exist already: the photo lands at their next stream. Icons are best effort: a failed lookup, download, or set never blocks the stream.

The live stream title is `author - title`, plus the start time in the app timezone when `telegram_live.title_timestamp` is on (the default): `author - title (2026-11-24 12:31 JST)`.

When a Telegram restream ends and `telegram_live.post_chat_log` is on (the default), the finished chat log file is posted to the channel. This needs `record_chat`: without a chat file there is nothing to post.

With `telegram_live.record` on, the server-side recording lands in the account's Saved Messages after the stream ends. Saved Messages has no shareable link, so with `telegram_live.forward_recording` on (the default) the app forwards the file to the channel once it arrives (polling up to 10 minutes). The forwarded post is the archive copy, and its channel link goes to the operator notification.

A recording message carries no channel name or title, so the app matches it strictly: video file only (each recording also drops an audio-only twin), duration within max(60 s, 10%) of the stream segment, landed after the stream ended, and never forwarded before (Telegram can deliver one recording several times; the app stores each forwarded file's size-and-duration fingerprint in `telegram_forwarded_recording_keys`). A file that matches two waiting channels at once goes to neither: a missing archive is better than another channel's recording in the wrong channel.

Public channels (with a `@username`) have a small per-account cap, so auto-create makes private channels. Join them with an invite link from the Telegram app.

To use a channel that exists already, add its peer id by hand and turn auto-create off if you want a missing entry to fail instead of creating a channel.

## Limits

- One live stream per Telegram channel. A second start on the same channel would end the first, so each source needs its own channel.
- Telegram gives no daily create quota and no split rule like YouTube. `max_concurrent_telegram_streams` still caps parallel restreams, for the uplink and the host.
- Telegram keeps no archive and no replay link. Use `telegram_both` when you need a copy.
- `telegram_live.record` starts the Telegram server-side recording. The file lands in the account's Saved Messages when the stream ends. A long stream can pass the Telegram file size cap (2 GB, or 4 GB with Premium), so the default is off.
- Fast channel creates can hit a Telegram rate limit. A failed create stops that start and logs the error. The next monitor cycle tries again.

## Commands

- `/mode [channel] telegram` or `telegram_both` sets the output mode.
- `/maxtelegram <n>` sets the parallel restream cap (`0` = unlimited).
- The control API and the web panel expose the same settings.
