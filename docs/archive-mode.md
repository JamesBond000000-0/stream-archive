# Archive mode

Archive mode saves a Twitch channel's past broadcasts (VODs) to YouTube. It is a separate mode from the live monitor: a run works through one channel's backlog once, and the monitor keeps watching live channels as usual.

## How it works

1. The admin sends `/archive <twitch username>` to the bot.
2. The app lists the channel's VODs on Twitch, oldest first.
3. The app reads the ledger channel on Telegram and skips every VOD already in it.
4. For each remaining VOD, the app rebuilds the VOD's playlist from the Twitch CDN, restreams it to YouTube in real time, and posts one entry to the ledger channel.

A run takes real time: a 5-hour VOD takes 5 hours to archive. Runs are serial, one VOD at a time, and one run at a time.

## The playlist and the audio

The rebuild uses the recovery layout on the Twitch CDN. The VOD thumbnail URL names the CDN host and the storage path, and the DVR playlist under that path lists the original segments.

Twitch mutes some VOD ranges for the public player. In the DVR playlist, a muted range's segment is named `<n>-unmuted.ts` or `<n>-unmuted.mp4` and holds the original audio. Whether that original still loads depends on the storage format: VODs stored as fragmented MP4 keep it, VODs stored as MPEG-TS do not. The app probes each unmuted segment before it starts:

- The segment still loads: the archive gets the original audio.
- The segment is gone: the muted twin (the same name with `-muted`) takes its place, so the archive still holds the full timeline.

The start and finish messages say how many segments fell back to the muted audio.

## Splitting long VODs

YouTube saves no archive of a broadcast longer than 12 hours. Archive mode splits a VOD into a chain of broadcasts of at most `archive.split_hours` hours each (default 11). The split is the same hot handover the live YouTube output uses: the next broadcast is live before the old one ends, so no part of the VOD is lost. The ledger entry lists every part's link.

## The ledger channel

The ledger is a private Telegram channel. The app creates it on first use with the title in `archive.ledger_channel_title` and stores its peer id in `archive.ledger_peer_id`. Each finished VOD gets one message:

```
vod: 2941241699
channel: twitchreo
title: こんばんは
date: 2026-11-26T11:29:21Z
youtube: https://www.youtube.com/watch?v=...
```

The app writes an entry only after the YouTube archive finishes clean. A failed or cut-short VOD stays out of the ledger, so the next run retries it. The ledger lives on Telegram, so it survives a lost or rebuilt server.

## Setup

Archive mode needs three things:

- YouTube set up, like a YouTube live restream.
- `telegram_live` on and logged in. The ledger uses the same user session.
- `archive.enabled` set to true.

```json
"archive": {
    "enabled": true,
    "ledger_channel_title": "Stream Archive Ledger",
    "ledger_peer_id": null,
    "split_hours": 11,
    "max_vods_per_run": 0
}
```

`max_vods_per_run` caps how many VODs one run archives. 0 means no cap. A channel with a deep backlog can take days of real time to archive in full, so a cap makes the first run a trial.

## Limits

- Only VODs still listed on Twitch take part. A deleted VOD is not in the list, so this mode cannot recover it.
- A VOD whose thumbnail is still a placeholder (Twitch is processing it) waits for a later run.
- The stream is a real-time restream. There is no faster path: YouTube receives the VOD at the speed it plays.
