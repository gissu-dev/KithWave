# KithWave Discord Music Bot

KithWave is a Discord music bot I built for my own server. It uses prefix commands, queue controls, button-based playback, and optional Spotify link support.

## Features
- Play music from YouTube search or URL
- Play live radio with preset dropdown menu (`radio`)
- Quick-play radio by preset name, station name lookup, or direct stream URL
- Import tracks from Spotify track/album/playlist links
- Import YouTube playlists
- Shuffle on import with `--shuffle`
- Per-server queue state
- `nowplaying` panel with control buttons (`Pause`, `Skip`, `Stop`, `Queue`, `Vol-`, `Vol+`, `Shuffle`, `Lyrics`)
- Lyrics lookup with fallback sources

## Stack
- Python + `discord.py`
- `yt-dlp` for extraction
- `ffmpeg` for audio
- `spotipy` for Spotify metadata

## Quick Start
1. Create a bot in the Discord Developer Portal.
2. Enable `Message Content Intent`.
3. Invite it with permissions:
   - `Send Messages`
   - `Embed Links`
   - `Read Message History`
   - `Connect`
   - `Speak`

Install dependencies:

```powershell
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

Install FFmpeg and verify:

```powershell
ffmpeg -version
```

Configure env file:

```powershell
copy .env.example .env
```

Run the bot:

```powershell
python bot.py
```

## Commands
`<prefix>` is set by `BOT_PREFIX` (default: `!`).

- `<prefix>play <query_or_url>`
- `<prefix>play --shuffle <playlist_url>`
- `<prefix>radio` (open preset station dropdown)
- `<prefix>radio <preset_name_or_station_or_stream_url>`
- `<prefix>queue`
- `<prefix>shuffle`
- `<prefix>nowplaying`
- `<prefix>lyrics`
- `<prefix>pause`
- `<prefix>volume` / `<prefix>volume 80`
- `<prefix>skip`
- `<prefix>stop`
- `<prefix>radioreload` (server manager only; reload `radio_presets.json`)
- `<prefix>spotifycheck`
- `<prefix>spotifylogin`
- `<prefix>spotifycode <full_callback_url>`

## Environment Variables
- `DISCORD_TOKEN` (required)
- `BOT_PREFIX` (optional, default `!`)
- `SPOTIFY_CLIENT_ID` (optional, needed for Spotify link support)
- `SPOTIFY_CLIENT_SECRET` (optional, needed for Spotify link support)
- `SPOTIFY_REDIRECT_URI` (optional, used for user-auth fallback)
- `SPOTIFY_MARKET` (optional, example: `US`)
- `PLAYLIST_ITEM_CAP` (optional, default `50`, range `1-500`)
- `RADIO_PRESETS_FILE` (optional, default `radio_presets.json`)

## Radio Presets
- Presets are loaded from [radio_presets.json](./radio_presets.json).
- Edit that file to add/remove stations, then run `<prefix>radioreload` or restart the bot.
- Each preset needs `id`, `name`, and at least one of `stream_url`, `stream_urls`, or `query`.
- Radio selection switches immediately to the new station and replaces the previous queue.

Example preset:

```json
{
  "id": "my_station",
  "name": "My Station",
  "stream_url": "https://example.com/live.mp3",
  "stream_urls": ["https://example.com/alt1.aac", "https://example.com/alt2.mp3"],
  "homepage": "https://example.com",
  "description": "What this station is",
  "aliases": ["my station", "station alias"]
}
```

## Optional Windows Launcher
Install Start Menu shortcuts once:

```powershell
powershell -ExecutionPolicy Bypass -File .\install_kithwave_shortcuts.ps1
```

Then search for `KithWave` in Windows Start and launch it.

## Notes
- Spotify audio is not streamed directly. Spotify metadata is resolved into playable sources.
- Radio station name lookup uses Radio Browser and queues the top matching station stream.
- Presets are validated at startup; invalid entries are skipped with log warnings.
- If audio fails, check that `ffmpeg` is installed and on `PATH`.
- Large playlist imports are capped by `PLAYLIST_ITEM_CAP` for responsiveness.

## Roadmap
- [ ] Add slash-command versions of the core controls
- [ ] Add small tests around query parsing and URL handling
- [ ] Improve reconnect behavior after voice disconnects
