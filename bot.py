import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote
from urllib.request import Request, urlopen

import discord
from discord.ext import commands
from dotenv import load_dotenv
import yt_dlp

try:
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
except ImportError:
    spotipy = None
    SpotifyClientCredentials = None
    SpotifyOAuth = None


load_dotenv()

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("kithwave")

SPOTIFY_URL_RE = re.compile(r"https?://open\.spotify\.com/(track|album|playlist)/([A-Za-z0-9]+)")
SPOTIFY_TRACK_ID_RE = re.compile(r"(?:spotify:track:|/tracks?/)([A-Za-z0-9]{22})")
YOUTUBE_PLAYLIST_URL_RE = re.compile(r"https?://(?:www\.|music\.)?(?:youtube\.com|youtu\.be)/\S*[?&]list=([A-Za-z0-9_-]+)")
try:
    PLAYLIST_ITEM_CAP = int(os.getenv("PLAYLIST_ITEM_CAP", "50"))
except ValueError:
    PLAYLIST_ITEM_CAP = 50
PLAYLIST_ITEM_CAP = max(1, min(500, PLAYLIST_ITEM_CAP))
LYRICS_CHAR_LIMIT = 3500
VOICE_STATUS_PREFIX = "Now playing: "
VOICE_STATUS_MAX_LEN = 500

YTDL_OPTIONS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "default_search": "ytsearch",
    "source_address": "0.0.0.0",
    "extract_flat": False,
}

FFMPEG_OPTIONS = {
    "before_options": "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5",
    "options": "-vn",
}

ytdl = yt_dlp.YoutubeDL(YTDL_OPTIONS)
ytdl_playlist = yt_dlp.YoutubeDL(
    {
        "quiet": True,
        "noplaylist": False,
        "extract_flat": True,
        "ignoreerrors": True,
        "source_address": "0.0.0.0",
    }
)


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "Live"
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{sec:02d}"
    return f"{minutes}:{sec:02d}"


@dataclass
class Track:
    title: str
    stream_url: str
    webpage_url: str
    duration: Optional[int]
    thumbnail: Optional[str]
    requested_by: str
    source_query: Optional[str] = None


class GuildMusicState:
    def __init__(self) -> None:
        self.queue: list[Track] = []
        self.current: Optional[Track] = None
        self.channel_id: Optional[int] = None
        self.control_message_id: Optional[int] = None
        self.volume: float = 0.01
        self.lock = asyncio.Lock()


class MusicControlView(discord.ui.View):
    def __init__(self, cog: "MusicCog", guild_id: int) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or interaction.guild.id != self.guild_id:
            return False
        return True

    @discord.ui.button(label="Pause/Resume", style=discord.ButtonStyle.secondary, custom_id="kithwave_pause_resume")
    async def pause_resume(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        ok, message = await self.cog.pause_resume_guild(interaction.guild)
        await interaction.response.send_message(message, ephemeral=True)
        if ok:
            await self.cog.refresh_now_playing_embed(interaction.guild)

    @discord.ui.button(label="Skip", style=discord.ButtonStyle.primary, custom_id="kithwave_skip")
    async def skip(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        ok, message = await self.cog.skip_guild(interaction.guild)
        await interaction.response.send_message(message, ephemeral=True)
        if ok:
            await self.cog.refresh_now_playing_embed(interaction.guild)

    @discord.ui.button(label="Stop", style=discord.ButtonStyle.danger, custom_id="kithwave_stop")
    async def stop(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        ok, message = await self.cog.stop_guild(interaction.guild)
        await interaction.response.send_message(message, ephemeral=True)

    @discord.ui.button(label="Queue", style=discord.ButtonStyle.success, custom_id="kithwave_queue")
    async def queue(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Server not found.", ephemeral=True)
            return
        embed = self.cog.build_queue_embed(interaction.guild)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Vol-", style=discord.ButtonStyle.secondary, custom_id="kithwave_vol_down", row=1)
    async def volume_down(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        ok, message = await self.cog.adjust_volume(interaction.guild, -20)
        await interaction.response.send_message(message, ephemeral=True)
        if ok:
            await self.cog.refresh_now_playing_embed(interaction.guild)

    @discord.ui.button(label="Vol+", style=discord.ButtonStyle.secondary, custom_id="kithwave_vol_up", row=1)
    async def volume_up(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        ok, message = await self.cog.adjust_volume(interaction.guild, 20)
        await interaction.response.send_message(message, ephemeral=True)
        if ok:
            await self.cog.refresh_now_playing_embed(interaction.guild)

    @discord.ui.button(label="Shuffle", style=discord.ButtonStyle.primary, custom_id="kithwave_shuffle", row=1)
    async def shuffle(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        ok, message = await self.cog.shuffle_queue(interaction.guild)
        await interaction.response.send_message(message, ephemeral=True)
        if ok:
            await self.cog.refresh_now_playing_embed(interaction.guild)

    @discord.ui.button(label="Lyrics", style=discord.ButtonStyle.secondary, custom_id="kithwave_lyrics", row=1)
    async def lyrics(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not interaction.guild:
            await interaction.response.send_message("Server not found.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        embed, message = await self.cog.current_lyrics_embed(interaction.guild)
        if embed:
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        await interaction.followup.send(message, ephemeral=True)


class MusicCog(commands.Cog):
    def __init__(self, bot: commands.Bot, prefix: str) -> None:
        self.bot = bot
        self.prefix = prefix
        self.states: dict[int, GuildMusicState] = {}
        self.embed_color = discord.Color.from_rgb(103, 28, 43)
        self.spotify_market: Optional[str] = None
        self.spotify = self._build_spotify_client()
        self.spotify_user = self._build_spotify_user_client()

    def refresh_spotify_clients_from_env(self) -> None:
        # Allow .env edits to apply without requiring a full bot restart.
        load_dotenv(override=True)
        market_raw = os.getenv("SPOTIFY_MARKET", "").strip().upper()
        self.spotify_market = market_raw if market_raw else None
        self.spotify = self._build_spotify_client()
        self.spotify_user = self._build_spotify_user_client()

    async def can_manage_spotify_auth(self, ctx: commands.Context) -> bool:
        if await self.bot.is_owner(ctx.author):
            return True
        if not isinstance(ctx.author, discord.Member):
            return False
        return bool(ctx.author.guild_permissions.manage_guild)

    def _build_spotify_client(self):
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

        if not client_id or not client_secret:
            return None
        if spotipy is None or SpotifyClientCredentials is None:
            log.warning("Spotify credentials provided, but spotipy is not installed.")
            return None

        try:
            return spotipy.Spotify(
                auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
            )
        except Exception as e:
            log.warning("Failed to create Spotify client: %s", e)
            return None

    def _build_spotify_user_client(self):
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")

        if not client_id or not client_secret or not redirect_uri:
            return None
        if spotipy is None or SpotifyOAuth is None:
            return None

        try:
            auth_manager = SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope="playlist-read-private playlist-read-collaborative",
                open_browser=True,
                show_dialog=True,
                cache_path=".spotify_user_cache",
            )
            return spotipy.Spotify(auth_manager=auth_manager)
        except Exception as e:
            log.warning("Failed to create Spotify user client: %s", e)
            return None

    def get_state(self, guild_id: int) -> GuildMusicState:
        if guild_id not in self.states:
            self.states[guild_id] = GuildMusicState()
        return self.states[guild_id]

    def _status_artist_title(self, track: Track) -> tuple[str, str]:
        for artist, title in self._lyrics_artist_title_candidates(track):
            artist_name = artist.strip() if isinstance(artist, str) else ""
            cleaned_title = self._clean_lyrics_query(title)
            if artist_name and cleaned_title:
                return artist_name, cleaned_title

        cleaned_title = self._clean_lyrics_query(track.title) or track.title.strip()
        if not cleaned_title:
            cleaned_title = "Unknown Track"
        return "Unknown Artist", cleaned_title

    def _build_voice_channel_status(self, track: Track) -> str:
        artist, title = self._status_artist_title(track)
        status_body = f"{title} - {artist}"
        max_body_len = max(4, VOICE_STATUS_MAX_LEN - len(VOICE_STATUS_PREFIX))
        if len(status_body) > max_body_len:
            status_body = status_body[: max_body_len - 3].rstrip() + "..."
        return f"{VOICE_STATUS_PREFIX}{status_body}"

    async def sync_voice_channel_status(self, guild: discord.Guild) -> None:
        state = self.get_state(guild.id)
        track = state.current
        voice_client = guild.voice_client
        if not track or not voice_client:
            return

        channel = voice_client.channel
        if not isinstance(channel, discord.VoiceChannel):
            return

        me = guild.me
        if not me:
            return
        perms = channel.permissions_for(me)
        can_set_status = perms.manage_channels or getattr(perms, "set_voice_channel_status", False)
        if not can_set_status:
            return

        current_status = ""
        try:
            payload = await self.bot.http.get_channel(channel.id)
            raw_status = payload.get("status")
            if isinstance(raw_status, str):
                current_status = raw_status.strip()
        except discord.HTTPException as e:
            log.debug("Failed to fetch voice channel status for guild %s: %s", guild.id, e)
            return

        if current_status and not current_status.lower().startswith(VOICE_STATUS_PREFIX.lower()):
            return

        desired_status = self._build_voice_channel_status(track)
        if current_status == desired_status:
            return

        try:
            await channel.edit(status=desired_status, reason="KithWave now playing status")
        except discord.Forbidden:
            pass
        except discord.HTTPException as e:
            log.warning("Failed to update voice channel status for guild %s: %s", guild.id, e)

    async def extract_track(self, query: str, requester: str) -> Track:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, lambda: ytdl.extract_info(query, download=False))

        if "entries" in info and info["entries"]:
            info = info["entries"][0]

        return Track(
            title=info.get("title", "Unknown Track"),
            stream_url=info["url"],
            webpage_url=info.get("webpage_url", query),
            duration=info.get("duration"),
            thumbnail=info.get("thumbnail"),
            requested_by=requester,
            source_query=query,
        )

    async def ensure_voice(self, guild: discord.Guild, member: discord.Member) -> discord.VoiceClient:
        if not member.voice or not member.voice.channel:
            raise RuntimeError("Join a voice channel first so I know where to play music.")

        voice_client = guild.voice_client
        target_channel = member.voice.channel

        if voice_client and voice_client.channel != target_channel:
            await voice_client.move_to(target_channel)
            return voice_client

        if voice_client:
            return voice_client

        state = self.get_state(guild.id)
        state.volume = 0.01
        return await target_channel.connect()

    def _spotify_kind_and_id(self, query: str) -> Optional[tuple[str, str]]:
        match = SPOTIFY_URL_RE.search(query)
        if not match:
            return None
        return match.group(1), match.group(2)

    def _track_search_text(self, track_obj: dict) -> Optional[str]:
        name = track_obj.get("name")
        if not name and isinstance(track_obj.get("track"), dict):
            return self._track_search_text(track_obj.get("track"))
        if not name:
            return None

        artists = track_obj.get("artists") or []
        artist_name: Optional[str] = None
        if artists and isinstance(artists, list):
            first_artist = artists[0]
            if isinstance(first_artist, dict):
                artist_name = first_artist.get("name")

        if not artist_name:
            album = track_obj.get("album") or {}
            album_artists = album.get("artists") if isinstance(album, dict) else None
            if isinstance(album_artists, list) and album_artists:
                first_album_artist = album_artists[0]
                if isinstance(first_album_artist, dict):
                    artist_name = first_album_artist.get("name")

        if artist_name:
            return f"{artist_name} - {name} audio"
        return f"{name} audio"

    def _http_get_json_sync(self, url: str) -> object:
        request = Request(url, headers={"User-Agent": "KithWave/1.0"})
        with urlopen(request, timeout=12) as response:
            payload = response.read().decode("utf-8", errors="replace")
        return json.loads(payload)

    def _clean_lyrics_query(self, text: str) -> str:
        cleaned = text.strip()
        cleaned = re.sub(r"\[[^\]]*\]", " ", cleaned)
        cleaned = re.sub(r"\([^)]*\)", " ", cleaned)
        cleaned = re.sub(
            r"\b(official|lyrics?|lyric|audio|video|visualizer|mv|hq|hd|4k|remastered)\b",
            " ",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
        return cleaned

    def _lyrics_query_candidates(self, track: Track) -> list[str]:
        candidates: list[str] = []
        seen: set[str] = set()

        def push(value: str) -> None:
            value = value.strip()
            if not value:
                return
            lowered = value.lower()
            if lowered in seen:
                return
            seen.add(lowered)
            candidates.append(value)

        sources = [track.source_query or "", track.title]
        for source in sources:
            cleaned = self._clean_lyrics_query(source)
            if not cleaned:
                continue
            push(cleaned)
            if " - " in cleaned:
                left, right = cleaned.split(" - ", 1)
                push(f"{left} {right}")
                push(right)
            else:
                push(cleaned.replace("-", " "))

        return candidates

    def _lyrics_artist_title_candidates(self, track: Track) -> list[tuple[Optional[str], str]]:
        pairs: list[tuple[Optional[str], str]] = []
        seen: set[tuple[str, str]] = set()

        def push(artist: Optional[str], title: str) -> None:
            title = title.strip()
            artist = artist.strip() if isinstance(artist, str) else None
            if not title:
                return
            key = ((artist or "").lower(), title.lower())
            if key in seen:
                return
            seen.add(key)
            pairs.append((artist, title))

        for query in self._lyrics_query_candidates(track):
            for separator in (" - ",):
                if separator in query:
                    artist_part, title_part = query.split(separator, 1)
                    push(artist_part, title_part)

            by_match = re.match(r"(.+?)\s+by\s+(.+)$", query, flags=re.IGNORECASE)
            if by_match:
                push(by_match.group(2), by_match.group(1))

            push(None, query)

        cleaned_title = self._clean_lyrics_query(track.title)
        if cleaned_title:
            push(None, cleaned_title)

        return pairs

    def _lyrics_from_lrclib_sync(self, artist: Optional[str], title: str) -> Optional[tuple[str, str, str]]:
        title = title.strip()
        artist = artist.strip() if isinstance(artist, str) else None
        if not title:
            return None

        def build_url(endpoint: str, artist_name: Optional[str]) -> str:
            params = [f"track_name={quote(title, safe='')}"]
            if artist_name:
                params.append(f"artist_name={quote(artist_name, safe='')}")
            return f"https://lrclib.net/api/{endpoint}?{'&'.join(params)}"

        urls: list[str] = []
        if artist:
            urls.append(build_url("get", artist))
            urls.append(build_url("search", artist))
        urls.append(build_url("search", None))

        for url in urls:
            try:
                payload = self._http_get_json_sync(url)
            except Exception:
                continue

            if isinstance(payload, dict):
                items = [payload]
            elif isinstance(payload, list):
                items = [item for item in payload if isinstance(item, dict)]
            else:
                continue

            for item in items[:10]:
                plain = item.get("plainLyrics")
                synced = item.get("syncedLyrics")
                lyrics_text = plain if isinstance(plain, str) and plain.strip() else None
                if not lyrics_text and isinstance(synced, str) and synced.strip():
                    lyrics_text = re.sub(r"^\[[0-9:.]+\]\s*", "", synced, flags=re.MULTILINE).strip()
                if not lyrics_text:
                    continue

                artist_name = item.get("artistName")
                track_name = item.get("trackName")
                final_artist = artist_name if isinstance(artist_name, str) and artist_name.strip() else (artist or "Unknown")
                final_title = track_name if isinstance(track_name, str) and track_name.strip() else title
                return final_artist, final_title, lyrics_text.strip()

        return None

    def _lyrics_from_artist_title_sync(self, artist: str, title: str) -> Optional[str]:
        lyrics_url = f"https://api.lyrics.ovh/v1/{quote(artist, safe='')}/{quote(title, safe='')}"
        try:
            payload = self._http_get_json_sync(lyrics_url)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        lyrics = payload.get("lyrics")
        if not isinstance(lyrics, str):
            return None
        lyrics = lyrics.strip()
        if not lyrics:
            return None
        return lyrics

    def _lyrics_lookup_sync(self, track: Track) -> Optional[tuple[str, str, str, str]]:
        for artist, title in self._lyrics_artist_title_candidates(track):
            result = self._lyrics_from_lrclib_sync(artist, title)
            if result:
                artist_name, track_title, lyrics = result
                return artist_name, track_title, lyrics, "lrclib"

        for query in self._lyrics_query_candidates(track):
            suggest_url = f"https://api.lyrics.ovh/suggest/{quote(query, safe='')}"
            try:
                payload = self._http_get_json_sync(suggest_url)
            except Exception:
                continue

            if not isinstance(payload, dict):
                continue
            items = payload.get("data")
            if not isinstance(items, list):
                continue

            for item in items[:8]:
                if not isinstance(item, dict):
                    continue
                title = item.get("title")
                artist_obj = item.get("artist")
                artist_name = artist_obj.get("name") if isinstance(artist_obj, dict) else None
                if not isinstance(title, str) or not isinstance(artist_name, str):
                    continue
                lyrics = self._lyrics_from_artist_title_sync(artist_name, title)
                if lyrics:
                    return artist_name, title, lyrics, "lyrics.ovh"
        return None

    async def current_lyrics_embed(self, guild: discord.Guild) -> tuple[Optional[discord.Embed], str]:
        state = self.get_state(guild.id)
        if not state.current:
            return None, "No track is playing right now."

        track = state.current
        loop = asyncio.get_running_loop()
        try:
            result = await asyncio.wait_for(loop.run_in_executor(None, lambda: self._lyrics_lookup_sync(track)), timeout=20)
        except asyncio.TimeoutError:
            return None, "Lyrics lookup timed out. Try again in a moment."
        except Exception as e:
            log.warning("Lyrics lookup failed for '%s': %s", track.title, e)
            return None, "Lyrics lookup failed right now. Try again."

        if not result:
            return None, "Could not find lyrics for the current track."

        artist, title, lyrics, lyrics_source = result
        formatted = lyrics.replace("\r\n", "\n").strip()
        formatted = re.sub(r"\n{3,}", "\n\n", formatted)
        truncated = False
        if len(formatted) > LYRICS_CHAR_LIMIT:
            formatted = formatted[:LYRICS_CHAR_LIMIT].rsplit("\n", 1)[0].rstrip()
            formatted += "\n..."
            truncated = True

        embed = discord.Embed(
            title=f"Lyrics: {title}",
            description=formatted or "No lyrics text available.",
            color=self.embed_color,
        )
        embed.add_field(name="Artist", value=f"`{artist}`", inline=True)
        embed.add_field(name="Track", value=f"[{track.title}]({track.webpage_url})", inline=False)
        footer = f"KithWave | Source: {lyrics_source}"
        if truncated:
            footer += " | truncated"
        embed.set_footer(text=footer)
        return embed, ""

    def _spotify_track_id_from_value(self, value: object, depth: int = 0) -> Optional[str]:
        if depth > 4:
            return None

        if isinstance(value, str):
            match = SPOTIFY_TRACK_ID_RE.search(value)
            if match:
                return match.group(1)
            return None

        if isinstance(value, list):
            for entry in value:
                track_id = self._spotify_track_id_from_value(entry, depth + 1)
                if track_id:
                    return track_id
            return None

        if not isinstance(value, dict):
            return None

        track_id = value.get("id")
        if isinstance(track_id, str) and track_id:
            return track_id

        for key in ("uri", "href"):
            val = value.get(key)
            if isinstance(val, str):
                match = SPOTIFY_TRACK_ID_RE.search(val)
                if match:
                    return match.group(1)

        external_urls = value.get("external_urls")
        if isinstance(external_urls, dict):
            ext_url = external_urls.get("spotify")
            if isinstance(ext_url, str):
                match = SPOTIFY_TRACK_ID_RE.search(ext_url)
                if match:
                    return match.group(1)

        for key in ("track", "linked_from", "item", "context"):
            nested = value.get(key)
            nested_id = self._spotify_track_id_from_value(nested, depth + 1)
            if nested_id:
                return nested_id

        return None

    def _spotify_terms_from_track_ids_sync(self, sp, track_ids: list[str], market_opt: Optional[str]) -> list[str]:
        unique_ids: list[str] = []
        for track_id in track_ids:
            if track_id and track_id not in unique_ids:
                unique_ids.append(track_id)

        hydrated_terms: list[str] = []
        for idx in range(0, len(unique_ids), 50):
            chunk = unique_ids[idx : idx + 50]
            try:
                if market_opt:
                    payload = sp.tracks(chunk, market=market_opt)
                else:
                    payload = sp.tracks(chunk)
            except Exception as e:
                log.warning("Spotify tracks hydration failed for market=%s: %s", market_opt, e)
                continue

            for track in payload.get("tracks", []) or []:
                if not isinstance(track, dict):
                    continue
                if track.get("is_local") or track.get("type") == "episode":
                    continue
                term = self._track_search_text(track)
                if term:
                    hydrated_terms.append(term)

        return hydrated_terms

    def _spotify_terms_sync(self, kind: str, item_id: str, spotify_client=None) -> list[str]:
        sp = spotify_client or self.spotify
        assert sp is not None
        terms: list[str] = []
        # Prefer explicit SPOTIFY_MARKET when provided; fall back to from_token
        # for user-auth only when no explicit market is configured.
        market = self.spotify_market or ("from_token" if spotify_client is self.spotify_user else None)

        def unique_markets(values: list[Optional[str]]) -> list[Optional[str]]:
            out: list[Optional[str]] = []
            for v in values:
                if v not in out:
                    out.append(v)
            return out

        if kind == "track":
            market_candidates = unique_markets([market, None, "from_token" if spotify_client is self.spotify_user else None])
            last_exc: Optional[Exception] = None
            for market_opt in market_candidates:
                try:
                    if market_opt:
                        data = sp.track(item_id, market=market_opt)
                    else:
                        data = sp.track(item_id)
                    term = self._track_search_text(data)
                    return [term] if term else []
                except Exception as e:
                    last_exc = e
                    continue
            if last_exc:
                raise last_exc
            return []

        if kind == "album":
            market_candidates = unique_markets([market, None, "from_token" if spotify_client is self.spotify_user else None])
            last_exc: Optional[Exception] = None
            for market_opt in market_candidates:
                terms.clear()
                try:
                    offset = 0
                    while True:
                        if market_opt:
                            page = sp.album_tracks(item_id, limit=50, offset=offset, market=market_opt)
                        else:
                            page = sp.album_tracks(item_id, limit=50, offset=offset)
                        for item in page.get("items", []):
                            term = self._track_search_text(item)
                            if term:
                                terms.append(term)
                        if not page.get("next"):
                            break
                        offset += len(page.get("items", []))
                    if terms:
                        return terms
                except Exception as e:
                    last_exc = e
                    continue
            if last_exc and not terms:
                raise last_exc
            return terms

        if kind == "playlist":
            unresolved_track_ids: list[str] = []

            def append_terms_from_page(page_obj: dict) -> int:
                items = page_obj.get("items", [])
                for item in items:
                    track: Optional[dict] = None
                    track_ref: object = item
                    if isinstance(item, dict):
                        raw_track = item.get("track")
                        if raw_track is None:
                            raw_track = item.get("item")
                        if isinstance(raw_track, dict):
                            track = raw_track
                            track_ref = raw_track
                        elif raw_track is not None:
                            track_ref = raw_track
                        elif isinstance(item.get("name"), str):
                            # Some endpoints can return track-like objects directly.
                            track = item
                    if track and (track.get("is_local") or track.get("type") == "episode"):
                        continue

                    if track:
                        term = self._track_search_text(track)
                        if term:
                            terms.append(term)
                            continue

                    unresolved_id = self._spotify_track_id_from_value(track_ref)
                    if not unresolved_id and track:
                        unresolved_id = self._spotify_track_id_from_value(track)
                    if unresolved_id:
                        unresolved_track_ids.append(unresolved_id)
                return len(items)

            market_candidates = unique_markets([market, None, "from_token" if spotify_client is self.spotify_user else None])
            last_exc: Optional[Exception] = None

            for market_opt in market_candidates:
                terms.clear()
                unresolved_track_ids.clear()
                try:
                    try:
                        offset = 0
                        while True:
                            # Request track items only. Including episodes can require user-auth
                            # even when the playlist itself is public.
                            kwargs = {
                                "limit": 100,
                                "offset": offset,
                                "additional_types": ("track",),
                            }
                            if market_opt:
                                kwargs["market"] = market_opt
                            page = sp.playlist_items(item_id, **kwargs)
                            got = append_terms_from_page(page)
                            if not page.get("next") or got == 0:
                                break
                            offset += got
                    except Exception as e:
                        # Fallback for playlists where playlist_items fails under app auth.
                        last_exc = e
                        log.warning(
                            "playlist_items failed for %s market=%s, falling back to playlist_tracks: %s",
                            item_id,
                            market_opt,
                            e,
                        )
                        terms.clear()
                        offset = 0
                        while True:
                            kwargs = {"limit": 100, "offset": offset}
                            if market_opt:
                                kwargs["market"] = market_opt
                            page = sp.playlist_tracks(item_id, **kwargs)
                            got = append_terms_from_page(page)
                            if not page.get("next") or got == 0:
                                break
                            offset += got

                    if not terms:
                        # Final fallback: read first page from playlist object itself.
                        if market_opt:
                            playlist_obj = sp.playlist(item_id, market=market_opt)
                        else:
                            playlist_obj = sp.playlist(item_id)
                        tracks_obj = playlist_obj.get("tracks", {}) if isinstance(playlist_obj, dict) else {}
                        append_terms_from_page(tracks_obj)

                    if not terms and unresolved_track_ids:
                        hydrated_terms = self._spotify_terms_from_track_ids_sync(sp, unresolved_track_ids, market_opt)
                        if hydrated_terms:
                            terms.extend(hydrated_terms)

                    if terms:
                        return terms
                except Exception as e:
                    last_exc = e
                    continue

            if last_exc and not terms:
                raise last_exc
            return terms

        return terms

    def spotify_playlist_probe_sync(self, playlist_ref: str) -> str:
        playlist_ref = playlist_ref.strip()
        target = self._spotify_kind_and_id(playlist_ref)
        if target:
            kind, item_id = target
            if kind != "playlist":
                return "Probe expects a Spotify playlist link or playlist ID."
        else:
            item_id = playlist_ref

        if not item_id or not re.fullmatch(r"[A-Za-z0-9]+", item_id):
            return "Invalid playlist reference. Paste the full Spotify playlist URL or playlist ID."

        lines: list[str] = [
            f"playlist_id={item_id}",
            f"market_env={self.spotify_market or 'none'}",
            f"user_token_ready={'yes' if self.spotify_user_token_ready() else 'no'}",
        ]

        clients: list[tuple[str, object]] = []
        if self.spotify_user:
            clients.append(("user", self.spotify_user))
        if self.spotify:
            clients.append(("app", self.spotify))

        if not clients:
            lines.append("no Spotify clients configured")
            return "\n".join(lines)

        def unique_markets(values: list[Optional[str]]) -> list[Optional[str]]:
            out: list[Optional[str]] = []
            for val in values:
                if val not in out:
                    out.append(val)
            return out

        for label, sp in clients:
            market_candidates = unique_markets(
                [
                    self.spotify_market,
                    None,
                    "from_token" if label == "user" else None,
                ]
            )

            for market_opt in market_candidates:
                market_label = market_opt or "none"
                kwargs = {"limit": 20, "offset": 0, "additional_types": ("track",)}
                if market_opt:
                    kwargs["market"] = market_opt

                try:
                    page = sp.playlist_items(item_id, **kwargs)
                    items = page.get("items", []) or []
                    term_ready = 0
                    id_only = 0
                    skipped_local = 0
                    skipped_episode = 0

                    for item in items:
                        track: Optional[dict] = None
                        track_ref: object = item
                        if isinstance(item, dict):
                            raw_track = item.get("track")
                            if raw_track is None:
                                raw_track = item.get("item")
                            if isinstance(raw_track, dict):
                                track = raw_track
                                track_ref = raw_track
                            elif raw_track is not None:
                                track_ref = raw_track
                            elif isinstance(item.get("name"), str):
                                track = item

                        if track and track.get("is_local"):
                            skipped_local += 1
                            continue
                        if track and track.get("type") == "episode":
                            skipped_episode += 1
                            continue

                        if track:
                            term = self._track_search_text(track)
                            if term:
                                term_ready += 1
                                continue

                        unresolved_id = self._spotify_track_id_from_value(track_ref)
                        if not unresolved_id and track:
                            unresolved_id = self._spotify_track_id_from_value(track)
                        if unresolved_id:
                            id_only += 1

                    lines.append(
                        f"{label}/{market_label}: items={len(items)} term_ready={term_ready} "
                        f"id_only={id_only} local={skipped_local} episodes={skipped_episode} "
                        f"next={'yes' if page.get('next') else 'no'}"
                    )

                    if items and isinstance(items[0], dict):
                        item0 = items[0]
                        item_keys = ",".join(sorted(str(k) for k in item0.keys())[:10])
                        lines.append(f"{label}/{market_label}: item0_keys={item_keys}")
                        raw_track0 = item0.get("track")
                        if isinstance(raw_track0, dict):
                            track_keys = ",".join(sorted(str(k) for k in raw_track0.keys())[:12])
                            sample_name = raw_track0.get("name")
                            lines.append(
                                f"{label}/{market_label}: track0_keys={track_keys} track0_name={sample_name!r}"
                            )
                        elif raw_track0 is not None:
                            lines.append(f"{label}/{market_label}: track0_type={type(raw_track0).__name__}")
                except Exception as e:
                    status = self.spotify_error_status(e)
                    status_text = str(status) if status is not None else "unknown"
                    lines.append(f"{label}/{market_label}: error={status_text} {e}")

            try:
                resolved_terms = self._spotify_terms_sync("playlist", item_id, sp)
                lines.append(f"{label}/resolver: terms={len(resolved_terms)}")
            except Exception as e:
                status = self.spotify_error_status(e)
                status_text = str(status) if status is not None else "unknown"
                lines.append(f"{label}/resolver: error={status_text} {e}")

        output = "\n".join(lines)
        if len(output) > 1900:
            output = output[:1897] + "..."
        return output

    def spotify_health_check_sync(self) -> str:
        if not self.spotify:
            return "Spotify client is not configured. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in .env and restart."
        try:
            result = self.spotify.search(q="artist:depeche mode", type="track", limit=1)
            items = result.get("tracks", {}).get("items", [])
            if items:
                return "Spotify auth looks good."
            return "Spotify auth worked, but search returned no items."
        except Exception as e:
            err_text = str(e)
            if "401" in err_text or "Unauthorized" in err_text:
                return "Spotify auth failed (401). Recheck SPOTIFY_CLIENT_ID/SPOTIFY_CLIENT_SECRET in .env, then restart bot."
            if "403" in err_text:
                return "Spotify denied access (403). Check app permissions/restrictions in Spotify Developer Dashboard."
            return f"Spotify check failed: {e}"

    def spotify_user_token_ready(self) -> bool:
        if not self.spotify_user:
            return False
        auth_manager = getattr(self.spotify_user, "auth_manager", None)
        if not auth_manager:
            return False
        try:
            token_info = auth_manager.get_cached_token()
            if not token_info:
                return False
            if hasattr(auth_manager, "is_token_expired") and auth_manager.is_token_expired(token_info):
                return False
            required_scopes = set(str(getattr(auth_manager, "scope", "")).split())
            token_scopes = set(str(token_info.get("scope", "")).split())
            if required_scopes and not required_scopes.issubset(token_scopes):
                return False
            return True
        except Exception:
            return False

    def spotify_user_scope_status(self) -> str:
        if not self.spotify_user:
            return "user_scopes=missing-client"
        auth_manager = getattr(self.spotify_user, "auth_manager", None)
        if not auth_manager:
            return "user_scopes=missing-auth-manager"
        token_info = auth_manager.get_cached_token()
        if not token_info:
            return "user_scopes=no-token"
        required = set(str(getattr(auth_manager, "scope", "")).split())
        got = set(str(token_info.get("scope", "")).split())
        missing = sorted(required - got)
        if missing:
            return f"user_scopes=missing:{','.join(missing)}"
        return "user_scopes=ok"

    def spotify_user_auth_url_sync(self) -> str:
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "").strip()
        if not self.spotify_user:
            return (
                "Spotify user auth is not configured. Set SPOTIFY_REDIRECT_URI in .env "
                "(example: http://127.0.0.1:9090/callback), then restart the bot."
            )

        auth_manager = getattr(self.spotify_user, "auth_manager", None)
        if not auth_manager:
            return "Spotify user auth manager is unavailable."

        try:
            auth_url = auth_manager.get_authorize_url()
        except Exception as e:
            return f"Could not build Spotify auth URL: {e}"

        return (
            "Open this Spotify login URL, approve access, then paste the FULL redirected callback URL into:\n"
            f"`{self.prefix}spotifycode <callback_url>`\n\n{auth_url}"
        )

    def spotify_user_exchange_callback_sync(self, callback_url: str) -> str:
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "").strip()
        if not self.spotify_user:
            return (
                "Spotify user auth is not configured. Set SPOTIFY_REDIRECT_URI in .env "
                "(example: http://127.0.0.1:9090/callback), then restart the bot."
            )

        auth_manager = getattr(self.spotify_user, "auth_manager", None)
        if not auth_manager:
            return "Spotify user auth manager is unavailable."

        try:
            code = auth_manager.parse_response_code(callback_url)
            if not code:
                return (
                    "Could not parse auth code from callback URL. "
                    "Copy the full redirected URL from your browser address bar."
                )

            auth_manager.get_access_token(code, check_cache=False)
            me = self.spotify_user.current_user()
            user_name = me.get("display_name") or me.get("id") or "unknown"
            scope_status = self.spotify_user_scope_status()
            return f"Spotify user auth ready as `{user_name}`. {scope_status}"
        except Exception as e:
            msg = str(e)
            if "INVALID_CLIENT" in msg or "redirect_uri" in msg.lower():
                return (
                    "Spotify user auth failed: redirect URI mismatch. "
                    f"Set this exact URI in Spotify App Settings > Redirect URIs: {redirect_uri}"
                )
            if "user not registered" in msg.lower() or "developer dashboard" in msg.lower():
                return (
                    "Spotify user auth failed: your Spotify account is not allowed for this app yet. "
                    "Add your Spotify account email in Spotify Dashboard > User Management."
                )
            return (
                "Spotify user auth failed. Recheck Redirect URI and User Management in Spotify dashboard. "
                f"Details: {e}"
            )

    def spotify_user_login_sync(self) -> str:
        # Backward-compatible wrapper if older code paths call this.
        return self.spotify_user_auth_url_sync()

    def spotify_user_status_sync(self) -> str:
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "").strip()
        if not self.spotify_user:
            return (
                "Spotify user auth is not configured. Set SPOTIFY_REDIRECT_URI in .env "
                "(example: http://127.0.0.1:9090/callback), then restart the bot."
            )
        try:
            if self.spotify_user_token_ready():
                me = self.spotify_user.current_user()
                user_name = me.get("display_name") or me.get("id") or "unknown"
                return f"Spotify user auth ready as `{user_name}`. {self.spotify_user_scope_status()}"
            return (
                "Spotify user auth not completed for required playlist scopes yet. "
                "Run `.spotifylogin`, approve in browser, then run `.spotifycode <callback_url>`."
            )
        except Exception as e:
            msg = str(e)
            if "INVALID_CLIENT" in msg or "redirect_uri" in msg.lower():
                return (
                    "Spotify user auth failed: redirect URI mismatch. "
                    f"Set this exact URI in Spotify App Settings > Redirect URIs: {redirect_uri}"
                )
            if "user not registered" in msg.lower() or "developer dashboard" in msg.lower():
                return (
                    "Spotify user auth failed: your Spotify account is not allowed for this app yet. "
                    "Add your Spotify account email in Spotify Dashboard > User Management."
                )
            return (
                "Spotify user auth failed. Recheck Redirect URI and User Management in Spotify dashboard. "
                f"Details: {e}"
            )

    def _is_youtube_playlist_url(self, query: str) -> bool:
        return bool(YOUTUBE_PLAYLIST_URL_RE.search(query))

    def spotify_error_status(self, exc: Exception) -> Optional[int]:
        status = getattr(exc, "http_status", None)
        if isinstance(status, int):
            return status
        text = str(exc)
        if "401" in text:
            return 401
        if "403" in text:
            return 403
        if "404" in text:
            return 404
        return None

    def _youtube_playlist_terms_sync(self, playlist_url: str) -> list[str]:
        info = ytdl_playlist.extract_info(playlist_url, download=False)
        if not info:
            return []

        entries = info.get("entries") or []
        terms: list[str] = []
        for entry in entries:
            if not entry:
                continue

            webpage_url = entry.get("webpage_url")
            if isinstance(webpage_url, str) and webpage_url:
                terms.append(webpage_url)
                continue

            entry_url = entry.get("url")
            if isinstance(entry_url, str) and entry_url.startswith("http"):
                terms.append(entry_url)
                continue

            video_id = entry.get("id")
            if isinstance(video_id, str) and video_id:
                terms.append(f"https://www.youtube.com/watch?v={video_id}")

        return terms

    def parse_play_request(self, raw_query: str) -> tuple[str, bool]:
        query = raw_query.strip()
        shuffle_requested = False

        while query:
            lowered = query.lower()
            if lowered in ("--shuffle", "-s", "shuffle"):
                shuffle_requested = True
                query = ""
                continue
            if lowered.startswith("--shuffle "):
                shuffle_requested = True
                query = query[10:].strip()
                continue
            if lowered.startswith("-s "):
                shuffle_requested = True
                query = query[3:].strip()
                continue
            if lowered.startswith("shuffle "):
                shuffle_requested = True
                query = query[8:].strip()
                continue
            break

        return query, shuffle_requested

    async def resolve_queries(self, query: str) -> tuple[list[str], Optional[str]]:
        spotify_target = self._spotify_kind_and_id(query)
        if spotify_target:
            self.refresh_spotify_clients_from_env()
            if not self.spotify and not self.spotify_user:
                raise RuntimeError(
                    "Spotify link detected, but Spotify API is not configured. "
                    "Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in .env."
                )

            kind, item_id = spotify_target
            loop = asyncio.get_running_loop()
            primary_client = self.spotify or self.spotify_user
            if kind == "playlist" and self.spotify_user and self.spotify_user_token_ready():
                primary_client = self.spotify_user
            try:
                terms = await loop.run_in_executor(None, lambda: self._spotify_terms_sync(kind, item_id, primary_client))
            except Exception as e:
                err_text = str(e)
                err_status = self.spotify_error_status(e)
                if (
                    kind == "playlist"
                    and self.spotify_user
                    and self.spotify_user_token_ready()
                    and primary_client is self.spotify
                    and (err_status == 401 or "Unauthorized" in err_text)
                ):
                    try:
                        terms = await loop.run_in_executor(
                            None, lambda: self._spotify_terms_sync(kind, item_id, self.spotify_user)
                        )
                    except Exception as user_e:
                        user_text = str(user_e)
                        user_status = self.spotify_error_status(user_e)
                        if user_status == 401 or "Unauthorized" in user_text:
                            raise RuntimeError(
                                "Spotify playlist access still failed (401) under user auth. "
                                "Run `.spotifylogin`, approve Spotify, then run `.spotifycode <callback_url>` and retry."
                            ) from user_e
                        raise RuntimeError(f"Spotify user-auth playlist error: {user_e}") from user_e
                elif kind == "playlist" and primary_client is self.spotify_user and (err_status == 401 or "Unauthorized" in err_text):
                    raise RuntimeError(
                        "Spotify user token was rejected for playlist access (401). "
                        "Run `.spotifylogin` and then `.spotifycode <callback_url>` again "
                        "to refresh required playlist scopes."
                    ) from e
                elif err_status == 401 or "Unauthorized" in err_text:
                    if kind == "playlist" and self.spotify_user and not self.spotify_user_token_ready():
                        raise RuntimeError(
                            "Spotify playlist needs user auth with playlist scopes. "
                            "Run `.spotifylogin` then `.spotifycode <callback_url>` and retry."
                        ) from e
                    raise RuntimeError(
                        "Spotify playlist access failed (401). Track links can still work if creds are valid. "
                        "This usually means the playlist is private/collaborative (or not a public playlist). "
                        "Make it public and try again, or run `.spotifylogin` then `.spotifycode` for user auth."
                    ) from e
                if err_status == 403:
                    raise RuntimeError(
                        "Spotify denied access (403). If this is a playlist, it may be private. "
                        "Make it public and try again."
                    ) from e
                if err_status == 404:
                    raise RuntimeError("Spotify item not found. Check that the link is valid and public.") from e
                raise RuntimeError(f"Spotify API error while loading link: {e}") from e
            if not terms:
                raise RuntimeError(
                    "Could not read tracks from that Spotify link. "
                    "This can happen if tracks are unavailable for the current market. "
                    "Try setting SPOTIFY_MARKET in .env (example: US), ensure `.spotifylogin`/`.spotifycode` is completed, "
                    "or run `.spotifyprobe <playlist_url>` for diagnostics."
                )

            if len(terms) > PLAYLIST_ITEM_CAP:
                terms = terms[:PLAYLIST_ITEM_CAP]
                return terms, f"Spotify {kind} detected. Loaded first {PLAYLIST_ITEM_CAP} songs."
            return terms, f"Spotify {kind} detected."

        if self._is_youtube_playlist_url(query):
            loop = asyncio.get_running_loop()
            terms = await loop.run_in_executor(None, lambda: self._youtube_playlist_terms_sync(query))
            if not terms:
                raise RuntimeError("Could not read tracks from that YouTube playlist link.")

            if len(terms) > PLAYLIST_ITEM_CAP:
                terms = terms[:PLAYLIST_ITEM_CAP]
                return terms, f"YouTube playlist detected. Loaded first {PLAYLIST_ITEM_CAP} songs."
            return terms, "YouTube playlist detected."

        return [query], None

    async def import_remaining_queries(
        self,
        guild_id: int,
        channel_id: int,
        requester_mention: str,
        queries: list[str],
    ) -> None:
        if not queries:
            return

        state = self.get_state(guild_id)
        added_count = 0
        failed_count = 0

        for entry in queries:
            try:
                track = await self.extract_track(entry, requester_mention)
                state.queue.append(track)
                added_count += 1
            except Exception as e:
                failed_count += 1
                log.warning("Background load failed for '%s': %s", entry, e)

        guild = self.bot.get_guild(guild_id)
        if guild:
            channel = guild.get_channel(channel_id)
            if channel and isinstance(channel, discord.abc.Messageable):
                summary = f"Playlist import complete: added `{added_count}` track(s)"
                if failed_count:
                    summary += f", skipped `{failed_count}`."
                else:
                    summary += "."
                await channel.send(summary)

            await self.refresh_now_playing_embed(guild)

    async def play_next(self, guild: discord.Guild) -> None:
        state = self.get_state(guild.id)
        voice_client = guild.voice_client
        if not voice_client:
            return

        async with state.lock:
            if not state.queue:
                state.current = None
                if voice_client.is_connected():
                    await voice_client.disconnect()
                await self.delete_control_panel(guild)
                return

            track = state.queue.pop(0)
            state.current = track

            def after_play(error: Optional[Exception]) -> None:
                if error:
                    log.error("Player error in guild %s: %s", guild.id, error)
                asyncio.run_coroutine_threadsafe(self.play_next(guild), self.bot.loop)

            raw_source = discord.FFmpegPCMAudio(track.stream_url, **FFMPEG_OPTIONS)
            source = discord.PCMVolumeTransformer(raw_source, volume=state.volume)
            voice_client.play(source, after=after_play)

        await self.send_now_playing_embed(guild)
        await self.sync_voice_channel_status(guild)

    async def upsert_control_panel(self, guild: discord.Guild, *, force_new_message: bool = False) -> None:
        state = self.get_state(guild.id)
        if not state.channel_id:
            return
        channel = guild.get_channel(state.channel_id)
        if not channel or not isinstance(channel, discord.abc.Messageable):
            return

        embed = self.build_now_playing_embed(guild)
        view = MusicControlView(self, guild.id)

        if force_new_message and state.control_message_id and hasattr(channel, "fetch_message"):
            try:
                old_message = await channel.fetch_message(state.control_message_id)
                await old_message.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
            finally:
                state.control_message_id = None

        if state.control_message_id and hasattr(channel, "fetch_message"):
            try:
                message = await channel.fetch_message(state.control_message_id)
                await message.edit(embed=embed, view=view)
                return
            except discord.NotFound:
                state.control_message_id = None
            except discord.Forbidden:
                return
            except discord.HTTPException as e:
                log.warning("Failed to edit control panel for guild %s: %s", guild.id, e)
                state.control_message_id = None

        try:
            message = await channel.send(embed=embed, view=view)
        except discord.HTTPException as e:
            log.warning("Failed to send control panel for guild %s: %s", guild.id, e)
            return
        state.control_message_id = message.id

    async def delete_control_panel(self, guild: Optional[discord.Guild]) -> None:
        if not guild:
            return
        state = self.get_state(guild.id)
        if not state.control_message_id:
            return

        channel = guild.get_channel(state.channel_id) if state.channel_id else None
        if not channel or not hasattr(channel, "fetch_message"):
            state.control_message_id = None
            return

        try:
            message = await channel.fetch_message(state.control_message_id)
            await message.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
        finally:
            state.control_message_id = None

    async def send_now_playing_embed(self, guild: discord.Guild) -> None:
        await self.upsert_control_panel(guild, force_new_message=True)

    async def refresh_now_playing_embed(self, guild: Optional[discord.Guild]) -> None:
        if not guild:
            return
        await self.upsert_control_panel(guild)

    def build_now_playing_embed(self, guild: discord.Guild) -> discord.Embed:
        state = self.get_state(guild.id)
        embed = discord.Embed(
            title="KithWave: Night Choir",
            color=self.embed_color,
            description="Gothic echoes drift through the hall.",
        )

        if state.current:
            embed.add_field(name="Now Playing", value=f"[{state.current.title}]({state.current.webpage_url})", inline=False)
            embed.add_field(name="Duration", value=f"`{format_duration(state.current.duration)}`", inline=True)
            embed.add_field(name="Requested By", value=state.current.requested_by, inline=True)
            if state.current.thumbnail:
                embed.set_thumbnail(url=state.current.thumbnail)
        else:
            embed.add_field(name="Now Playing", value=f"Silence in the crypt. Use `{self.prefix}play` to begin.", inline=False)

        status = "Paused" if guild.voice_client and guild.voice_client.is_paused() else "Playing"
        if not guild.voice_client or not guild.voice_client.is_playing():
            status = "Idle"

        embed.add_field(name="Status", value=f"`{status}`", inline=True)
        embed.add_field(name="Queue Size", value=f"`{len(state.queue)}`", inline=True)
        embed.add_field(name="Volume", value=f"`{int(state.volume * 100)}%`", inline=True)
        embed.set_footer(text="KithWave | velvet dusk radio")
        return embed

    def build_queue_embed(self, guild: discord.Guild) -> discord.Embed:
        state = self.get_state(guild.id)
        embed = discord.Embed(
            title="KithWave Queue",
            color=self.embed_color,
            description="Hymns waiting in the candlelight.",
        )

        if state.current:
            embed.add_field(name="Now Playing", value=f"[{state.current.title}]({state.current.webpage_url})", inline=False)

        if not state.queue:
            embed.add_field(name="Up Next", value="No hymns are waiting.", inline=False)
        else:
            lines = []
            for idx, track in enumerate(state.queue[:10], start=1):
                lines.append(f"**{idx}.** [{track.title}]({track.webpage_url}) | `{format_duration(track.duration)}`")
            embed.add_field(name="Up Next", value="\n".join(lines), inline=False)

        embed.set_footer(text="KithWave")
        return embed

    async def pause_resume_guild(self, guild: Optional[discord.Guild]) -> tuple[bool, str]:
        if not guild or not guild.voice_client:
            return False, "I am not connected to voice."
        vc = guild.voice_client
        if vc.is_playing():
            vc.pause()
            return True, "Paused."
        if vc.is_paused():
            vc.resume()
            return True, "Resumed."
        return False, "Nothing is currently playing."

    async def skip_guild(self, guild: Optional[discord.Guild]) -> tuple[bool, str]:
        if not guild or not guild.voice_client:
            return False, "I am not connected to voice."
        vc = guild.voice_client
        if vc.is_playing() or vc.is_paused():
            vc.stop()
            return True, "Skipped."
        return False, "Nothing to skip."

    async def stop_guild(self, guild: Optional[discord.Guild]) -> tuple[bool, str]:
        if not guild:
            return False, "Guild not found."
        state = self.get_state(guild.id)
        state.queue.clear()
        state.current = None
        vc = guild.voice_client
        if vc:
            if vc.is_playing() or vc.is_paused():
                vc.stop()
            await vc.disconnect()
        await self.delete_control_panel(guild)
        return True, "Stopped playback, cleared queue, and disconnected."

    async def set_volume_percent(self, guild: Optional[discord.Guild], percent: int) -> tuple[bool, str]:
        if not guild:
            return False, "Guild not found."
        clamped = max(0, min(200, percent))
        state = self.get_state(guild.id)
        state.volume = clamped / 100.0
        vc = guild.voice_client
        if vc and isinstance(vc.source, discord.PCMVolumeTransformer):
            vc.source.volume = state.volume
        return True, f"Volume set to `{clamped}%`."

    async def adjust_volume(self, guild: Optional[discord.Guild], delta_percent: int) -> tuple[bool, str]:
        if not guild:
            return False, "Guild not found."
        state = self.get_state(guild.id)
        current = int(state.volume * 100)
        return await self.set_volume_percent(guild, current + delta_percent)

    async def shuffle_queue(self, guild: Optional[discord.Guild]) -> tuple[bool, str]:
        if not guild:
            return False, "Guild not found."
        state = self.get_state(guild.id)
        if len(state.queue) < 2:
            return False, "Need at least 2 queued tracks to shuffle."
        random.shuffle(state.queue)
        return True, f"Shuffled `{len(state.queue)}` queued tracks."

    @commands.command(name="play")
    async def play_cmd(self, ctx: commands.Context, *, query: str) -> None:
        if not ctx.guild or not isinstance(ctx.author, discord.Member):
            await ctx.send("This command works in a server only.")
            return

        parsed_query, shuffle_requested = self.parse_play_request(query)
        if not parsed_query:
            await ctx.send(f"Usage: `{self.prefix}play [--shuffle] <query or playlist link>`")
            return

        try:
            voice_client = await self.ensure_voice(ctx.guild, ctx.author)
        except RuntimeError as e:
            await ctx.send(str(e))
            return

        state = self.get_state(ctx.guild.id)
        state.channel_id = ctx.channel.id

        async with ctx.typing():
            try:
                pending_queries, source_note = await self.resolve_queries(parsed_query)
            except RuntimeError as e:
                await ctx.send(str(e))
                return

            added: list[Track] = []
            failed_early: list[str] = []
            preload_target = 3 if len(pending_queries) > 3 else len(pending_queries)
            ordered_queries = list(pending_queries)
            if shuffle_requested and len(ordered_queries) > 1:
                random.shuffle(ordered_queries)

            remaining_queries: list[str] = []
            for entry in ordered_queries:
                if len(added) >= preload_target:
                    remaining_queries.append(entry)
                    continue
                try:
                    track = await self.extract_track(entry, ctx.author.mention)
                    state.queue.append(track)
                    added.append(track)
                except Exception as e:
                    failed_early.append(entry)
                    log.warning("Failed to preload entry '%s': %s", entry, e)

            if failed_early:
                remaining_queries = failed_early + remaining_queries

        if not added:
            await ctx.send("I couldn't load anything from that request.")
            return

        if remaining_queries:
            asyncio.create_task(
                self.import_remaining_queries(
                    guild_id=ctx.guild.id,
                    channel_id=ctx.channel.id,
                    requester_mention=ctx.author.mention,
                    queries=remaining_queries,
                )
            )

        embed = discord.Embed(color=self.embed_color, title="Added to KithWave Queue")
        if len(pending_queries) > 1:
            embed.description = (
                f"Queued **{len(added)}** now, importing **{len(remaining_queries)}** more in background."
            )
            embed.add_field(name="First", value=f"[{added[0].title}]({added[0].webpage_url})", inline=False)
        elif len(added) == 1:
            track = added[0]
            embed.description = f"[{track.title}]({track.webpage_url})"
            embed.add_field(name="Length", value=f"`{format_duration(track.duration)}`", inline=True)
            if track.thumbnail:
                embed.set_thumbnail(url=track.thumbnail)
        source_bits = []
        if source_note:
            source_bits.append(source_note)
        if shuffle_requested and len(pending_queries) > 1:
            source_bits.append("Shuffled playlist order.")
        if source_bits:
            embed.add_field(name="Source", value=" ".join(source_bits), inline=False)
        embed.set_footer(text="KithWave")
        await ctx.send(embed=embed)

        if not voice_client.is_playing() and not voice_client.is_paused():
            await self.play_next(ctx.guild)

    @commands.command(name="queue")
    async def queue_cmd(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            await ctx.send("This command works in a server only.")
            return
        await ctx.send(embed=self.build_queue_embed(ctx.guild))

    @commands.command(name="skip")
    async def skip_cmd(self, ctx: commands.Context) -> None:
        ok, message = await self.skip_guild(ctx.guild)
        await ctx.send(message)
        if ok:
            await self.refresh_now_playing_embed(ctx.guild)

    @commands.command(name="pause")
    async def pause_cmd(self, ctx: commands.Context) -> None:
        ok, message = await self.pause_resume_guild(ctx.guild)
        await ctx.send(message)
        if ok:
            await self.refresh_now_playing_embed(ctx.guild)

    @commands.command(name="stop")
    async def stop_cmd(self, ctx: commands.Context) -> None:
        ok, message = await self.stop_guild(ctx.guild)
        await ctx.send(message)

    @commands.command(name="shuffle")
    async def shuffle_cmd(self, ctx: commands.Context) -> None:
        ok, message = await self.shuffle_queue(ctx.guild)
        await ctx.send(message)
        if ok:
            await self.refresh_now_playing_embed(ctx.guild)

    @commands.command(name="nowplaying")
    async def nowplaying_cmd(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            await ctx.send("This command works in a server only.")
            return
        state = self.get_state(ctx.guild.id)
        state.channel_id = ctx.channel.id
        await self.upsert_control_panel(ctx.guild)

    @commands.command(name="lyrics")
    async def lyrics_cmd(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            await ctx.send("This command works in a server only.")
            return
        async with ctx.typing():
            embed, message = await self.current_lyrics_embed(ctx.guild)
        if embed:
            await ctx.send(embed=embed)
            return
        await ctx.send(message)

    @commands.command(name="volume")
    async def volume_cmd(self, ctx: commands.Context, percent: Optional[int] = None) -> None:
        if not ctx.guild:
            await ctx.send("This command works in a server only.")
            return
        state = self.get_state(ctx.guild.id)
        if percent is None:
            await ctx.send(f"Current volume: `{int(state.volume * 100)}%`")
            return
        ok, message = await self.set_volume_percent(ctx.guild, percent)
        await ctx.send(message)
        if ok:
            await self.refresh_now_playing_embed(ctx.guild)

    @commands.command(name="spotifycheck")
    async def spotifycheck_cmd(self, ctx: commands.Context) -> None:
        if not await self.can_manage_spotify_auth(ctx):
            await ctx.send("Only server managers can use Spotify setup commands.")
            return
        self.refresh_spotify_clients_from_env()
        try:
            async with ctx.typing():
                loop = asyncio.get_running_loop()
                app_msg = await asyncio.wait_for(loop.run_in_executor(None, self.spotify_health_check_sync), timeout=20)
                user_msg = await asyncio.wait_for(loop.run_in_executor(None, self.spotify_user_status_sync), timeout=20)
        except asyncio.TimeoutError:
            await ctx.send("Spotify check timed out. Try again in a few seconds.")
            return
        await ctx.send(f"{app_msg}\n{user_msg}")

    @commands.command(name="spotifylogin")
    async def spotifylogin_cmd(self, ctx: commands.Context) -> None:
        if not await self.can_manage_spotify_auth(ctx):
            await ctx.send("Only server managers can use Spotify setup commands.")
            return
        self.refresh_spotify_clients_from_env()
        try:
            loop = asyncio.get_running_loop()
            message = await asyncio.wait_for(loop.run_in_executor(None, self.spotify_user_auth_url_sync), timeout=20)
        except asyncio.TimeoutError:
            await ctx.send("Spotify login URL generation timed out. Try again.")
            return
        await ctx.send(message)

    @commands.command(name="spotifycode")
    async def spotifycode_cmd(self, ctx: commands.Context, *, callback_url: str) -> None:
        if not await self.can_manage_spotify_auth(ctx):
            await ctx.send("Only server managers can use Spotify setup commands.")
            return
        self.refresh_spotify_clients_from_env()
        try:
            async with ctx.typing():
                loop = asyncio.get_running_loop()
                message = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: self.spotify_user_exchange_callback_sync(callback_url)),
                    timeout=30,
                )
        except asyncio.TimeoutError:
            await ctx.send("Spotify code exchange timed out. Paste the callback URL again.")
            return
        await ctx.send(message)

    @commands.command(name="spotifydebug")
    async def spotifydebug_cmd(self, ctx: commands.Context) -> None:
        if not await self.can_manage_spotify_auth(ctx):
            await ctx.send("Only server managers can use Spotify setup commands.")
            return
        self.refresh_spotify_clients_from_env()
        redirect = os.getenv("SPOTIFY_REDIRECT_URI", "").strip()
        msg = (
            f"cwd=`{os.getcwd()}` | "
            f"spotify_client={'yes' if self.spotify else 'no'} | "
            f"spotify_user={'yes' if self.spotify_user else 'no'} | "
            f"redirect=`{redirect or 'missing'}` | "
            f"market=`{self.spotify_market or 'none'}` | "
            f"user_token_ready={'yes' if self.spotify_user_token_ready() else 'no'} | "
            f"{self.spotify_user_scope_status()}"
        )
        await ctx.send(msg)

    @commands.command(name="spotifyprobe")
    async def spotifyprobe_cmd(self, ctx: commands.Context, *, playlist_ref: str) -> None:
        if not await self.can_manage_spotify_auth(ctx):
            await ctx.send("Only server managers can use Spotify setup commands.")
            return
        self.refresh_spotify_clients_from_env()
        try:
            async with ctx.typing():
                loop = asyncio.get_running_loop()
                message = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: self.spotify_playlist_probe_sync(playlist_ref)),
                    timeout=30,
                )
        except asyncio.TimeoutError:
            await ctx.send("Spotify probe timed out. Try again in a few seconds.")
            return
        await ctx.send(f"```text\n{message}\n```")

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if not self.bot.user or member.id != self.bot.user.id:
            return
        if before.channel and after.channel is None:
            await self.delete_control_panel(member.guild)


class KithWaveBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        prefix = os.getenv("BOT_PREFIX", "!")
        super().__init__(command_prefix=prefix, intents=intents)
        self.prefix = prefix

    async def setup_hook(self) -> None:
        await self.add_cog(MusicCog(self, self.prefix))

    async def on_ready(self) -> None:
        log.info("Logged in as %s (%s)", self.user, self.user.id if self.user else "unknown")
        log.info("Prefix commands enabled: %s", self.prefix)


def main() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN in environment.")
    bot = KithWaveBot()
    bot.run(token)


if __name__ == "__main__":
    main()
