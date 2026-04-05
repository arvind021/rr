# Copyright (c) 2025 AnonymousX1025
# Licensed under the MIT License.
# This file is part of AnonXMusic


import os
import re
import random
import asyncio
import aiohttp
import redis.asyncio as aioredis
from pathlib import Path

from py_yt import Playlist, VideosSearch

from anony import config, logger
from anony.helpers import Track, utils


CACHE_DIR = "anony/cache/audio"
os.makedirs(CACHE_DIR, exist_ok=True)

# Ek saath max 3 download (overload nahi hoga)
_download_semaphore = asyncio.Semaphore(3)


class YouTube:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.cookies = []
        self.checked = False
        self.cookie_dir = "anony/cookies"
        self.warned = False
        self.regex = re.compile(
            r"(https?://)?(www\.|m\.|music\.)?"
            r"(youtube\.com/(watch\?v=|shorts/|playlist\?list=)|youtu\.be/)"
            r"([A-Za-z0-9_-]{11}|PL[A-Za-z0-9_-]+)([&?][^\s]*)?"
        )
        self.iregex = re.compile(
            r"https?://(?:www\.|m\.|music\.)?(?:youtube\.com|youtu\.be)"
            r"(?!/(watch\?v=[A-Za-z0-9_-]{11}|shorts/[A-Za-z0-9_-]{11}"
            r"|playlist\?list=PL[A-Za-z0-9_-]+|[A-Za-z0-9_-]{11}))\S*"
        )

    def get_cookies(self):
        if not self.checked:
            for file in os.listdir(self.cookie_dir):
                if file.endswith(".txt"):
                    self.cookies.append(f"{self.cookie_dir}/{file}")
            self.checked = True
        if not self.cookies:
            if not self.warned:
                self.warned = True
                logger.warning("Cookies are missing; downloads might fail.")
            return None
        return random.choice(self.cookies)

    async def save_cookies(self, urls: list[str]) -> None:
        logger.info("Saving cookies from urls...")
        async with aiohttp.ClientSession() as session:
            for url in urls:
                name = url.split("/")[-1]
                link = "https://batbin.me/raw/" + name
                async with session.get(link) as resp:
                    resp.raise_for_status()
                    with open(f"{self.cookie_dir}/{name}.txt", "wb") as fw:
                        fw.write(await resp.read())
        logger.info(f"Cookies saved in {self.cookie_dir}.")

    def valid(self, url: str) -> bool:
        return bool(re.match(self.regex, url))

    def invalid(self, url: str) -> bool:
        return bool(re.match(self.iregex, url))

    async def search(self, query: str, m_id: int, video: bool = False) -> Track | None:
        try:
            _search = VideosSearch(query, limit=1, with_live=False)
            results = await _search.next()
        except Exception:
            return None
        if results and results["result"]:
            data = results["result"][0]
            return Track(
                id=data.get("id"),
                channel_name=data.get("channel", {}).get("name"),
                duration=data.get("duration"),
                duration_sec=utils.to_seconds(data.get("duration")),
                message_id=m_id,
                title=data.get("title")[:25],
                thumbnail=data.get("thumbnails", [{}])[-1].get("url").split("?")[0],
                url=data.get("link"),
                view_count=data.get("viewCount", {}).get("short"),
                video=video,
            )
        return None

    async def playlist(self, limit: int, user: str, url: str, video: bool) -> list[Track | None]:
        tracks = []
        try:
            plist = await Playlist.get(url)
            for data in plist["videos"][:limit]:
                track = Track(
                    id=data.get("id"),
                    channel_name=data.get("channel", {}).get("name", ""),
                    duration=data.get("duration"),
                    duration_sec=utils.to_seconds(data.get("duration")),
                    title=data.get("title")[:25],
                    thumbnail=data.get("thumbnails")[-1].get("url").split("?")[0],
                    url=data.get("link").split("&list=")[0],
                    user=user,
                    view_count="",
                    video=video,
                )
                tracks.append(track)
        except Exception:
            pass
        return tracks

    async def _baby_get_stream_url(self, video_id: str, video: bool = False) -> str | None:
        """Fetch stream URL from BabyAPI with Redis cache + retry logic."""
        api_key = config.BABY_API_KEY
        base_url = config.BABY_BASE_URL
        endpoint = "video" if video else "song"
        cache_key = f"baby:{'video' if video else 'audio'}:{video_id}"

        try:
            redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)

            # Redis cache check
            cached = await redis.get(cache_key)
            if cached:
                logger.info(f"BabyAPI Redis cache hit for {video_id}")
                await redis.aclose()
                return cached

            # BabyAPI fetch — 3 retries
            stream_url = None
            for attempt in range(3):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"{base_url}/api/{endpoint}",
                            params={"query": video_id, "api": api_key},
                            headers={"x-api-key": api_key},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            if resp.status != 200:
                                logger.warning(f"BabyAPI attempt {attempt+1} returned {resp.status}")
                                await asyncio.sleep(1)
                                continue
                            data = await resp.json()
                            stream_url = data.get("stream")
                            if stream_url:
                                break
                except asyncio.TimeoutError:
                    logger.warning(f"BabyAPI timeout attempt {attempt+1} for {video_id}")
                    await asyncio.sleep(1)
                except Exception as ex:
                    logger.warning(f"BabyAPI attempt {attempt+1} error: {ex}")
                    await asyncio.sleep(1)

            if not stream_url:
                await redis.aclose()
                return None

            # Redis mein 20 min cache (1 hour se kam — URL expire hone se pehle)
            await redis.set(cache_key, stream_url, ex=1200)
            await redis.aclose()

            logger.info(f"BabyAPI stream URL fetched for {video_id}")
            return stream_url

        except Exception as ex:
            logger.warning(f"BabyAPI error: {ex}")
            return None

    def _ytdlp_cache(self, video_id: str) -> None:
        """Sync yt-dlp download for cache — 423 pe fallback."""
        import yt_dlp
        output = f"{CACHE_DIR}/{video_id}.m4a"
        if Path(output).exists():
            return

        cookie = self.get_cookies()
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": output,
            "quiet": True,
            "no_warnings": True,
            "cookiefile": cookie,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
            logger.info(f"Cached via yt-dlp: {video_id}")
        except Exception as ex:
            logger.warning(f"yt-dlp cache failed: {ex}")
            if Path(output).exists():
                Path(output).unlink()

    async def _cache_audio(self, video_id: str, stream_url: str) -> None:
        """
        BabyAPI stream URL se directly download karo local cache mein.
        423 aaye toh yt-dlp se cache karo.
        Lock file se duplicate download prevent.
        """
        output = f"{CACHE_DIR}/{video_id}.m4a"
        lock = f"{CACHE_DIR}/{video_id}.lock"

        # Already downloaded ya downloading hai?
        if Path(output).exists() or Path(lock).exists():
            return

        # Lock banao
        Path(lock).touch()

        async with _download_semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        stream_url,
                        timeout=aiohttp.ClientTimeout(total=120),
                    ) as resp:
                        if resp.status == 423:
                            # Direct download allowed nahi — yt-dlp se karo
                            logger.info(f"423 Locked, switching to yt-dlp cache: {video_id}")
                            await asyncio.to_thread(self._ytdlp_cache, video_id)
                            return

                        if resp.status != 200:
                            logger.warning(f"Cache download failed, status {resp.status}: {video_id}")
                            return

                        with open(output, "wb") as f:
                            async for chunk in resp.content.iter_chunked(64 * 1024):
                                f.write(chunk)

                logger.info(f"Cached audio via BabyAPI stream: {video_id}")

            except asyncio.TimeoutError:
                logger.warning(f"Cache download timeout: {video_id}")
                if Path(output).exists():
                    Path(output).unlink()
            except Exception as ex:
                logger.warning(f"Cache download error: {ex}")
                if Path(output).exists():
                    Path(output).unlink()
            finally:
                # Lock hamesha delete karo
                Path(lock).unlink(missing_ok=True)

    async def download(self, video_id: str, video: bool = False) -> str | None:
        # Layer 1: Local file check (instant ⚡)
        if not video:
            cached = f"{CACHE_DIR}/{video_id}.m4a"
            if Path(cached).exists():
                logger.info(f"Local cache hit: {video_id}")
                return cached

        # Layer 2: BabyAPI (Redis cached ⚡⚡)
        stream_url = await self._baby_get_stream_url(video_id, video)
        if stream_url:
            logger.info(f"BabyAPI stream URL fetched for {video_id}")
            # Background download — lock se duplicate prevent
            if not video:
                cached = f"{CACHE_DIR}/{video_id}.m4a"
                lock = f"{CACHE_DIR}/{video_id}.lock"
                if not Path(cached).exists() and not Path(lock).exists():
                    asyncio.create_task(self._cache_audio(video_id, stream_url))
            return stream_url

        # Layer 3: yt-dlp fallback (last resort)
        logger.warning(f"BabyAPI failed for {video_id}, falling back to yt-dlp")
        return await self._ytdlp_download(video_id, video)

    async def prefetch(self, video_id: str, video: bool = False) -> None:
        """Next song pehle se background mein ready karo."""
        if not video and Path(f"{CACHE_DIR}/{video_id}.m4a").exists():
            return

        stream_url = await self._baby_get_stream_url(video_id, video)
        if stream_url and not video:
            cached = f"{CACHE_DIR}/{video_id}.m4a"
            lock = f"{CACHE_DIR}/{video_id}.lock"
            if not Path(cached).exists() and not Path(lock).exists():
                asyncio.create_task(self._cache_audio(video_id, stream_url))

    async def _ytdlp_download(self, video_id: str, video: bool = False) -> str | None:
        """Last resort fallback — sirf tab jab BabyAPI fail ho."""
        import yt_dlp

        url = self.base + video_id
        ext = "mp4" if video else "m4a"
        filename = f"downloads/{video_id}.{ext}"

        if Path(filename).exists():
            return filename

        cookie = self.get_cookies()
        base_opts = {
            "outtmpl": "downloads/%(id)s.%(ext)s",
            "quiet": True,
            "noplaylist": True,
            "geo_bypass": True,
            "no_warnings": True,
            "overwrites": False,
            "nocheckcertificate": True,
            "cookiefile": cookie,
        }

        if video:
            ydl_opts = {
                **base_opts,
                "format": "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio)",
                "merge_output_format": "mp4",
            }
        else:
            ydl_opts = {
                **base_opts,
                "format": "bestaudio/best",  # FIX: webm opus se change kiya
            }

        def _download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    ydl.download([url])
                except (yt_dlp.utils.DownloadError, yt_dlp.utils.ExtractorError):
                    return None
                except Exception as ex:
                    logger.warning("Download failed: %s", ex)
                    return None
            return filename

        return await asyncio.to_thread(_download)
        
