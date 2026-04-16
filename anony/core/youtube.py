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
DOWNLOAD_DIR = "downloads"
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Max 3 concurrent downloads to avoid overload
_download_semaphore = asyncio.Semaphore(3)

# Module-level Redis singleton — avoids per-call connection exhaustion (#1)
_redis: aioredis.Redis | None = None


async def _get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis


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
            # Fix #12: ensure cookie_dir exists before listing
            os.makedirs(self.cookie_dir, exist_ok=True)
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
        # Fix #12: ensure cookie_dir exists before writing
        os.makedirs(self.cookie_dir, exist_ok=True)
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
            # Fix #8: guard against None title
            # Fix #9: guard against None thumbnail URL
            title = (data.get("title") or "")[:25]
            thumb_url = data.get("thumbnails", [{}])[-1].get("url") or ""
            return Track(
                id=data.get("id"),
                channel_name=data.get("channel", {}).get("name"),
                duration=data.get("duration"),
                duration_sec=utils.to_seconds(data.get("duration")),
                message_id=m_id,
                title=title,
                thumbnail=thumb_url.split("?")[0],
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
                # Fix #8: guard against None title
                # Fix #9: guard against None thumbnail URL
                title = (data.get("title") or "")[:25]
                thumb_url = (data.get("thumbnails") or [{}])[-1].get("url") or ""
                track = Track(
                    id=data.get("id"),
                    channel_name=data.get("channel", {}).get("name", ""),
                    duration=data.get("duration"),
                    duration_sec=utils.to_seconds(data.get("duration")),
                    title=title,
                    thumbnail=thumb_url.split("?")[0],
                    url=data.get("link", "").split("&list=")[0],
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

        # Fix #1 & #14: use singleton Redis; use try/finally so connection is never leaked
        redis = await _get_redis()
        try:
            # Redis cache check
            cached = await redis.get(cache_key)
            if cached:
                logger.info(f"BabyAPI Redis cache hit for {video_id}")
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
                return None

            # Cache for 20 minutes
            await redis.set(cache_key, stream_url, ex=1200)
            return stream_url

        except Exception as ex:
            logger.warning(f"BabyAPI error: {ex}")
            return None
        # Fix #14: singleton is intentionally not closed here — it's reused across calls

    def _ytdlp_cache(self, video_id: str) -> None:
        """Sync yt-dlp download for cache — fallback on 423."""
        import yt_dlp
        output = Path(f"{CACHE_DIR}/{video_id}.m4a")
        if output.exists():
            return

        cookie = self.get_cookies()
        ydl_opts = {
            "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            "outtmpl": f"{CACHE_DIR}/{video_id}.%(ext)s",
            "quiet": True,
            "no_warnings": True,
            "cookiefile": cookie,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

            # Rename to .m4a if a different extension was downloaded
            # Fix #4: broaden extension list to include mp3
            for ext in ["webm", "opus", "ogg", "mp3"]:
                src = Path(f"{CACHE_DIR}/{video_id}.{ext}")
                if src.exists():
                    src.rename(output)
                    break

            # Fix #4: warn if no expected file found after download
            if not output.exists():
                logger.warning(f"yt-dlp cache: no output file found for {video_id}")
                return

            logger.info(f"Cached via yt-dlp: {video_id}")
        except Exception as ex:
            logger.warning(f"yt-dlp cache failed: {ex}")
            for ext in ["m4a", "webm", "opus", "ogg", "mp3"]:
                p = Path(f"{CACHE_DIR}/{video_id}.{ext}")
                if p.exists():
                    p.unlink()

    async def _cache_audio(self, video_id: str, stream_url: str) -> None:
        """
        Download audio from BabyAPI stream URL into local cache.
        Falls back to yt-dlp on 423. Uses atomic lock to prevent duplicate downloads.
        """
        output = Path(f"{CACHE_DIR}/{video_id}.m4a")
        lock = Path(f"{CACHE_DIR}/{video_id}.lock")

        # Fix #3: atomic lock using exclusive file creation — eliminates race condition
        try:
            lock.open("x").close()
        except FileExistsError:
            return  # Another coroutine is already downloading this

        if output.exists():
            lock.unlink(missing_ok=True)
            return

        async with _download_semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        stream_url,
                        timeout=aiohttp.ClientTimeout(total=120),
                    ) as resp:
                        if resp.status == 423:
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
                output.unlink(missing_ok=True)
            except Exception as ex:
                logger.warning(f"Cache download error: {ex}")
                output.unlink(missing_ok=True)
            finally:
                # Fix #14 pattern: always release lock regardless of outcome
                lock.unlink(missing_ok=True)

    async def download(self, video_id: str, video: bool = False) -> str | None:
        # Layer 1: Local file check (instant)
        if not video:
            cached = Path(f"{CACHE_DIR}/{video_id}.m4a")
            if cached.exists():
                logger.info(f"Local cache hit: {video_id}")
                return str(cached)

        # Layer 2: BabyAPI — verify URL with HEAD request
        stream_url = await self._baby_get_stream_url(video_id, video)
        if stream_url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(
                        stream_url,
                        timeout=aiohttp.ClientTimeout(total=5),
                        allow_redirects=True,
                    ) as resp:
                        # Fix #10: 301/302 are already followed by allow_redirects=True,
                        # so final status will only ever be 200/206 on success.
                        if resp.status in (200, 206):
                            logger.info(f"BabyAPI stream verified for {video_id}")
                            # Background cache for audio only
                            if not video:
                                cached = Path(f"{CACHE_DIR}/{video_id}.m4a")
                                lock = Path(f"{CACHE_DIR}/{video_id}.lock")
                                if not cached.exists() and not lock.exists():
                                    asyncio.create_task(self._cache_audio(video_id, stream_url))
                            return stream_url
                        else:
                            logger.warning(f"BabyAPI URL dead ({resp.status}), falling back: {video_id}")
            except Exception as ex:
                logger.warning(f"BabyAPI URL verify failed: {ex}, falling back: {video_id}")

        # Layer 3: yt-dlp fallback (last resort)
        logger.warning(f"BabyAPI failed for {video_id}, falling back to yt-dlp")
        return await self._ytdlp_download(video_id, video)

    async def prefetch(self, video_id: str, video: bool = False) -> None:
        """Pre-fetch next song in background before it's needed."""
        if not video:
            if Path(f"{CACHE_DIR}/{video_id}.m4a").exists():
                return
            stream_url = await self._baby_get_stream_url(video_id, video=False)
            if stream_url:
                cached = Path(f"{CACHE_DIR}/{video_id}.m4a")
                lock = Path(f"{CACHE_DIR}/{video_id}.lock")
                if not cached.exists() and not lock.exists():
                    asyncio.create_task(self._cache_audio(video_id, stream_url))
        # Fix #5: video prefetch — fetch stream URL only if we'll actually use it
        else:
            stream_url = await self._baby_get_stream_url(video_id, video=True)
            if stream_url:
                logger.info(f"Prefetched video stream URL for {video_id}")
                # Video streams are not cached locally; URL is in Redis for fast retrieval

    async def _ytdlp_download(self, video_id: str, video: bool = False) -> str | None:
        """Last resort fallback — only when BabyAPI fails."""
        import yt_dlp

        url = self.base + video_id
        ext = "mp4" if video else "m4a"
        filename = f"{DOWNLOAD_DIR}/{video_id}.{ext}"

        if Path(filename).exists():
            return filename

        cookie = self.get_cookies()
        base_opts = {
            "outtmpl": f"{DOWNLOAD_DIR}/%(id)s.%(ext)s",
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
                "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            }

        def _download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    ydl.download([url])
                except (yt_dlp.utils.DownloadError, yt_dlp.utils.ExtractorError) as ex:
                    # Fix #2: log DownloadError/ExtractorError instead of silently swallowing
                    logger.warning(f"yt-dlp DownloadError for {video_id}: {ex}")
                    return None
                except Exception as ex:
                    logger.warning(f"yt-dlp unexpected error for {video_id}: {ex}")
                    return None

            # Find actual downloaded file (extension may differ from requested)
            for actual_ext in ["m4a", "webm", "opus", "ogg", "mp3", "mp4"]:
                actual = Path(f"{DOWNLOAD_DIR}/{video_id}.{actual_ext}")
                if actual.exists():
                    if actual_ext != ext:
                        actual.rename(filename)
                    return filename

            logger.warning(f"yt-dlp download completed but no output file found for {video_id}")
            return None

        return await asyncio.to_thread(_download)
                                
