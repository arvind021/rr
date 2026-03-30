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
        """Fetch stream URL from BabyAPI with Redis cache."""
        api_key = config.BABY_API_KEY
        base_url = config.BABY_BASE_URL
        endpoint = "video" if video else "song"
        cache_key = f"baby:{'video' if video else 'audio'}:{video_id}"

        try:
            redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)

            # Check cache first
            cached = await redis.get(cache_key)
            if cached:
                logger.info(f"BabyAPI cache hit for {video_id}")
                await redis.aclose()
                return cached

            async with aiohttp.ClientSession() as session:
                # Step 1: Get token and file_id
                async with session.get(
                    f"{base_url}/api/{endpoint}",
                    params={"query": video_id, "api": api_key},
                    headers={"x-api-key": api_key},
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"BabyAPI {endpoint} returned {resp.status}")
                        await redis.aclose()
                        return None
                    data = await resp.json()

            token = data.get("token")
            file_id = data.get("file_id") or data.get("id")

            if not token or not file_id:
                logger.warning(f"BabyAPI missing token/file_id: {data}")
                await redis.aclose()
                return None

            # Step 2: Build stream URL
            stream_url = (
                f"{base_url}/api/stream/{file_id}"
                f"?token={token}&api={api_key}"
            )

            # Cache for 1 hour
            await redis.set(cache_key, stream_url, ex=3600)
            await redis.aclose()
            return stream_url

        except Exception as ex:
            logger.warning(f"BabyAPI error: {ex}")
            return None

    async def download(self, video_id: str, video: bool = False) -> str | None:
        # Try BabyAPI first
        stream_url = await self._baby_get_stream_url(video_id, video)
        if stream_url:
            logger.info(f"BabyAPI stream URL fetched for {video_id}")
            return stream_url

        # Fallback: yt-dlp
        logger.warning(f"BabyAPI failed for {video_id}, falling back to yt-dlp")
        return await self._ytdlp_download(video_id, video)

    async def _ytdlp_download(self, video_id: str, video: bool = False) -> str | None:
        import yt_dlp
        url = self.base + video_id
        ext = "mp4" if video else "webm"
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
                "format": "bestaudio[ext=webm][acodec=opus]",
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
