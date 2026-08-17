"""Read-only YouTube channel feed ingestion for owned video resources."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import Request, urlopen
from xml.etree import ElementTree


YOUTUBE_FEED_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"


@dataclass(frozen=True)
class YouTubeVideo:
    """One public video discovered in an owned channel feed."""

    video_id: str
    channel_id: str
    title: str
    url: str
    published_at: str


def parse_youtube_feed(payload: bytes) -> list[YouTubeVideo]:
    """Parse the public Atom feed without trusting arbitrary HTML."""

    root = ElementTree.fromstring(payload)
    namespaces = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
    }
    videos: list[YouTubeVideo] = []
    for entry in root.findall("atom:entry", namespaces):
        video_id = (entry.findtext("yt:videoId", "", namespaces) or "").strip()
        channel_id = (entry.findtext("yt:channelId", "", namespaces) or "").strip()
        title = (entry.findtext("atom:title", "", namespaces) or "").strip()
        published = (entry.findtext("atom:published", "", namespaces) or "").strip()
        if not video_id or not channel_id or not title:
            continue
        videos.append(
            YouTubeVideo(
                video_id=video_id,
                channel_id=channel_id,
                title=title[:500],
                url=f"https://www.youtube.com/watch?v={video_id}",
                published_at=published,
            )
        )
    return videos


class YouTubeFeedClient:
    """Bounded client for recent public uploads from one configured channel."""

    def __init__(self, *, channel_id: str, timeout_seconds: float = 15.0) -> None:
        self.channel_id = channel_id.strip()
        self.timeout_seconds = timeout_seconds

    def list_recent_videos(self) -> list[YouTubeVideo]:
        if not self.channel_id:
            return []
        request = Request(
            YOUTUBE_FEED_URL.format(channel_id=self.channel_id),
            headers={"User-Agent": "AnataContentOperations/1.0"},
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = response.read(1_000_000)
        return parse_youtube_feed(payload)
