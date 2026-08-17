from sales_support_agent.integrations.youtube_source import parse_youtube_feed


def test_parse_youtube_feed_returns_owned_video_metadata() -> None:
    payload = b'''<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"
          xmlns:yt="http://www.youtube.com/xml/schemas/2015">
      <entry>
        <yt:videoId>P2APW9iqEzU</yt:videoId>
        <yt:channelId>UCDvyO7gjwDzmMg2fzZHn49Q</yt:channelId>
        <title>Shopify Opens Native B2B</title>
        <published>2026-08-01T12:00:00+00:00</published>
      </entry>
    </feed>'''
    videos = parse_youtube_feed(payload)
    assert len(videos) == 1
    assert videos[0].video_id == "P2APW9iqEzU"
    assert videos[0].url == "https://www.youtube.com/watch?v=P2APW9iqEzU"
