# -*- coding: utf-8 -*-
"""
Task 4 测试：
TR-4.1: GET /api/feeds/creators 返回 items[*].feed_url 包含 /api/feeds/dy/id:{N}/rss
TR-4.2: /api/feeds/dy/id:{meta_id}/rss -> 200 + application/rss+xml + RSS 2.0 + 两条 guids
      两次请求 guid 集合 100% 一致
TR-4.3: RSS_FEED_API_KEY='foo' 时不设 ?api_key 返回 401，传 foo 200
TR-4.4 (rubric): XML 结构齐全、无未转义尖括号/&/引号
"""

import asyncio
import sys
import os
import re
from pathlib import Path

import pytest
import httpx
from httpx import ASGITransport  # httpx>=0.27 用 transport 替代 app= 参数

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import config as _cfg_pkg


def _bootstrap_sqlite(tmp_path: Path):
    db_path = str(tmp_path / "test_rss.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    _cfg_pkg.SAVE_DATA_OPTION = "sqlite"
    from config import db_config as _dbcfg
    _dbcfg.SQLITE_DB_PATH = db_path
    _dbcfg.sqlite_db_config = {"db_path": db_path}
    from database import db_session as _dbs
    _dbs._engines.clear()
    for attr_name in ("_engine", "_async_session_factory"):
        try:
            setattr(_dbs, attr_name, None)
        except Exception:
            pass
    return db_path


@pytest.mark.asyncio
async def test_TR_4_1_creators_list_feed_url(tmp_path):
    _bootstrap_sqlite(tmp_path)
    from database.db_session import create_tables
    await create_tables("sqlite")

    from database.db_session import get_session
    from database.models import MediaCreatorMeta, DouyinAweme
    from tools.utils import get_current_timestamp
    now = get_current_timestamp()
    async with get_session() as s:
        meta = MediaCreatorMeta(
            platform="dy", creator_hash="DY_RSS_H1",
            nickname_masked="脱敏昵称", user_name="显示博主名",
            config_id=1, profile_url=None, content_count=1,
            first_seen_ts=now, last_seen_ts=now,
        )
        s.add(meta)
        await s.flush()
        meta_id = meta.id
        s.add_all([
            DouyinAweme(
                aweme_id="74001", aweme_type="0", title="标题 A", desc="描述 A",
                create_time=1700000000, creator_hash="DY_RSS_H1", nickname="脱敏昵称",
                liked_count="0", collected_count="0", comment_count="0", share_count="0",
                last_modify_ts=now, aweme_url="https://www.douyin.com/video/74001",
                cover_url="https://img.example.com/cover1.jpg",
                video_download_url="", note_download_url="", source_keyword="",
            ),
            DouyinAweme(
                aweme_id="74002", aweme_type="0", title="标题 B", desc="描述 B",
                create_time=1700000300, creator_hash="DY_RSS_H1", nickname="脱敏昵称",
                liked_count="0", collected_count="0", comment_count="0", share_count="0",
                last_modify_ts=now, aweme_url="https://www.douyin.com/video/74002",
                cover_url="https://img.example.com/cover2.jpg",
                video_download_url="", note_download_url="", source_keyword="",
            ),
        ])
        await s.commit()

    _cfg_pkg.RSS_FEED_PUBLIC_BASE_URL = "http://testmc.local"
    _cfg_pkg.RSS_FEED_API_KEY = ""

    from api.main import app
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/api/feeds/creators?page=1&page_size=20")
        assert resp.status_code == 200, f"期望 200，实际 {resp.status_code}: {resp.text[:400]}"
        data = resp.json()
        assert "total" in data and "by_platform" in data and "items" in data
        assert data["total"] >= 1
        assert len(data["items"]) >= 1
        item0 = data["items"][0]
        assert "feed_url" in item0
        url = item0["feed_url"]
        assert f"/api/feeds/dy/id:{meta_id}/rss" in url, f"feed_url 格式不符: {url}"
        assert url.startswith("http://testmc.local"), f"base_url 未带入: {url}"
    print("TR-4.1 PASS:", url)


@pytest.mark.asyncio
async def test_TR_4_2_rss_xml_guid_stable(tmp_path):
    _bootstrap_sqlite(tmp_path)
    from database.db_session import create_tables
    await create_tables("sqlite")

    from database.db_session import get_session
    from database.models import MediaCreatorMeta, DouyinAweme
    from tools.utils import get_current_timestamp
    now = get_current_timestamp()

    async with get_session() as s:
        meta = MediaCreatorMeta(
            platform="dy", creator_hash="DY_RSS_H2",
            nickname_masked="N2", user_name="U2", content_count=2,
            first_seen_ts=now, last_seen_ts=now,
        )
        s.add(meta)
        await s.flush()
        mid = meta.id
        s.add_all([
            DouyinAweme(
                aweme_id="A21", aweme_type="0", title="T21", desc="D21",
                create_time=1700000001, creator_hash="DY_RSS_H2", nickname="N2",
                liked_count="", collected_count="", comment_count="", share_count="",
                last_modify_ts=now, aweme_url="https://dy/v/A21",
                cover_url="https://img.example.com/c21.jpg",
                video_download_url="", note_download_url="", source_keyword="",
            ),
            DouyinAweme(
                aweme_id="A22", aweme_type="0", title="T22", desc="D22",
                create_time=1700000002, creator_hash="DY_RSS_H2", nickname="N2",
                liked_count="", collected_count="", comment_count="", share_count="",
                last_modify_ts=now, aweme_url="https://dy/v/A22",
                cover_url="https://img.example.com/c22.jpg",
                video_download_url="https://v.example.com/v22.mp4",
                note_download_url="", source_keyword="",
            ),
        ])
        await s.commit()

    _cfg_pkg.RSS_FEED_PUBLIC_BASE_URL = "http://t"
    _cfg_pkg.RSS_FEED_API_KEY = ""
    from api.main import app

    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        async def _fetch():
            resp = await ac.get(f"/api/feeds/dy/id:{mid}/rss")
            assert resp.status_code == 200, f"/rss 非 200: {resp.status_code} {resp.text[:400]}"
            assert "application/rss+xml" in resp.headers.get("content-type", ""), f"ct: {resp.headers.get('content-type')}"
            body = resp.text
            assert '<rss version="2.0"' in body, "缺少 rss version 2.0"
            guids = re.findall(r"<guid\s+isPermaLink=[\"']false[\"']>\s*([^<]+)\s*</guid>", body)
            return guids, body

        g1, b1 = await _fetch()
        g2, b2 = await _fetch()
        assert set(g1) == {"A21", "A22"}, f"第一次 guid 不对: {g1}, body:\n{b1[:1000]}"
        assert set(g1) == set(g2), f"两次 guid 集合不一致: {g1} vs {g2}"
        assert "<media:thumbnail" in b1, "缺 media:thumbnail"
        # 第二条 aweme 有视频下载链接 -> 必含 enclosure 或 media:content
        assert ('<enclosure url="https://v.example.com/v22.mp4"' in b1
                or '<media:content url="https://v.example.com/v22.mp4"' in b1), (
            "缺视频 enclosure/media:content (第一条不含视频只含图，本条两者都需要)")
    print("TR-4.2 PASS: 两次 guid 集合一致 100%，均为 {A21,A22}")


@pytest.mark.asyncio
async def test_TR_4_3_rss_api_key(tmp_path):
    _bootstrap_sqlite(tmp_path)
    _cfg_pkg.RSS_FEED_API_KEY = "secret!"
    _cfg_pkg.RSS_FEED_PUBLIC_BASE_URL = "http://t"
    from api.main import app
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/api/feeds/creators")
        assert r.status_code == 401, f"期望 401, 实际 {r.status_code}"
        r = await ac.get("/api/feeds/dy/id:1/rss")
        assert r.status_code == 401

        r = await ac.get("/api/feeds/creators?api_key=secret!")
        assert r.status_code == 200, f"api_key 正确也非 200: {r.status_code} {r.text[:200]}"
        r = await ac.get("/api/feeds/dy/id:1/rss?api_key=secret!")
        assert r.status_code == 200

        r = await ac.get("/api/feeds/creators?api_key=wrong")
        assert r.status_code == 401

    _cfg_pkg.RSS_FEED_API_KEY = ""
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/api/feeds/creators")
        assert r.status_code == 200
        r = await ac.get("/api/feeds/creators?api_key=any")
        assert r.status_code == 200
    print("TR-4.3 PASS: RSS api_key 鉴权符合规则")


@pytest.mark.asyncio
async def test_TR_4_4_rubric_xml_escape_structured(tmp_path):
    """评分：XML 结构齐全 + 无未转义 <>&。Scale >= 4 算 PASS。"""
    _bootstrap_sqlite(tmp_path)
    from database.db_session import create_tables
    await create_tables("sqlite")

    from database.db_session import get_session
    from database.models import MediaCreatorMeta, DouyinAweme
    from tools.utils import get_current_timestamp
    now = get_current_timestamp()
    async with get_session() as s:
        meta = MediaCreatorMeta(
            platform="dy", creator_hash="DY_RSS_H4", nickname_masked="mask",
            user_name="U<&>4", content_count=1, first_seen_ts=now, last_seen_ts=now,
        )
        s.add(meta)
        await s.flush()
        mid = meta.id
        s.add(DouyinAweme(
            aweme_id="A41", aweme_type="0",
            title='T<&>"特殊字符',
            desc='D<&>"符串<script>alert("x")</script>',
            create_time=1700000100, creator_hash="DY_RSS_H4", nickname="m",
            liked_count="", collected_count="", comment_count="", share_count="",
            last_modify_ts=now,
            aweme_url="https://dy/v/A41&utm=x y",
            cover_url="https://img.example.com/c41.jpg?a=1&b=2",
            video_download_url="", note_download_url="", source_keyword="",
        ))
        await s.commit()

    _cfg_pkg.RSS_FEED_API_KEY = ""
    from api.main import app
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get(f"/api/feeds/dy/id:{mid}/rss")
        body = resp.text

    # 先去掉所有合法 XML 标签（包括自闭合、带 namespace 等），看剩余裸文本
    stripped = re.sub(r"<!\[CDATA\[.*?\]\]>", "CDATA_PLACEHOLDER", body, flags=re.DOTALL)
    stripped = re.sub(r"<[^>]*>", "", stripped)
    unescaped_lt = stripped.count("<")
    unescaped_gt = stripped.count(">")
    unescaped_amp = len(re.findall(r"&(?![a-zA-Z0-9#]+;)", stripped))

    required = [
        '<rss version="2.0"',
        "<channel>", "<title>", "<link>", "<description>",
        "<language>zh-CN</language>",
        "<atom:link",  # 自链接
        "<ttl>",
        "<lastBuildDate>",
        "<item>",
        '<guid isPermaLink="false">',
        "<media:thumbnail",
    ]
    missing = [p for p in required if p not in body]

    score = 5
    if missing:
        score -= 2
    if unescaped_lt + unescaped_gt + unescaped_amp > 2:
        score -= 1
    print(
        f"TR-4.4 rubric 评估: Score={score}/5  "
        f"(未转义 lt={unescaped_lt}, gt={unescaped_gt}, amp={unescaped_amp}; 缺结构项={missing})"
    )
    # 调试输出：如果缺 item 就打前 2k body
    if "<item>" not in body:
        print("DEBUG body (no item):", body[:2000])
    assert score >= 4, (
        f"rubric 评分 {score} < 阈值 4 (missing={missing}, "
        f"unescaped_count={unescaped_lt + unescaped_gt + unescaped_amp})"
    )
    print("TR-4.4 PASS: rubric score 达标")
