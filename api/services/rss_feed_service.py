# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/api/services/rss_feed_service.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#
# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

"""
对外 RSS 2.0 Feed 服务。两个能力：
1. list_creators  博主总览 + 分平台统计
2. render_rss_feed(platform, creator_ref)  输出单博主合法 RSS 2.0 XML（Miniflux 可直接订阅）
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from email.utils import formatdate
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from xml.sax.saxutils import escape as _xml_escape

import config as _cfg_runtime
from database.db_session import get_session
from database.models import (
    BilibiliVideo,
    DouyinAweme,
    MediaCrawlerConfig,
    MediaCreatorMeta,
    XhsNote,
    ZhihuContent,
)
from sqlalchemy import func, or_, select

# ==================== 平台内容 ORM -> RSS item 字段映射 ====================
PLATFORM_CONTENT_SCHEMA: Dict[str, Dict[str, Any]] = {
    "dy": {
        "orm": DouyinAweme,
        "col_id": "aweme_id",
        "col_title": "title",
        "col_desc": "desc",
        "col_link": "aweme_url",
        "col_cover": "cover_url",
        "col_media": "video_download_url",
        "col_ts": "create_time",  # Unix 秒级
        "col_hash": "creator_hash",
        "col_nick": "nickname",
    },
    "xhs": {
        "orm": XhsNote,
        "col_id": "note_id",
        "col_title": "title",
        "col_desc": "desc",
        "col_link": "note_url",
        "col_cover": "image_list",  # 逗号分隔，取第一张
        "col_media": "video_url",
        "col_ts": "time",
        "col_hash": "creator_hash",
        "col_nick": "nickname",
    },
    "bili": {
        "orm": BilibiliVideo,
        "col_id": "video_id",
        "col_title": "title",
        "col_desc": "desc",
        "col_link": "video_url",
        "col_cover": "video_cover_url",
        "col_media": None,
        "col_ts": "create_time",
        "col_hash": "creator_hash",
        "col_nick": "nickname",
    },
    "zhihu": {
        "orm": ZhihuContent,
        "col_id": "content_id",
        "col_title": "title",
        "col_desc": "desc",
        "col_link": "content_url",
        "col_cover": None,
        "col_media": None,
        "col_ts_str": "created_time",  # 知乎存的是 VARCHAR，需多格式解析
        "col_hash": "creator_hash",
        "col_nick": "user_nickname",
    },
}

_SUPPORTED_PLATFORMS = set(PLATFORM_CONTENT_SCHEMA.keys())

# ---------------- 辅助 ----------------
def _escape(text: Optional[str]) -> str:
    """RSS XML 所有用户输入必须 escape；None/空转空串。"""
    if not text:
        return ""
    return _xml_escape(str(text))


_ZHIHU_TS_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d",
)


def _parse_zhihu_ts(raw: Optional[str]) -> datetime:
    if not raw:
        return datetime.now(tz=timezone.utc)
    raw = str(raw).strip()
    for fmt in _ZHIHU_TS_FORMATS:
        try:
            dt = datetime.strptime(raw[: len(fmt) + 4], fmt)  # 容忍多余字符
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            continue
    # 纯数字当作 Unix 秒级兜底
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return datetime.now(tz=timezone.utc)


def _ts_to_rfc822(ts_sec: Any) -> str:
    """把各种时间类型 → RFC822（RSS pubDate 需要）。"""
    try:
        if ts_sec is None:
            pass
        elif isinstance(ts_sec, (int, float)) and ts_sec:
            dt = datetime.fromtimestamp(int(ts_sec), tz=timezone.utc)
            return formatdate(int(dt.timestamp()), usegmt=True)
        elif isinstance(ts_sec, datetime):
            return formatdate(int(ts_sec.astimezone(timezone.utc).timestamp()), usegmt=True)
    except (ValueError, OSError, OverflowError):
        pass
    return formatdate(datetime.now(tz=timezone.utc).timestamp(), usegmt=True)


def _parse_ts_to_dt(platform: str, row: Any) -> datetime:
    schema = PLATFORM_CONTENT_SCHEMA[platform]
    if "col_ts_str" in schema:
        raw = getattr(row, schema["col_ts_str"], None)
        return _parse_zhihu_ts(raw if raw is not None else "")
    ts_val = getattr(row, schema.get("col_ts"), None) or 0
    try:
        return datetime.fromtimestamp(int(ts_val), tz=timezone.utc) if ts_val else datetime.now(tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return datetime.now(tz=timezone.utc)


class RssFeedService:
    # ---------------- 对外 API 1：博主列表 + 统计 ----------------
    async def list_creators(
        self,
        platform: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """
        返回 {
          "total": N, "by_platform": {"dy": n, "bili": m, ...},
          "items": [{id, platform, user_name, nickname_masked, creator_hash, config_id,
                     content_count, first_seen_ts, last_seen_ts, feed_url}]
        }
        """
        page_size = max(1, min(page_size, 100))
        page = max(1, page)
        offset = (page - 1) * page_size

        # 非 DB 系存储：直接返回空（不抛异常）
        save_opt = (getattr(_cfg_runtime, "SAVE_DATA_OPTION", "") or "").lower()
        if save_opt not in {"db", "sqlite", "postgres", "mysql"}:
            return {"total": 0, "by_platform": {}, "items": []}

        try:
            async with get_session() as s:
                # COUNT
                q_count = select(func.count(MediaCreatorMeta.id))
                if platform:
                    q_count = q_count.where(MediaCreatorMeta.platform == platform)
                total = (await s.execute(q_count)).scalar_one() or 0

                # by_platform GROUP BY
                q_group = select(
                    MediaCreatorMeta.platform, func.count(MediaCreatorMeta.id)
                ).group_by(MediaCreatorMeta.platform)
                rows = (await s.execute(q_group)).all()
                by_platform = {pf: c for pf, c in rows}

                # 分页
                q = select(MediaCreatorMeta).order_by(
                    MediaCreatorMeta.last_seen_ts.desc()
                )
                if platform:
                    q = q.where(MediaCreatorMeta.platform == platform)
                q = q.offset(offset).limit(page_size)
                meta_rows = (await s.execute(q)).scalars().all()
        except Exception as e:
            # 表未建等异常 → 空返回 + warning
            from tools import utils
            utils.logger.warning(f"[RssFeedService] list_creators 查询异常: {e}")
            return {"total": 0, "by_platform": {}, "items": []}

        base = (getattr(_cfg_runtime, "RSS_FEED_PUBLIC_BASE_URL", "") or "").rstrip("/")
        api_key = getattr(_cfg_runtime, "RSS_FEED_API_KEY", "") or ""

        items: List[Dict[str, Any]] = []
        for r in meta_rows:
            query = {}
            if api_key:
                query["api_key"] = api_key
            qs = ("?" + urlencode(query)) if query else ""
            # 订阅 URL 按 meta.id 订阅（id:<id>）
            feed_url = f"{base}/api/feeds/{r.platform}/id:{r.id}/rss{qs}"
            items.append({
                "id": r.id,
                "platform": r.platform,
                "user_name": r.user_name or r.nickname_masked or f"{r.platform}_creator",
                "nickname_masked": r.nickname_masked,
                "creator_hash": r.creator_hash,
                "config_id": r.config_id,
                "content_count": r.content_count or 0,
                "first_seen_ts": r.first_seen_ts,
                "last_seen_ts": r.last_seen_ts,
                "feed_url": feed_url,
            })
        return {"total": total, "by_platform": by_platform, "items": items}

    # ---------------- 对外 API 2：单博主 RSS 2.0 ----------------
    async def render_rss_feed(self, platform: str, creator_ref: str) -> str:
        platform = (platform or "").strip().lower()
        if platform not in _SUPPORTED_PLATFORMS:
            raise ValueError(f"不支持的 platform: {platform}")
        if not creator_ref:
            raise ValueError("creator_ref 不能为空")
        save_opt = (getattr(_cfg_runtime, "SAVE_DATA_OPTION", "") or "").lower()
        if save_opt not in {"db", "sqlite", "postgres", "mysql"}:
            meta_info, hashes = {}, []
        else:
            meta_info, hashes = await self._resolve_creator_ref(platform, creator_ref)

        if not hashes:
            return self._render_empty_rss(platform, creator_ref, meta_info)

        rows = await self._query_latest_content(platform, hashes,
                                                limit=getattr(_cfg_runtime, "RSS_FEED_MAX_ITEMS", 50) or 50)
        return self._render_rss_xml(platform, creator_ref, meta_info, rows)

    # ---------------- 内部：解析 creator_ref → (meta, hashes list) ----------------
    async def _resolve_creator_ref(self, platform: str, ref: str):
        async with get_session() as s:
            # case 1: 纯数字 → 按 media_creator_meta.id
            if ref.isdigit():
                m = (await s.execute(
                    select(MediaCreatorMeta).where(
                        MediaCreatorMeta.platform == platform,
                        MediaCreatorMeta.id == int(ref),
                    )
                )).scalar_one_or_none()
                if m:
                    return self._meta_to_info(m), [m.creator_hash]
            # case 2: "id:<N>" → 优先按 meta.id 精确匹配（list_creators 输出的 id:{meta.id} 语义）
            #         若无命中，再按 config_id 聚合所有 hash（同一 config 下可能多账号）
            if ref.startswith("id:"):
                try:
                    n = int(ref.split(":", 1)[1])
                except (IndexError, ValueError) as e:
                    raise ValueError(f"非法 id:<id> 格式: {ref}") from e
                # 2a. meta.id 精确
                m = (await s.execute(
                    select(MediaCreatorMeta).where(
                        MediaCreatorMeta.platform == platform,
                        MediaCreatorMeta.id == n,
                    )
                )).scalar_one_or_none()
                if m:
                    return self._meta_to_info(m), [m.creator_hash]
                # 2b. config_id 聚合
                metas = (await s.execute(
                    select(MediaCreatorMeta).where(
                        MediaCreatorMeta.platform == platform,
                        MediaCreatorMeta.config_id == n,
                    )
                )).scalars().all()
                if metas:
                    cfg = (await s.execute(
                        select(MediaCrawlerConfig).where(MediaCrawlerConfig.id == n)
                    )).scalar_one_or_none()
                    info = {
                        "title": (cfg.user_name if cfg else "") or f"{platform} creator config_id={n}",
                        "link":  (cfg.url if cfg else "") or "",
                        "desc":  f"{platform} 创作者内容聚合 ({cfg.user_name if cfg else n})",
                    }
                    return info, [m2.creator_hash for m2 in metas]
                # 2c. 若 meta/config 都无命中但 config 存在（尚未抓取内容场景）
                cfg = (await s.execute(
                    select(MediaCrawlerConfig).where(MediaCrawlerConfig.id == n)
                )).scalar_one_or_none()
                if cfg:
                    info = {"title": cfg.user_name or f"{platform}_{n}", "link": cfg.url,
                            "desc": f"{cfg.platform} 创作者 {cfg.user_name}（尚未抓取到内容）"}
                    return info, []
            # case 3: "hash:<hash>" → 精确按 hash
            if ref.startswith("hash:"):
                h = ref.split(":", 1)[1]
                m = (await s.execute(
                    select(MediaCreatorMeta).where(
                        MediaCreatorMeta.platform == platform,
                        MediaCreatorMeta.creator_hash == h,
                    )
                )).scalar_one_or_none()
                if m:
                    return self._meta_to_info(m), [h]
            # case 4: 按 user_name 精确匹配（兜底）
            m = (await s.execute(
                select(MediaCreatorMeta).where(
                    MediaCreatorMeta.platform == platform,
                    or_(MediaCreatorMeta.user_name == ref, MediaCreatorMeta.nickname_masked == ref),
                )
            )).scalar_one_or_none()
            if m:
                return self._meta_to_info(m), [m.creator_hash]
        return {}, []

    @staticmethod
    def _meta_to_info(m: MediaCreatorMeta) -> Dict[str, str]:
        cfg_runtime = _cfg_runtime
        name = (m.user_name or m.nickname_masked or
                f"{m.platform}_{(m.creator_hash or '')[:8]}")
        link = (m.profile_url or getattr(cfg_runtime, "RSS_FEED_PUBLIC_BASE_URL", "") or "")
        return {
            "title": name,
            "link":  link,
            "desc":  f"{m.platform} 创作者内容聚合（{name}）",
        }

    # ---------------- 内部：按 hash 查内容 ----------------
    async def _query_latest_content(self, platform: str, hashes: List[str], limit: int) -> List[Any]:
        schema = PLATFORM_CONTENT_SCHEMA[platform]
        orm = schema["orm"]
        col_hash = getattr(orm, schema["col_hash"])
        ts_col = schema.get("col_ts")
        if "col_ts_str" in schema:
            order_by_col = getattr(orm, schema["col_ts_str"])
        else:
            order_by_col = getattr(orm, ts_col)
        q = (
            select(orm)
            .where(col_hash.in_(hashes))
            .order_by(order_by_col.desc())
            .limit(limit)
        )
        try:
            async with get_session() as s:
                return (await s.execute(q)).scalars().all()
        except Exception as e:
            from tools import utils
            utils.logger.warning(f"[RssFeedService] query content({platform}) 异常: {e}")
            return []

    # ---------------- 渲染 RSS ----------------
    def _render_empty_rss(self, platform: str, ref: str, meta: Dict[str, str]) -> str:
        title = (meta.get("title") or f"{platform} creator {ref}").strip()
        link = (meta.get("link") or getattr(_cfg_runtime, "RSS_FEED_PUBLIC_BASE_URL", "") or "").strip()
        desc = (meta.get("desc") or f"No content yet for {platform}/{ref}").strip()
        now = formatdate(datetime.now(tz=timezone.utc).timestamp(), usegmt=True)
        ttl = getattr(_cfg_runtime, "RSS_FEED_TTL_MINUTES", 60) or 60
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">\n'
            "  <channel>\n"
            f"    <title>{_escape(title)}</title>\n"
            f"    <link>{_escape(link)}</link>\n"
            f"    <description>{_escape(desc)}</description>\n"
            "    <language>zh-CN</language>\n"
            f"    <lastBuildDate>{now}</lastBuildDate>\n"
            f"    <ttl>{ttl}</ttl>\n"
            "  </channel>\n"
            "</rss>\n"
        )

    def _render_rss_xml(self, platform: str, creator_ref: str,
                        meta_info: Dict[str, str], rows: List[Any]) -> str:
        schema = PLATFORM_CONTENT_SCHEMA[platform]
        base = (getattr(_cfg_runtime, "RSS_FEED_PUBLIC_BASE_URL", "") or "").rstrip("/")
        api_key = getattr(_cfg_runtime, "RSS_FEED_API_KEY", "") or ""
        qs = f"?api_key={api_key}" if api_key else ""
        self_link = f"{base}/api/feeds/{platform}/{_escape(creator_ref)}/rss{qs}"

        title = (meta_info.get("title") or f"{platform} creator feed").strip()
        link = (meta_info.get("link") or base).strip() or base
        desc = (meta_info.get("desc") or title).strip()
        ttl = getattr(_cfg_runtime, "RSS_FEED_TTL_MINUTES", 60) or 60

        items_xml_parts: List[str] = []
        latest_ts = datetime.now(tz=timezone.utc)

        for row in rows:
            item_id = str(getattr(row, schema["col_id"], "") or "")
            item_title = str(getattr(row, schema["col_title"], "") or "")
            item_desc = str(getattr(row, schema["col_desc"], "") or "")
            item_link = str(getattr(row, schema["col_link"], "") or "")

            # 封面 / 视频
            cover_col = schema.get("col_cover")
            media_col = schema.get("col_media")
            cover_url = ""
            if cover_col:
                raw = getattr(row, cover_col) or ""
                if raw:
                    cover_url = str(raw).split(",")[0].strip()
            media_url = (str(getattr(row, media_col)) if media_col and getattr(row, media_col, None) else "") or ""

            pub_dt = _parse_ts_to_dt(platform, row)
            pub_rfc = formatdate(int(pub_dt.timestamp()), usegmt=True)
            if pub_dt > latest_ts:
                latest_ts = pub_dt

            enc: List[str] = []
            if cover_url:
                enc.append(
                    f'      <media:thumbnail url="{_escape(cover_url)}"/>'
                )
                enc.append(
                    f'      <enclosure url="{_escape(cover_url)}" type="image/jpeg" length="0"/>'
                )
            if media_url:
                enc.append(
                    f'      <media:content url="{_escape(media_url)}" type="video/mp4"/>'
                )
                enc.append(
                    f'      <enclosure url="{_escape(media_url)}" type="video/mp4" length="0"/>'
                )
            enclosure_block = "\n".join(enc) if enc else ""

            # title 兜底：取 desc 前 80 字
            display_title = item_title or item_desc[:80] or (f"{platform}_{item_id}")
            items_xml_parts.append(
                "    <item>\n"
                f"      <title>{_escape(display_title[:200])}</title>\n"
                f"      <link>{_escape(item_link)}</link>\n"
                f"      <description>{_escape(item_desc)}</description>\n"
                f"      <pubDate>{pub_rfc}</pubDate>\n"
                f'      <guid isPermaLink="false">{_escape(item_id)}</guid>\n'
                f"      <category>{_escape(platform)}</category>\n"
                + (enclosure_block + "\n" if enclosure_block else "")
                + "    </item>"
            )

        latest_rfc = formatdate(int(latest_ts.timestamp()), usegmt=True)
        channel_header = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:media="http://search.yahoo.com/mrss/">\n'
            "  <channel>\n"
            f"    <title>{_escape(title)}</title>\n"
            f"    <link>{_escape(link)}</link>\n"
            f"    <description>{_escape(desc)}</description>\n"
            "    <language>zh-CN</language>\n"
            f'    <atom:link href="{_escape(self_link)}" rel="self" type="application/rss+xml"/>\n'
            f"    <lastBuildDate>{latest_rfc}</lastBuildDate>\n"
            f"    <pubDate>{latest_rfc}</pubDate>\n"
            f"    <ttl>{ttl}</ttl>\n"
        )
        items_block = "\n".join(items_xml_parts) + ("\n" if items_xml_parts else "")
        footer = "  </channel>\n</rss>\n"
        return channel_header + items_block + footer


rss_feed_service = RssFeedService()
