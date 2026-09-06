# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/services/creator_meta_service.py
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
CreatorMetaService — 抓取内容时自动 upsert 创作者 hash ↔ 人可读信息的映射表。
供 RSS Feed 接口按"博主"聚合使用。
任何异常只打 warning 日志，绝不阻断主爬取流程（遵循 NFR-4）。
"""

from typing import Optional, Tuple

import config as _cfg_runtime
from database.db_session import get_session
from database.models import MediaCreatorMeta, MediaCrawlerConfig
from sqlalchemy import select
from tools import utils


class CreatorMetaService:

    async def upsert(
        self,
        platform: str,
        creator_hash: str,
        nickname_masked: str = "",
        user_name: str = "",
        config_id: Optional[int] = None,
        profile_url: Optional[str] = None,
    ) -> None:
        """
        将单条内容的创作者信息写入/更新 media_creator_meta。
        - creator_hash 为空直接忽略（protect DB 约束）。
        - 若 platform+hash 不存在 → insert，content_count=1。
        - 若已存在 → 补全空字段（nickname_masked / user_name / config_id / profile_url）
          → content_count += 1 → last_seen_ts = now。
        """
        if not creator_hash or not platform:
            return

        # 非 DB 存储模式：不做操作（表可能不存在，直接 return，避免 SQL 异常）
        save_opt = (getattr(_cfg_runtime, "SAVE_DATA_OPTION", "") or "").lower()
        if save_opt not in {"db", "sqlite", "postgres", "mysql"}:
            return

        try:
            now = utils.get_current_timestamp()
            async with get_session() as s:
                stmt = select(MediaCreatorMeta).where(
                    MediaCreatorMeta.platform == platform,
                    MediaCreatorMeta.creator_hash == creator_hash,
                )
                obj = (await s.execute(stmt)).scalar_one_or_none()
                if not obj:
                    s.add(MediaCreatorMeta(
                        platform=platform,
                        creator_hash=creator_hash,
                        nickname_masked=(nickname_masked or "")[:255],
                        user_name=(user_name or "")[:255],
                        config_id=config_id,
                        profile_url=profile_url,
                        content_count=1,
                        first_seen_ts=now,
                        last_seen_ts=now,
                    ))
                else:
                    # 补填缺失的信息（只填尚未有值的字段）
                    if not obj.nickname_masked and nickname_masked:
                        obj.nickname_masked = (nickname_masked or "")[:255]
                    if not obj.user_name and user_name:
                        obj.user_name = (user_name or "")[:255]
                    if obj.config_id is None and config_id is not None:
                        obj.config_id = config_id
                    if not obj.profile_url and profile_url:
                        obj.profile_url = profile_url
                    obj.content_count = (obj.content_count or 0) + 1
                    obj.last_seen_ts = now
        except Exception as e:  # pragma: no cover - 仅降级
            utils.logger.warning(
                f"[CreatorMetaService] upsert 失败({platform}/{creator_hash[:12]}...): {e}"
            )

    # ---------- 辅助：运行时按 URL/ID 反查 user_name + config_id ----------
    async def lookup_config_info(self, platform: str, url_or_id: str) -> Tuple[str, Optional[int]]:
        """
        根据当前爬虫使用的 URL/ID 反查 media_crawler_config 中的 user_name + config_id。
        查找优先顺序：
          ① DB media_crawler_config WHERE platform=? AND url=? (enabled=1)
          ② 运行时 config.*_CREATOR_ID_LIST_new (dict 数组) 按 url 匹配 (仅 dy)
          ③ 运行时 config.*_CREATOR_ID_LIST / *_CREATOR_URL_LIST 按 index 反查
        返回 (user_name, config_id_or_None)
        """
        if not url_or_id:
            return "", None

        # ① 查 DB
        save_opt = (getattr(_cfg_runtime, "SAVE_DATA_OPTION", "") or "").lower()
        if save_opt in {"db", "sqlite", "postgres", "mysql"}:
            try:
                async with get_session() as s:
                    stmt = select(MediaCrawlerConfig).where(
                        MediaCrawlerConfig.platform == platform,
                        MediaCrawlerConfig.url == url_or_id,
                        MediaCrawlerConfig.enabled == 1,
                    ).limit(1)
                    cfg = (await s.execute(stmt)).scalar_one_or_none()
                    if cfg:
                        return (cfg.user_name or ""), cfg.id
            except Exception as e:
                utils.logger.warning(
                    f"[CreatorMetaService] lookup DB 配置失败({platform}): {e}"
                )

        # ② 抖音新结构（dict 数组）
        if platform == "dy":
            new_list = getattr(_cfg_runtime, "DY_CREATOR_ID_LIST_new", None) or []
            if isinstance(new_list, list):
                for d in new_list:
                    if isinstance(d, dict) and d.get("url") == url_or_id:
                        return (d.get("user_name") or ""), None

        # ③ 纯 URL 数组（所有平台兜底）
        urls = None
        if platform == "dy":
            urls = getattr(_cfg_runtime, "DY_CREATOR_ID_LIST", None) or []
        elif platform == "xhs":
            urls = getattr(_cfg_runtime, "XHS_CREATOR_ID_LIST", None) or []
        elif platform == "bili":
            urls = getattr(_cfg_runtime, "BILI_CREATOR_ID_LIST", None) or []
        elif platform == "zhihu":
            urls = getattr(_cfg_runtime, "ZHIHU_CREATOR_URL_LIST", None) or []
        elif platform == "wb":
            urls = getattr(_cfg_runtime, "WEIBO_CREATOR_ID_LIST", None) or []
        elif platform == "ks":
            urls = getattr(_cfg_runtime, "KS_CREATOR_ID_LIST", None) or []
        elif platform == "tieba":
            urls = getattr(_cfg_runtime, "TIEBA_CREATOR_URL_LIST", None) or []
        if isinstance(urls, list) and url_or_id in urls:
            i = urls.index(url_or_id)
            return f"{platform}_creator_{i}", None
        return "", None


creator_meta_service = CreatorMetaService()
