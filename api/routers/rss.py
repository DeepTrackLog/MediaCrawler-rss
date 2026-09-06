# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/api/routers/rss.py
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
对外 RSS 2.0 订阅接口（Miniflux 直接可用）。
两个端点：
  GET /api/feeds/creators                          博主列表 + 分平台统计
  GET /api/feeds/{platform}/{creator_ref}/rss       单博主 RSS 2.0 XML
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

import config as _cfg_runtime
from ..services.rss_feed_service import rss_feed_service

router = APIRouter(prefix="/feeds", tags=["rss"])


def _check_api_key(api_key: Optional[str]) -> None:
    configured_key = getattr(_cfg_runtime, "RSS_FEED_API_KEY", "") or ""
    if not configured_key:
        return
    if api_key != configured_key:
        raise HTTPException(status_code=401, detail="Invalid api_key")


@router.get("/creators", summary="博主列表 + 分平台统计 (RSS 需求 2)")
async def list_creators(
    platform: Optional[str] = Query(None, description="过滤平台：dy/xhs/bili/zhihu"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数"),
    api_key: Optional[str] = Query(None, description="RSS API Key（在 RSS_FEED_API_KEY 配置了时必传）"),
):
    _check_api_key(api_key)
    if not getattr(_cfg_runtime, "RSS_FEED_ENABLED", True):
        raise HTTPException(status_code=503, detail="RSS feed disabled in config")
    return await rss_feed_service.list_creators(platform=platform, page=page, page_size=page_size)


@router.get("/{platform}/{creator_ref}/rss",
            summary="单博主 RSS 2.0 Feed (RSS 需求 1，Miniflux 直接订阅此 URL)",
            response_class=Response)
async def creator_rss_feed(
    platform: str,
    creator_ref: str,
    api_key: Optional[str] = Query(None, description="RSS API Key"),
):
    _check_api_key(api_key)
    if not getattr(_cfg_runtime, "RSS_FEED_ENABLED", True):
        raise HTTPException(status_code=503, detail="RSS feed disabled in config")
    try:
        xml_body = await rss_feed_service.render_rss_feed(platform, creator_ref)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return Response(
        content=xml_body,
        media_type="application/rss+xml; charset=utf-8",
        headers={"Cache-Control": "public, max-age=300"},
    )
