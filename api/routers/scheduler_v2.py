# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# 本代码仅用于学习与研究。遵循项目根目录 LICENSE。

"""
Scheduler V2 API Router：
  GET  /api/scheduler/jobs      -> 查看 scheduler 状态、已注册 job、最近一次 run 记录
  POST /api/scheduler/trigger   -> 手动触发一次全平台或指定平台爬取
                                    ?platform=dy&platform=bili   指定
                                    ?platform=all               全部（按 SCHEDULER_PLATFORM_ORDER）
"""
from platform import platform
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from services.scheduler_service_v2 import scheduler_service_v2

router = APIRouter(prefix="/scheduler", tags=["scheduler"])


@router.get("/jobs", summary="获取调度器状态 + 已注册 job 列表 + 最近 run 记录")
async def get_scheduler_status():
    return scheduler_service_v2.describe()

@router.get("/triggerAlljobs")
async def get_scheduler_status():
    # 允许传 "bilibili" 等别名字符串 → 规范化
    try:
        result = await scheduler_service_v2.trigger_manual( None)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result


@router.post("/trigger", summary="手动触发一次爬取（指定平台 / 全部）")
async def trigger_crawl(
    platform: Optional[List[str]] = Query(
        None, description="平台 dy/bili/xhs/zhihu，可多次传。不传或传 all 表示按 SCHEDULER_PLATFORM_ORDER 全部。"
    ),
):
    platforms: List[str]
    if not platform:
        platforms = []   # 空表示全部
    elif "all" in platform:
        platforms = []
    else:
        # 允许传 "bilibili" 等别名字符串 → 规范化
        alias = {
            "bilibili": "bili",
            "douyin": "dy",
            "xiaohongshu": "xhs",
            "xiaohs": "xhs",
            "xhs": "xhs",
            "dy": "dy", "bili": "bili", "zhihu": "zhihu",
            "ks": "ks", "wb": "wb", "tieba": "tieba", "weibo": "wb",
            "kuaishou": "ks",
        }
        normalized = []
        for p in platform:
            key = str(p).lower()
            if key in alias:
                normalized.append(alias[key])
            else:
                normalized.append(key)   # 让 service 自己校验
        platforms = normalized
    try:
        result = await scheduler_service_v2.trigger_manual(platforms=platforms or None)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result
