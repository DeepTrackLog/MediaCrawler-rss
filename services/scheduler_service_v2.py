# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/services/scheduler_service_v2.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#
# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。

"""
SchedulerServiceV2: 基于 apscheduler AsyncIOScheduler 的轻量定时任务。
功能：
  - 每天按 SCHEDULER_DAILY_CRONS 列表（默认 6:00、7:00）按序触发
    [dy → bili → xhs → zhihu] 的 creator 模式爬取（通过 CrawlerManager 子进程）。
  - 平台间隔 SCHEDULER_BETWEEN_PLATFORM_SLEEP_SEC，单平台超时 SCHEDULER_CRAWL_TIMEOUT_HOURS。
  - 支持 GET /api/scheduler/jobs（状态+已注册 job+最近 run 记录）
    POST /api/scheduler/trigger（手动 run 指定平台 / 全部）
不引入 Redis/Celery，所有状态放内存；进程重启由 FastAPI lifespan 重新注册 job。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import config as _cfg_runtime
from tools import utils

logger = utils.logger


# ============= 枚举 & 请求 schema =============
# 这里 import 放在方法内部避免 import 时循环依赖
def _platform_enum(platform: str):
    from api.schemas import PlatformEnum
    for p in PlatformEnum:
        if p.value == platform:
            return p
    raise ValueError(f"不支持的平台: {platform}")


def _login_type_enum(value: str):
    from api.schemas import LoginTypeEnum
    for p in LoginTypeEnum:
        if p.value == value:
            return p
    raise ValueError(f"不支持的 login_type: {value}")


def _crawler_type_enum(value: str):
    from api.schemas import CrawlerTypeEnum
    for p in CrawlerTypeEnum:
        if p.value == value:
            return p
    raise ValueError(value)


def _save_option_enum(value: str):
    from api.schemas import SaveDataOptionEnum
    for p in SaveDataOptionEnum:
        if p.value == value:
            return p
    # 兼容用户把 SAVE_DATA_OPTION 配成 db/postgres/mysql 等，但 scheduler 默认 save_option 配 jsonl
    logger.warning(f"[SchedulerV2] save_option 不在枚举内: {value}，回退 jsonl")
    from api.schemas import SaveDataOptionEnum as E
    return E.JSONL


@dataclass
class PipelineRecord:
    run_id: str
    started_at: str            # ISO
    finished_at: str = ""
    status: str = "running"    # running/success/failed/partial/timeout
    trigger: str = "manual"    # cron / manual
    platforms: List[str] = field(default_factory=list)
    per_platform: List[Dict[str, Any]] = field(default_factory=list)
    error: str = ""


# ============= cron 5 段解析 =============
def _parse_cron(expr: str) -> Tuple[str, str, str, str, str]:
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"CRON 表达式必须 5 段 (minute hour day month dow): {expr}")
    return parts[0], parts[1], parts[2], parts[3], parts[4]


# ============= 服务主体 =============
class SchedulerServiceV2:
    def __init__(self) -> None:
        self._scheduler = None       # apscheduler AsyncIOScheduler
        self._started = False
        self._pipeline_lock = asyncio.Lock()
        self._records: List[PipelineRecord] = []   # 最新的在前
        self._max_records = 20

    # ---------------- Lifecycle (FastAPI lifespan) ----------------
    async def start(self) -> None:
        """启动 scheduler。SCHEDULER_ENABLED=False 时仅保留手动触发能力。"""
        if self._started:
            return
        enabled = bool(getattr(_cfg_runtime, "SCHEDULER_ENABLED", True))
        cron_exprs = list(getattr(_cfg_runtime, "SCHEDULER_DAILY_CRONS", []) or [])
        if enabled and cron_exprs:
            try:
                from apscheduler.schedulers.asyncio import AsyncIOScheduler
                from apscheduler.triggers.cron import CronTrigger
            except Exception as e:  # pragma: no cover
                logger.warning(f"[SchedulerV2] apscheduler 未安装/异常，跳过定时任务注册: {e}")
                self._started = True
                return

            # 兼容时区：默认 Asia/Shanghai；若系统不可用则退 None（local tz）
            tz = None
            try:
                import zoneinfo  # type: ignore
                tz = zoneinfo.ZoneInfo("Asia/Shanghai")
            except Exception:
                tz = None

            self._scheduler = AsyncIOScheduler(
                jobstores={},  # memory default
                timezone=tz,
            )
            for idx, expr in enumerate(cron_exprs):
                try:
                    minute, hour, day, month, dow = _parse_cron(expr)
                    trig = CronTrigger(
                        minute=minute, hour=hour, day=day, month=month, day_of_week=dow,
                        timezone=tz,
                    )
                    self._scheduler.add_job(
                        self._cron_entry,
                        trigger=trig,
                        id=f"daily_crawl_{idx}",
                        name=f"MediaCrawler daily @ {expr}",
                        kwargs={"trigger": "cron", "cron_expr": expr},
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=int(getattr(_cfg_runtime,
                                                      "SCHEDULER_MISFIRE_GRACE_SEC",
                                                      3600)),
                    )
                    logger.info(f"[SchedulerV2] registered job daily_crawl_{idx}: {expr} (tz={tz})")
                except Exception as e:
                    logger.error(f"[SchedulerV2] 注册 cron 失败 expr={expr}: {e}")
            self._scheduler.start()
        else:
            logger.info(f"[SchedulerV2] skipped (enabled={enabled}, crons={len(cron_exprs)})")
        self._started = True

    async def shutdown(self) -> None:
        if self._scheduler is not None:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception as e:
                logger.warning(f"[SchedulerV2] shutdown err: {e}")
            self._scheduler = None
        self._started = False

    # ---------------- Scheduler Job Entry ----------------
    async def _cron_entry(self, trigger: str = "cron", cron_expr: str = "") -> None:
        logger.info(f"[SchedulerV2] cron fired: trigger={trigger} cron={cron_expr}")
        platforms = list(getattr(_cfg_runtime, "SCHEDULER_PLATFORM_ORDER", []) or [])
        await self._run_pipeline(platforms=platforms, trigger=trigger, reason=cron_expr)

    # ---------------- Public API ----------------
    async def trigger_manual(self, platforms: Optional[List[str]] = None) -> Dict[str, Any]:
        order = list(getattr(_cfg_runtime, "SCHEDULER_PLATFORM_ORDER", []) or [])
        if not platforms:
            plats = order
        else:
            plats = [p for p in platforms if p in order]
            extra = [p for p in platforms if p not in order]
            if extra:
                plats.extend(extra)  # 允许临时指定非默认顺序的平台也可运行
        if not plats:
            raise ValueError("没有可执行的平台")
        return await self._run_pipeline(plats, trigger="manual", reason="http_trigger")

    def describe(self) -> Dict[str, Any]:
        jobs = []
        if self._scheduler is not None:
            try:
                for j in self._scheduler.get_jobs():
                    jobs.append({
                        "id": j.id,
                        "name": j.name,
                        "next_run_time": j.next_run_time.isoformat() if j.next_run_time else None,
                        "trigger": str(j.trigger),
                    })
            except Exception as e:
                jobs = [{"error": str(e)}]
        return {
            "enabled": bool(getattr(_cfg_runtime, "SCHEDULER_ENABLED", True)),
            "scheduler_started": self._started,
            "pipeline_running": self._pipeline_lock.locked(),
            "cron_exprs": list(getattr(_cfg_runtime, "SCHEDULER_DAILY_CRONS", []) or []),
            "platform_order": list(getattr(_cfg_runtime, "SCHEDULER_PLATFORM_ORDER", []) or []),
            "between_platform_sleep_sec": int(getattr(_cfg_runtime, "SCHEDULER_BETWEEN_PLATFORM_SLEEP_SEC", 300) or 300),
            "crawl_timeout_hours": int(getattr(_cfg_runtime, "SCHEDULER_CRAWL_TIMEOUT_HOURS", 5) or 5),
            "jobs": jobs,
            "records": [asdict(r) for r in self._records[: self._max_records]],
        }

    # ---------------- Core Pipeline ----------------
    async def _run_pipeline(self, platforms: List[str], trigger: str, reason: str) -> Dict[str, Any]:
        if self._pipeline_lock.locked():
            return {"ok": False, "skipped": True, "reason": "pipeline already running"}
        async with self._pipeline_lock:
            import uuid
            rid = uuid.uuid4().hex[:10]
            rec = PipelineRecord(
                run_id=rid,
                started_at=datetime.now().isoformat(timespec="seconds"),
                trigger=trigger,
                platforms=list(platforms),
            )
            self._records.insert(0, rec)
            if len(self._records) > self._max_records:
                self._records.pop()
            try:
                results: List[Dict[str, Any]] = []
                timeout_h = int(getattr(_cfg_runtime, "SCHEDULER_CRAWL_TIMEOUT_HOURS", 5) or 5)
                sleep_s = int(getattr(_cfg_runtime, "SCHEDULER_BETWEEN_PLATFORM_SLEEP_SEC", 300) or 300)
                for i, plat in enumerate(platforms):
                    logger.info(f"[SchedulerV2][{rid}] start platform={plat} ({i+1}/{len(platforms)})")
                    one = await self._run_one_platform(plat, timeout_h=timeout_h)
                    results.append(one)
                    logger.info(f"[SchedulerV2][{rid}] platform={plat} done -> {one}")
                    if i != len(platforms) - 1 and sleep_s > 0:
                        logger.info(f"[SchedulerV2][{rid}] sleep {sleep_s}s before next platform")
                        await asyncio.sleep(sleep_s)
                rec.per_platform = results
                failed_count = sum(1 for r in results if not r.get("ok"))
                timeout_count = sum(1 for r in results if r.get("status") == "timeout_killed")
                if timeout_count:
                    rec.status = "timeout"
                elif failed_count == 0:
                    rec.status = "success"
                elif failed_count == len(results):
                    rec.status = "failed"
                else:
                    rec.status = "partial"
                rec.finished_at = datetime.now().isoformat(timespec="seconds")
            except Exception as e:
                utils.logger.info(
                    f"抛出异常：  {e} "
                )
                import traceback
                logger.exception(f"[SchedulerV2][{rid}] pipeline error: {e}")
                rec.status = "failed"
                rec.error = f"{e.__class__.__name__}: {e}"
                rec.finished_at = datetime.now().isoformat(timespec="seconds")
            return {"ok": rec.status in ("success", "partial"), "record": asdict(rec)}

    async def _run_one_platform(self, platform: str, timeout_h: int) -> Dict[str, Any]:
        """运行单平台：构造请求 → 通过 CrawlerManager 启动 subprocess → 等待结束/超时 kill。"""
        from api.services.crawler_manager import CrawlerManager
        from api.schemas import CrawlerStartRequest
        cm = CrawlerManager()
        defaults = dict(getattr(_cfg_runtime, "SCHEDULER_CRAWLER_DEFAULTS", {}) or {})

        # 若当前配置项 SAVE_DATA_OPTION 是 sqlite/db/postgres/mysql，则 scheduler 的 save_option 也同步过去
        save_opt_cfg = (getattr(_cfg_runtime, "SAVE_DATA_OPTION", "") or "").lower()
        if save_opt_cfg in {"sqlite", "db", "postgres", "mysql"}:
            # 若枚举没这个 literal（DB 对应 "db"），就转：mysql/postgres → db
            save_val = "db" if save_opt_cfg in {"postgres", "mysql"} else save_opt_cfg
        else:
            save_val = str(defaults.get("save_option") or "jsonl")

        request = CrawlerStartRequest(
            platform=_platform_enum(platform),
            login_type=_login_type_enum(str(defaults.get("login_type") or "cookie")),
            crawler_type=_crawler_type_enum(str(defaults.get("crawler_type") or "creator")),
            save_option=_save_option_enum(save_val),
            headless=bool(defaults.get("headless", False)),
            enable_comments=bool(defaults.get("enable_comments", True)),
            enable_sub_comments=bool(defaults.get("enable_sub_comments", False)),
            max_notes_count=defaults.get("max_notes_count"),
            max_comments_count=defaults.get("max_comments_count"),
            # creator_ids 留空：main.py 启动后会自行从 DB 加载对应平台的 creator 列表（creator_config_db_provider.load_and_apply）
            creator_ids="",
            keywords="",
            specified_ids="",
            start_page=1,
            cookies="",
        )
        started = await cm.start(request)
        if not started:
            return {"platform": platform, "ok": False, "status": "start_failed",
                    "reason": "CrawlerManager.start returned False (另一个实例可能在运行)"}

        timeout_sec = max(1, timeout_h) * 3600
        deadline = time.monotonic() + timeout_sec
        out = {"platform": platform, "ok": False, "status": "unknown", "exit_code": None,
               "timeout_h": timeout_h}
        try:
            while True:
                if cm.process is None:
                    out["status"] = "no_process"
                    break
                rc = cm.process.poll()
                if rc is not None:
                    out["exit_code"] = rc
                    out["ok"] = (rc == 0)
                    out["status"] = "ok" if rc == 0 else f"exit_{rc}"
                    break
                if time.monotonic() > deadline:
                    out["status"] = "timeout_killed"
                    out["ok"] = False
                    logger.warning(f"[SchedulerV2] platform={platform} 超时 {timeout_h}h，尝试 stop")
                    try:
                        stopped = await cm.stop()
                        out["stop_result"] = stopped
                    except Exception as e2:
                        logger.warning(f"[SchedulerV2] stop err: {e2}")
                        try:
                            cm.process.kill()
                        except Exception:
                            pass
                    break
                await asyncio.sleep(3)
        except Exception as e:
            out["status"] = "exception"
            out["error"] = f"{e.__class__.__name__}: {e}"
        finally:
            # 收尾：等待 read_task，还活着的 process kill
            if cm.process is not None and cm.process.poll() is None:
                try:
                    cm.process.kill()
                except Exception:
                    pass
            if cm._read_task is not None and not cm._read_task.done():
                try:
                    await asyncio.wait_for(cm._read_task, timeout=10)
                except Exception:
                    pass
        return out


scheduler_service_v2 = SchedulerServiceV2()
