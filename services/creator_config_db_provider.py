# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/services/creator_config_db_provider.py
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
从 DB media_crawler_config 表读取目标创作者名单，并覆盖运行时 config.*_CREATOR_ID_LIST*。
当 SAVE_DATA_OPTION 非 DB 系 / DB 表为空 / 读取失败时，自动降级为硬编码原值，100% 兼容旧行为。
"""

from typing import Optional, Dict, List

# 注意：所有"全局运行时配置"统一通过 `import config`（顶层包）访问，
# 不要用 `from config import base_config as X` 拿单独的模块对象，
# 否则外部（CLI / main / crawler_manager）改写运行时值时这里读不到。
import config as _cfg_runtime
from database.db_session import get_session
from database.models import MediaCrawlerConfig
from sqlalchemy import select
from tools import utils


class CreatorConfigDbProvider:
    """
    统一的创作者目标配置源。
    优先级：media_crawler_config(DB) -> 硬编码 *_CREATOR_ID_LIST*
    """

    _DB_SAVE_OPTIONS = {"db", "sqlite", "postgres", "mysql"}

    def _use_db(self) -> bool:
        """只有 SAVE_DATA_OPTION 属于 DB 系时才查库，避免无谓 SQL。"""
        opt = (getattr(_cfg_runtime, "SAVE_DATA_OPTION", "") or "").lower()
        return opt in self._DB_SAVE_OPTIONS

    async def load_and_apply(self, platform: Optional[str] = None) -> Dict[str, List[Dict]]:
        """
        加载 DB 中的创作者目标配置，按 platform 回填到运行时 config.*_LIST。
        返回 {platform: [{"id": int, "user_name": str, "url": str, "sort_order": int, "tags": str}]}。
        platform=None 时一次性回填所有支持的平台。
        """
        targets = [platform] if platform else ["dy", "xhs", "bili", "zhihu", "wb", "ks", "tieba"]
        result: Dict[str, List[Dict]] = {}
        for pf in targets:
            if self._use_db():
                rows = await self._query_enabled_rows(pf)
            else:
                rows = []

            if not rows:
                # 空表或非 DB 模式：保留硬编码（把硬编码回读成统一结构，返回给上层便于统一用返回值）
                result[pf] = self._from_hardcoded(pf)
                count = len(result[pf])
                if count:
                    utils.logger.info(
                        f"[CreatorConfigDbProvider] 平台 {pf} DB 无配置，保留硬编码 {count} 条。"
                    )
                else:
                    utils.logger.info(
                        f"[CreatorConfigDbProvider] 平台 {pf} DB 无配置且硬编码为空，列表将为空。"
                    )
                continue

            # DB 有配置 -> 覆盖运行时 config
            urls = [r["url"] for r in rows]
            dicts = [{"user_name": r["user_name"], "url": r["url"]} for r in rows]
            self._apply_to_config(pf, urls, dicts)
            result[pf] = rows
            utils.logger.info(
                f"[CreatorConfigDbProvider] 平台 {pf} 从 DB 加载 {len(rows)} 位创作者（sort_order 排序）。"
            )
        return result

    async def _query_enabled_rows(self, platform: str) -> List[Dict]:
        """按 platform + enabled=1 查询，sort_order 升序。任意异常吞掉并 warning，不中断上层。"""
        try:
            async with get_session() as session:
                stmt = select(MediaCrawlerConfig).where(
                    MediaCrawlerConfig.platform == platform,
                    MediaCrawlerConfig.enabled == 1,
                ).order_by(
                    MediaCrawlerConfig.sort_order.asc(),
                    MediaCrawlerConfig.id.asc(),
                )
                objs = (await session.execute(stmt)).scalars().all()
                return [
                    {
                        "id": o.id,
                        "user_name": (o.user_name or "").strip(),
                        "url": (o.url or "").strip(),
                        "sort_order": o.sort_order or 0,
                        "tags": o.tags or "",
                    }
                    for o in objs
                    if (o.url or "").strip()
                ]
        except Exception as e:  # pragma: no cover - 异常分支，只记录日志
            utils.logger.warning(
                f"[CreatorConfigDbProvider] 查询 DB 配置失败（平台 {platform}）: {e}。降级硬编码。"
            )
            return []

    def _from_hardcoded(self, platform: str) -> List[Dict]:
        """从各平台 config.*_CREATOR_ID_LIST* 硬编码中读回统一结构。"""
        out: List[Dict] = []
        if platform == "dy":
            # 优先 DY_CREATOR_ID_LIST_new（字典数组），没有再回退 DY_CREATOR_ID_LIST（纯 URL 数组）
            new_list = getattr(_cfg_runtime, "DY_CREATOR_ID_LIST_new", None) or []
            if isinstance(new_list, list) and new_list:
                for i, d in enumerate(new_list):
                    url = ""
                    name = f"dy_creator_{i}"
                    if isinstance(d, dict):
                        url = d.get("url", "") or ""
                        name = d.get("user_name") or name
                    else:
                        url = str(d)
                    if url:
                        out.append({"id": i, "user_name": name, "url": url, "sort_order": i, "tags": ""})
            else:
                urls = getattr(_cfg_runtime, "DY_CREATOR_ID_LIST", None) or []
                for i, u in enumerate(urls):
                    if u:
                        out.append({"id": i, "user_name": f"dy_creator_{i}", "url": u, "sort_order": i, "tags": ""})
        elif platform == "xhs":
            urls = getattr(_cfg_runtime, "XHS_CREATOR_ID_LIST", None) or []
            for i, u in enumerate(urls):
                if u:
                    out.append({"id": i, "user_name": f"xhs_creator_{i}", "url": u, "sort_order": i, "tags": ""})
        elif platform == "bili":
            urls = getattr(_cfg_runtime, "BILI_CREATOR_ID_LIST", None) or []
            for i, u in enumerate(urls):
                if u:
                    out.append({"id": i, "user_name": f"bili_creator_{i}", "url": u, "sort_order": i, "tags": ""})
        elif platform == "zhihu":
            # 知乎变量名是 ZHIHU_CREATOR_URL_LIST
            urls = getattr(_cfg_runtime, "ZHIHU_CREATOR_URL_LIST", None) or []
            for i, u in enumerate(urls):
                if u:
                    out.append({"id": i, "user_name": f"zhihu_creator_{i}", "url": u, "sort_order": i, "tags": ""})
        elif platform == "wb":
            urls = getattr(_cfg_runtime, "WEIBO_CREATOR_ID_LIST", None) or []
            for i, u in enumerate(urls):
                if u:
                    out.append({"id": i, "user_name": f"wb_creator_{i}", "url": u, "sort_order": i, "tags": ""})
        elif platform == "ks":
            urls = getattr(_cfg_runtime, "KS_CREATOR_ID_LIST", None) or []
            for i, u in enumerate(urls):
                if u:
                    out.append({"id": i, "user_name": f"ks_creator_{i}", "url": u, "sort_order": i, "tags": ""})
        elif platform == "tieba":
            urls = getattr(_cfg_runtime, "TIEBA_CREATOR_URL_LIST", None) or []
            for i, u in enumerate(urls):
                if u:
                    out.append({"id": i, "user_name": f"tieba_creator_{i}", "url": u, "sort_order": i, "tags": ""})
        return out

    @staticmethod
    def _apply_to_config(platform: str, urls: List[str], dicts: List[Dict]) -> None:
        """把 DB 结果写入运行时 config.*_LIST 全局变量，保持与 CLI 参数解析相同的链路。"""
        if platform == "dy":
            _cfg_runtime.DY_CREATOR_ID_LIST = list(urls)
            if hasattr(_cfg_runtime, "DY_CREATOR_ID_LIST_new"):
                _cfg_runtime.DY_CREATOR_ID_LIST_new = [
                    {"user_name": d["user_name"], "url": d["url"]} for d in dicts
                ]
        elif platform == "xhs":
            _cfg_runtime.XHS_CREATOR_ID_LIST = list(urls)
        elif platform == "bili":
            _cfg_runtime.BILI_CREATOR_ID_LIST = list(urls)
        elif platform == "zhihu":
            _cfg_runtime.ZHIHU_CREATOR_URL_LIST = list(urls)
        elif platform == "wb":
            if hasattr(_cfg_runtime, "WEIBO_CREATOR_ID_LIST"):
                _cfg_runtime.WEIBO_CREATOR_ID_LIST = list(urls)
        elif platform == "ks":
            if hasattr(_cfg_runtime, "KS_CREATOR_ID_LIST"):
                _cfg_runtime.KS_CREATOR_ID_LIST = list(urls)
        elif platform == "tieba":
            if hasattr(_cfg_runtime, "TIEBA_CREATOR_URL_LIST"):
                _cfg_runtime.TIEBA_CREATOR_URL_LIST = list(urls)


# 全局单例
creator_config_db_provider = CreatorConfigDbProvider()
