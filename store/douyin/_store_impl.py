# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/store/douyin/_store_impl.py
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


# -*- coding: utf-8 -*-
# @Author  : persist1@126.com
# @Time    : 2025/9/5 19:34
# @Desc    : Douyin storage implementation class
import asyncio
import json
import os
import pathlib
from typing import Dict

import httpx
from sqlalchemy import select

import config
from base.base_crawler import AbstractStore
from database.db_session import get_session
from database.models import DouyinAweme, DouyinAwemeComment
from tools import utils, words
from tools.async_file_writer import AsyncFileWriter
from tools.httpx_util import make_async_client
from var import crawler_type_var
from database.mongodb_store_base import MongoDBStoreBase


class DouyinCsvStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(),
            platform="douyin"
        )

    async def store_content(self, content_item: Dict):
        """
        Douyin content CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.file_writer.write_to_csv(
            item=content_item,
            item_type="contents"
        )

    async def store_comment(self, comment_item: Dict):
        """
        Douyin comment CSV storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        await self.file_writer.write_to_csv(
            item=comment_item,
            item_type="comments"
        )

    async def store_creator(self, creator: Dict):
        """
        Douyin creator CSV storage implementation
        Args:
            creator: creator item dict

        Returns:

        """
        await self.file_writer.write_to_csv(
            item=creator,
            item_type="creators"
        )


class DouyinDbStoreImplement(AbstractStore):
    async def store_content(self, content_item: Dict):
        """
        Douyin content DB storage implementation
        Args:
            content_item: content item dict
        """
        aweme_id = content_item.get("aweme_id")
        async with get_session() as session:
            result = await session.execute(select(DouyinAweme).where(DouyinAweme.aweme_id == aweme_id))
            aweme_detail = result.scalar_one_or_none()

            if not aweme_detail:
                content_item["add_ts"] = utils.get_current_timestamp()
                if content_item.get("title"):
                    new_content = DouyinAweme(**content_item)
                    session.add(new_content)
            else:
                for key, value in content_item.items():
                    setattr(aweme_detail, key, value)
            await session.commit()

    async def store_comment(self, comment_item: Dict):
        """
        Douyin comment DB storage implementation
        Args:
            comment_item: comment item dict
        """
        comment_id = comment_item.get("comment_id")
        async with get_session() as session:
            result = await session.execute(select(DouyinAwemeComment).where(DouyinAwemeComment.comment_id == comment_id))
            comment_detail = result.scalar_one_or_none()

            if not comment_detail:
                comment_item["add_ts"] = utils.get_current_timestamp()
                new_comment = DouyinAwemeComment(**comment_item)
                session.add(new_comment)
            else:
                for key, value in comment_item.items():
                    setattr(comment_detail, key, value)
            await session.commit()

    async def store_creator(self, creator: Dict):
        # 教学版：创作者个人资料不再落库
        pass


class DouyinJsonStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(),
            platform="douyin"
        )

    async def store_content(self, content_item: Dict):
        """
        content JSON storage implementation
        Args:
            content_item:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=content_item,
            item_type="contents"
        )

    async def store_comment(self, comment_item: Dict):
        """
        comment JSON storage implementation
        Args:
            comment_item:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=comment_item,
            item_type="comments"
        )

    async def store_creator(self, creator: Dict):
        """
        creator JSON storage implementation
        Args:
            creator:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=creator,
            item_type="creators"
        )



class DouyinJsonlStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(),
            platform="douyin"
        )

    async def store_content(self, content_item: Dict):
        await self.file_writer.write_to_jsonl(
            item=content_item,
            item_type="contents"
        )

    async def store_comment(self, comment_item: Dict):
        await self.file_writer.write_to_jsonl(
            item=comment_item,
            item_type="comments"
        )

    async def store_creator(self, creator: Dict):
        await self.file_writer.write_to_jsonl(
            item=creator,
            item_type="creators"
        )


class DouyinSqliteStoreImplement(DouyinDbStoreImplement):
    pass


class DouyinMongoStoreImplement(AbstractStore):
    """Douyin MongoDB storage implementation"""

    def __init__(self):
        self.mongo_store = MongoDBStoreBase(collection_prefix="douyin")

    async def store_content(self, content_item: Dict):
        """
        Store video content to MongoDB
        Args:
            content_item: Video content data
        """
        aweme_id = content_item.get("aweme_id")
        if not aweme_id:
            return

        await self.mongo_store.save_or_update(
            collection_suffix="contents",
            query={"aweme_id": aweme_id},
            data=content_item
        )
        utils.logger.info(f"[DouyinMongoStoreImplement.store_content] Saved aweme {aweme_id} to MongoDB")

    async def store_comment(self, comment_item: Dict):
        """
        Store comment to MongoDB
        Args:
            comment_item: Comment data
        """
        comment_id = comment_item.get("comment_id")
        if not comment_id:
            return

        await self.mongo_store.save_or_update(
            collection_suffix="comments",
            query={"comment_id": comment_id},
            data=comment_item
        )
        utils.logger.info(f"[DouyinMongoStoreImplement.store_comment] Saved comment {comment_id} to MongoDB")

    async def store_creator(self, creator_item: Dict):
        # 教学版：创作者个人资料不再落库
        pass


class DouyinRestStoreImplement(AbstractStore):
    """Douyin REST storage implementation.

    把抓取数据通过 HTTP POST 推送到第三方聚合服务（PostgreSQL 后端 + RSS feed 输出）。
    本类只实现客户端契约，第三方服务独立开发。
    """

    def __init__(self):
        base_url = getattr(config, "REST_STORE_BASE_URL", "").rstrip("/")
        if not base_url:
            raise ValueError(
                "[DouyinRestStoreImplement] config.REST_STORE_BASE_URL 未配置，无法初始化 REST store"
            )
        self._base_url = base_url
        self._api_key = getattr(config, "REST_STORE_API_KEY", "")
        self._timeout = getattr(config, "REST_STORE_TIMEOUT", 10.0)
        ssl_verify = getattr(config, "REST_STORE_SSL_VERIFY", None)
        client_kwargs = {"timeout": self._timeout}
        if ssl_verify is not None:
            client_kwargs["verify"] = bool(ssl_verify)
        self._client: httpx.AsyncClient = make_async_client(**client_kwargs)
        self._headers = {"Authorization": f"Bearer {self._api_key}"} if self._api_key else {}

    async def _post(self, path: str, body: Dict) -> bool:
        """统一 POST，失败仅 warning 不抛，避免中断爬虫主流程。"""
        url = f"{self._base_url}{path}"
        try:
            resp = await self._client.post(url, json=body, headers=self._headers)
            if 200 <= resp.status_code < 300:
                return True
            utils.logger.warning(
                f"[DouyinRestStoreImplement._post] {url} -> {resp.status_code}: {resp.text[:200]}"
            )
        except httpx.HTTPError as e:
            utils.logger.warning(f"[DouyinRestStoreImplement._post] {url} 请求异常: {e}")
        return False

    async def store_content(self, content_item: Dict):
        aweme_id = content_item.get("aweme_id")
        if not aweme_id:
            utils.logger.warning("[DouyinRestStoreImplement.store_content] 缺少 aweme_id，跳过")
            return
        if await self._post("/api/v1/contents", content_item):
            utils.logger.info(
                f"[DouyinRestStoreImplement.store_content] pushed aweme {aweme_id} "
                f"(creator_hash={content_item.get('creator_hash')})"
            )

    async def store_comment(self, comment_item: Dict):
        comment_id = comment_item.get("comment_id")
        if not comment_id:
            utils.logger.warning("[DouyinRestStoreImplement.store_comment] 缺少 comment_id，跳过")
            return
        if await self._post("/api/v1/comments", comment_item):
            utils.logger.info(f"[DouyinRestStoreImplement.store_comment] pushed comment {comment_id}")

    async def store_creator(self, creator: Dict):
        # 标准爬取流程不触发（store/douyin/__init__.py 的 save_creator 短路）。
        # 实现完整 upsert 供未来/直接调用；服务端亦可从 contents 自动聚合 creator。
        creator_hash = creator.get("creator_hash")
        if not creator_hash:
            utils.logger.warning("[DouyinRestStoreImplement.store_creator] 缺少 creator_hash，跳过")
            return
        if await self._post("/api/v1/creators", creator):
            utils.logger.info(f"[DouyinRestStoreImplement.store_creator] pushed creator {creator_hash}")

    async def close(self):
        """优雅关闭长连接 client（非抽象方法，调用方按需调用）。"""
        await self._client.aclose()


class DouyinExcelStoreImplement:
    """Douyin Excel storage implementation - Global singleton"""

    def __new__(cls, *args, **kwargs):
        from store.excel_store_base import ExcelStoreBase
        return ExcelStoreBase.get_instance(
            platform="douyin",
            crawler_type=crawler_type_var.get()
        )
