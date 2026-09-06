# -*- coding: utf-8 -*-
"""
验证 CreatorConfigDbProvider 三种场景：
TR-2.1 DB 有配置时正确覆盖 DY_CREATOR_ID_LIST_new
TR-2.2 DB 空配置时保持硬编码不报错
TR-2.3 SAVE_DATA_OPTION='jsonl'（非 DB 模式）时不查库、保持硬编码
"""

import asyncio
import sys
import os
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import config as _cfg_package
from tools.utils import get_current_timestamp


@pytest.fixture(scope="module")
def event_loop():
    """pytest-asyncio 需要的事件循环 fixture"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def _bootstrap_sqlite(tmpdir: Path):
    """用临时 DB 文件，改全局配置指向 sqlite。重置 db_session 单例以便重新建引擎。"""
    db_path = str(tmpdir / "test_creator_config.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    _cfg_package.SAVE_DATA_OPTION = "sqlite"
    from config import db_config as _dbcfg
    # 覆盖 db_config：SQLITE_DB_PATH 常量 + sqlite_db_config dict（db_session.create_tables 实际读这个 dict['db_path']）
    _dbcfg.SQLITE_DB_PATH = db_path
    _dbcfg.sqlite_db_config = {"db_path": db_path}
    # 重置 db_session 内的单例：重点是清空 _engines dict（get_async_engine 从这里缓存）
    from database import db_session as _dbs
    _dbs._engines.clear()
    # 多余字段兜底
    for attr_name in ("_engine", "_async_session_factory"):
        try:
            setattr(_dbs, attr_name, None)
        except Exception:
            pass
    return db_path


@pytest.mark.asyncio
async def test_TR_2_1_db_config_overrides_hardcoded(tmp_path):
    """DB 插入两条 dy 记录后，config.DY_CREATOR_ID_LIST_new 被覆盖为 DB 值（按 sort_order 排序）。"""
    _bootstrap_sqlite(tmp_path)

    # 建表
    from database.db_session import create_tables
    await create_tables("sqlite")

    # 手插两条（id 1 sort_order=1，id 2 sort_order=0，要验证按 sort_order 排序）
    from database.db_session import get_session
    from database.models import MediaCrawlerConfig
    from sqlalchemy import text
    ts = get_current_timestamp()
    async with get_session() as s:
        # 用 raw SQL 保证 created_at/updated_at 填好
        await s.execute(
            text("""INSERT INTO media_crawler_config
                    (id, platform, user_name, url, enabled, sort_order, tags, remark, created_at, updated_at)
                    VALUES (:id, :pf, :un, :url, 1, :so, '', '', :ts, :ts)"""),
            [
                {"id": 11, "pf": "dy", "un": "USER_B", "url": "FAKE_B", "so": 1, "ts": ts},
                {"id": 10, "pf": "dy", "un": "USER_A", "url": "FAKE_A", "so": 0, "ts": ts},
            ]
        )
        await s.commit()

    # 待测：加载 + 应用
    from services.creator_config_db_provider import CreatorConfigDbProvider
    pvd = CreatorConfigDbProvider()
    res = await pvd.load_and_apply("dy")

    # 断言返回结构
    dy_rows = res.get("dy", [])
    assert len(dy_rows) == 2, f"期望 2 条，实际 {len(dy_rows)}: {dy_rows}"
    # sort_order 小的在前（FAKE_A sort_order=0 在前）
    assert dy_rows[0]["url"] == "FAKE_A", f"排序错误，第一个应为 FAKE_A，实际 {dy_rows[0]}"
    assert dy_rows[0]["user_name"] == "USER_A"
    assert dy_rows[1]["url"] == "FAKE_B"
    assert dy_rows[1]["user_name"] == "USER_B"

    # 断言硬编码全局已覆盖
    assert isinstance(_cfg_package.DY_CREATOR_ID_LIST, list)
    assert len(_cfg_package.DY_CREATOR_ID_LIST) == 2
    assert _cfg_package.DY_CREATOR_ID_LIST[0] == "FAKE_A"
    assert _cfg_package.DY_CREATOR_ID_LIST[1] == "FAKE_B"

    new_list = _cfg_package.DY_CREATOR_ID_LIST_new
    assert len(new_list) == 2
    assert new_list[0] == {"user_name": "USER_A", "url": "FAKE_A"}
    assert new_list[1] == {"user_name": "USER_B", "url": "FAKE_B"}
    print("TR-2.1 PASS")


@pytest.mark.asyncio
async def test_TR_2_2_empty_db_falls_back_to_hardcoded(tmp_path):
    """DB 表为空（刚建表但没插数据）时，直接返回硬编码列表（不抛异常、保持与原始长度一致）。"""
    _bootstrap_sqlite(tmp_path)
    from database.db_session import create_tables
    await create_tables("sqlite")

    # 先保存原始长度
    orig_dy_new = list(_cfg_package.DY_CREATOR_ID_LIST_new or [])
    orig_len = len(orig_dy_new)

    from services.creator_config_db_provider import CreatorConfigDbProvider
    pvd = CreatorConfigDbProvider()
    res = await pvd.load_and_apply("dy")

    # 空表时，结果中 dy 应为硬编码（长度与原始相同）
    assert "dy" in res
    assert len(res["dy"]) == orig_len or orig_len == 0, (
        f"空表时应保持硬编码，原始 {orig_len} / 当前 {len(res['dy'])}"
    )
    # 全局未被覆盖成空
    assert len(_cfg_package.DY_CREATOR_ID_LIST_new or []) == orig_len
    print("TR-2.2 PASS")


@pytest.mark.asyncio
async def test_TR_2_3_non_db_mode_no_sql_access(tmp_path):
    """SAVE_DATA_OPTION='jsonl' 时不查 DB，直接使用硬编码，不抛异常。"""
    # 切到非 DB 模式（不改 db_config，保持 sqlite 库存在即可）
    _cfg_package.SAVE_DATA_OPTION = "jsonl"
    # 把 _from_hardcoded 结果提前记住（通过新建 provider 验证是否一致）
    from services.creator_config_db_provider import CreatorConfigDbProvider
    pvd = CreatorConfigDbProvider()
    hardcoded_backup = pvd._from_hardcoded("xhs")

    # 在未开 DB 模式下真实调用 load_and_apply
    res = await pvd.load_and_apply("xhs")
    # 结果应等于 hardcoded
    assert len(res.get("xhs", [])) == len(hardcoded_backup)
    # 全局 XHS_CREATOR_ID_LIST 未被清空（等于硬编码值）
    assert _cfg_package.XHS_CREATOR_ID_LIST == [r["url"] for r in hardcoded_backup] or (
        len(hardcoded_backup) == 0 and len(_cfg_package.XHS_CREATOR_ID_LIST) == 0
    ), "非 DB 模式下不应改动 XHS 列表"

    # _use_db() 返回 False
    assert pvd._use_db() is False, "jsonl 模式下 _use_db 应为 False"

    # 恢复
    _cfg_package.SAVE_DATA_OPTION = "sqlite"
    print("TR-2.3 PASS")
