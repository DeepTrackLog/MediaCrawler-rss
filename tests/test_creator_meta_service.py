# -*- coding: utf-8 -*-
"""
验证 CreatorMetaService：
TR-3.1 同一 (platform, hash) 连续 upsert 2 次 → content_count==2
TR-3.2 SAVE_DATA_OPTION='jsonl' 时直接 return，不抛异常、不访问 DB
"""

import asyncio
import sys
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import config as _cfg_pkg


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def _bootstrap_sqlite(tmp_path: Path):
    db_path = str(tmp_path / "test_meta.db")
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
async def test_TR_3_1_double_upsert_increments_content_count(tmp_path):
    _bootstrap_sqlite(tmp_path)
    from database.db_session import create_tables
    await create_tables("sqlite")

    from services.creator_meta_service import CreatorMetaService
    svc = CreatorMetaService()

    # 第 1 次 upsert
    await svc.upsert("dy", "HASH_TR_3_1", nickname_masked="脱敏昵称A", user_name="展示名A",
                     config_id=42, profile_url="https://x.test/u/a")

    # 第 2 次 upsert：同一个 platform+hash
    await svc.upsert("dy", "HASH_TR_3_1", nickname_masked="脱敏昵称B", user_name="",
                     config_id=None, profile_url=None)

    # 验证
    from database.db_session import get_session
    from database.models import MediaCreatorMeta
    from sqlalchemy import select
    async with get_session() as s:
        obj = (await s.execute(
            select(MediaCreatorMeta).where(
                MediaCreatorMeta.platform == "dy",
                MediaCreatorMeta.creator_hash == "HASH_TR_3_1",
            )
        )).scalar_one()
    assert obj is not None
    assert obj.content_count == 2, f"content_count 应为 2，实际 {obj.content_count}"
    assert obj.nickname_masked == "脱敏昵称A", "第一次的 nickname 不应被第二次空串覆盖"
    assert obj.user_name == "展示名A", "第一次的 user_name 不应被第二次空串覆盖"
    assert obj.config_id == 42, "第一次的 config_id 不应被第二次 None 覆盖"
    assert obj.profile_url == "https://x.test/u/a"
    print("TR-3.1 PASS: content_count=2，且空值不会覆盖已有的非空值。")


@pytest.mark.asyncio
async def test_TR_3_2_non_db_mode_no_error(tmp_path):
    """SAVE_DATA_OPTION='jsonl' 时，upsert 直接 return，不抛任何异常"""
    _cfg_pkg.SAVE_DATA_OPTION = "jsonl"
    from services.creator_meta_service import CreatorMetaService
    svc = CreatorMetaService()

    # 如果内部错误打开 DB 连接，此处会抛。不应抛。
    await svc.upsert("xhs", "SOME_HASH_XYZ", nickname_masked="X昵称A",
                     user_name="X展示名", config_id=1, profile_url="https://x.test/")
    print("TR-3.2 PASS: jsonl 模式下 upsert 静默 return，不报错。")

    # 恢复
    _cfg_pkg.SAVE_DATA_OPTION = "sqlite"
