# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#
"""
Unit tests for DouyinRestStoreImplement (REST store client contract).
"""

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from store.douyin import DouyinStoreFactory
from store.douyin._store_impl import DouyinRestStoreImplement


@pytest.fixture
def rest_store():
    """A DouyinRestStoreImplement wired to a fake base_url + api_key."""
    with patch("config.REST_STORE_BASE_URL", "https://feed.example.com"), \
            patch("config.REST_STORE_API_KEY", "test-key"), \
            patch("config.REST_STORE_TIMEOUT", 10.0), \
            patch("config.REST_STORE_SSL_VERIFY", None):
        store = DouyinRestStoreImplement()
    return store


def _ok_response():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = ""
    return mock_resp


class TestDouyinRestStoreFactory:
    @patch("config.SAVE_DATA_OPTION", "rest")
    @patch("config.REST_STORE_BASE_URL", "https://feed.example.com")
    def test_create_rest_store(self):
        store = DouyinStoreFactory.create_store()
        assert isinstance(store, DouyinRestStoreImplement)

    @patch("config.REST_STORE_BASE_URL", "")
    def test_init_raises_without_base_url(self):
        with pytest.raises(ValueError, match="REST_STORE_BASE_URL"):
            DouyinRestStoreImplement()

    def test_rest_registered_in_factory(self):
        assert "rest" in DouyinStoreFactory.STORES
        assert DouyinStoreFactory.STORES["rest"] is DouyinRestStoreImplement


class TestDouyinRestStoreImplement:
    @pytest.mark.asyncio
    async def test_store_content_posts_to_contents(self, rest_store):
        rest_store._client.post = AsyncMock(return_value=_ok_response())
        content_item = {"aweme_id": "123", "creator_hash": "hashabc", "title": "t", "desc": "d"}

        await rest_store.store_content(content_item)

        rest_store._client.post.assert_awaited_once()
        args, kwargs = rest_store._client.post.call_args
        assert args[0] == "https://feed.example.com/api/v1/contents"
        assert kwargs["json"] == content_item
        assert kwargs["headers"] == {"Authorization": "Bearer test-key"}

    @pytest.mark.asyncio
    async def test_store_comment_posts_to_comments(self, rest_store):
        rest_store._client.post = AsyncMock(return_value=_ok_response())
        comment_item = {"comment_id": "c1", "aweme_id": "123", "content": "hi"}

        await rest_store.store_comment(comment_item)

        args, kwargs = rest_store._client.post.call_args
        assert args[0] == "https://feed.example.com/api/v1/comments"
        assert kwargs["json"] == comment_item
        assert kwargs["headers"] == {"Authorization": "Bearer test-key"}

    @pytest.mark.asyncio
    async def test_store_creator_posts_to_creators(self, rest_store):
        rest_store._client.post = AsyncMock(return_value=_ok_response())
        creator = {"creator_hash": "hashabc", "nickname": "nick"}

        await rest_store.store_creator(creator)

        args, kwargs = rest_store._client.post.call_args
        assert args[0] == "https://feed.example.com/api/v1/creators"
        assert kwargs["json"] == creator

    @pytest.mark.asyncio
    async def test_store_content_skips_without_aweme_id(self, rest_store):
        rest_store._client.post = AsyncMock(return_value=_ok_response())
        await rest_store.store_content({"creator_hash": "x"})
        rest_store._client.post.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_store_comment_skips_without_comment_id(self, rest_store):
        rest_store._client.post = AsyncMock(return_value=_ok_response())
        await rest_store.store_comment({"aweme_id": "1"})
        rest_store._client.post.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_server_error_does_not_raise(self, rest_store):
        err_resp = MagicMock()
        err_resp.status_code = 500
        err_resp.text = "internal server error"
        rest_store._client.post = AsyncMock(return_value=err_resp)

        # should NOT raise (crawler main loop must not be interrupted)
        await rest_store.store_content({"aweme_id": "1"})
        rest_store._client.post.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_network_error_does_not_raise(self, rest_store):
        rest_store._client.post = AsyncMock(side_effect=httpx.ConnectError("boom"))

        # should NOT raise
        await rest_store.store_content({"aweme_id": "1"})
        rest_store._client.post.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_authorization_header_when_api_key_empty(self):
        with patch("config.REST_STORE_BASE_URL", "https://feed.example.com"), \
                patch("config.REST_STORE_API_KEY", ""):
            store = DouyinRestStoreImplement()
        store._client.post = AsyncMock(return_value=_ok_response())

        await store.store_content({"aweme_id": "1"})

        _, kwargs = store._client.post.call_args
        assert kwargs["headers"] == {}

    @pytest.mark.asyncio
    async def test_base_url_trailing_slash_stripped(self):
        with patch("config.REST_STORE_BASE_URL", "https://feed.example.com/"), \
                patch("config.REST_STORE_API_KEY", "k"):
            store = DouyinRestStoreImplement()
        store._client.post = AsyncMock(return_value=_ok_response())

        await store.store_content({"aweme_id": "1"})

        args, _ = store._client.post.call_args
        assert args[0] == "https://feed.example.com/api/v1/contents"
