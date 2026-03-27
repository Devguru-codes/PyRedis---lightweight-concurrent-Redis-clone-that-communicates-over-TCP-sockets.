from __future__ import annotations

import asyncio

import pytest

from pyredis.datastore import DataStore
from pyredis.errors import WrongTypeError
from pyredis.lru import LRUTracker
from pyredis.skiplist import SkipList


def test_lru_tracker_evicts_oldest():
    lru = LRUTracker()
    lru.touch("a")
    lru.touch("b")
    lru.touch("a")
    assert lru.pop_lru() == "b"


@pytest.mark.asyncio
async def test_ttl_expiration():
    store = DataStore(max_keys=5)
    await store.set("session", "value", ex=1)
    await asyncio.sleep(1.1)
    await store.purge_expired()
    assert await store.get("session") is None


@pytest.mark.asyncio
async def test_lru_eviction():
    store = DataStore(max_keys=2)
    await store.set("a", "1")
    await store.set("b", "2")
    assert await store.get("a") == "1"
    await store.set("c", "3")
    assert await store.get("b") is None
    assert await store.get("a") == "1"
    assert await store.get("c") == "3"


@pytest.mark.asyncio
async def test_persist_clears_ttl():
    store = DataStore(max_keys=5)
    await store.set("session", "value", ex=10)
    assert await store.persist("session") is True
    assert await store.ttl("session") == -1


@pytest.mark.asyncio
async def test_mset_mget_and_type():
    store = DataStore(max_keys=5)
    await store.mset([("a", "1"), ("b", "2")])
    assert await store.mget(["a", "b", "missing"]) == ["1", "2", None]
    assert await store.type_of("a") == "string"
    assert await store.type_of("missing") == "none"


@pytest.mark.asyncio
async def test_get_wrong_type_raises():
    store = DataStore(max_keys=5)
    await store.zadd("leaders", [(1.0, "alice")])
    with pytest.raises(WrongTypeError):
        await store.get("leaders")


@pytest.mark.asyncio
async def test_dbsize_and_exists_track_expired_records():
    store = DataStore(max_keys=5)
    await store.set("alive", "1")
    await store.set("soon-gone", "2", ex=1)
    assert await store.dbsize() == 2
    assert await store.exists("alive", "soon-gone", "missing") == 2
    await asyncio.sleep(1.1)
    await store.purge_expired()
    assert await store.dbsize() == 1
    assert await store.exists("alive", "soon-gone") == 1


@pytest.mark.asyncio
async def test_persist_returns_false_for_missing_or_non_expiring_key():
    store = DataStore(max_keys=5)
    await store.set("plain", "1")
    assert await store.persist("plain") is False
    assert await store.persist("missing") is False


def test_skiplist_range_ordering():
    skiplist = SkipList()
    skiplist.insert(2.0, "beta")
    skiplist.insert(1.0, "alpha")
    skiplist.insert(3.0, "gamma")
    assert skiplist.range(0, 1) == ["alpha", "beta"]
