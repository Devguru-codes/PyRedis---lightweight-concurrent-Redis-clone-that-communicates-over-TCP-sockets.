from __future__ import annotations

import asyncio
import time

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


@pytest.mark.asyncio
async def test_snapshot_export_and_import_round_trip():
    store = DataStore(max_keys=5)
    await store.set("name", "pyredis", ex=60)
    await store.zadd("leaders", [(1.0, "alice"), (2.0, "bob")])
    payload = await store.export_state()

    restored = DataStore(max_keys=5)
    await restored.import_state(payload)

    assert await restored.get("name") == "pyredis"
    assert await restored.zrange("leaders", 0, -1) == ["alice", "bob"]
    assert await restored.ttl("name") >= 0
    info = await restored.info()
    assert info["snapshot_loads"] == "1"


@pytest.mark.asyncio
async def test_import_skips_expired_records():
    store = DataStore(max_keys=5)
    payload = {
        "records": {
            "expired": {
                "kind": "string",
                "value": "x",
                "expires_at": time.time() - 5,
                "expiry_version": 1,
            }
        }
    }
    await store.import_state(payload)
    assert await store.get("expired") is None


@pytest.mark.asyncio
async def test_string_commands_append_setnx_incrby_and_strlen():
    store = DataStore(max_keys=5)
    assert await store.setnx("key", "a") is True
    assert await store.setnx("key", "b") is False
    assert await store.append("key", "bc") == 3
    assert await store.strlen("key") == 3
    assert await store.incrby("counter", 5) == 5
    assert await store.incrby("counter", -2) == 3


@pytest.mark.asyncio
async def test_getset_pttl_pexpire_keys_and_rename():
    store = DataStore(max_keys=10)
    await store.set("user:1", "alice")
    assert await store.getset("user:1", "bob") == "alice"
    assert await store.get("user:1") == "bob"
    assert await store.pexpire("user:1", 1500) is True
    assert await store.pttl("user:1") > 0
    assert await store.keys("user:*") == ["user:1"]
    await store.rename("user:1", "user:2")
    assert await store.get("user:2") == "bob"
    assert await store.keys("user:*") == ["user:2"]


@pytest.mark.asyncio
async def test_zset_score_card_and_remove():
    store = DataStore(max_keys=5)
    await store.zadd("leaders", [(2.0, "bob"), (1.0, "alice")])
    assert await store.zcard("leaders") == 2
    assert await store.zscore("leaders", "alice") == 1.0
    assert await store.zrem("leaders", "alice", "missing") == 1
    assert await store.zcard("leaders") == 1


@pytest.mark.asyncio
async def test_metrics_track_hits_misses_evictions_and_expirations():
    store = DataStore(max_keys=4)
    await store.set("a", "1")
    await store.set("b", "2")
    await store.set("c", "3")
    await store.set("expiring", "5", ex=1)
    assert await store.get("a") == "1"
    assert await store.get("missing") is None
    await store.set("d", "4")
    await asyncio.sleep(1.1)
    await store.purge_expired()
    info = await store.info()
    assert info["read_hits"] == "1"
    assert info["read_misses"] == "1"
    assert info["evicted_keys"] == "1"
    assert info["expired_keys"] == "1"


def test_skiplist_range_ordering():
    skiplist = SkipList()
    skiplist.insert(2.0, "beta")
    skiplist.insert(1.0, "alpha")
    skiplist.insert(3.0, "gamma")
    assert skiplist.range(0, 1) == ["alpha", "beta"]
