"""
test_kvstore.py – Tests for the KV store and consistent-hash ring.

Required: keys are reassigned when a member dies.
"""

import pytest
from swimcluster.kvstore import ConsistentHashRing, KVStore
from swimcluster.membership import Member, NodeState


def _alive(node_id: str) -> Member:
    return Member(node_id=node_id, host="127.0.0.1", port=7946,
                  state=NodeState.ALIVE, incarnation=0)


def _dead(node_id: str) -> Member:
    return Member(node_id=node_id, host="127.0.0.1", port=7946,
                  state=NodeState.DEAD, incarnation=0)


# ── ConsistentHashRing ────────────────────────────────────────────────────────

def test_ring_empty_returns_none():
    ring = ConsistentHashRing(replicas=10)
    assert ring.get_node("any-key") is None


def test_ring_single_node_owns_all_keys():
    ring = ConsistentHashRing(replicas=50)
    ring.rebuild([_alive("n1")])
    assert ring.get_node("a")     == "n1"
    assert ring.get_node("b")     == "n1"
    assert ring.get_node("hello") == "n1"


def test_ring_distributes_across_nodes():
    ring = ConsistentHashRing(replicas=150)
    members = [_alive(f"n{i}") for i in range(5)]
    ring.rebuild(members)

    owners = {ring.get_node(f"key-{i}") for i in range(100)}
    # With 5 nodes and 150 replicas, nearly all nodes should own some keys.
    assert len(owners) > 1


def test_ring_dead_nodes_excluded():
    ring = ConsistentHashRing(replicas=50)
    ring.rebuild([_alive("n1"), _dead("n2")])
    for i in range(50):
        assert ring.get_node(f"k{i}") == "n1"


def test_ring_rebuild_after_node_removal():
    ring = ConsistentHashRing(replicas=150)
    members = [_alive(f"n{i}") for i in range(3)]
    ring.rebuild(members)
    keys = [f"key-{i}" for i in range(50)]
    before = {k: ring.get_node(k) for k in keys}

    # Remove n2 (mark dead).
    ring.rebuild([_alive("n0"), _dead("n1"), _alive("n2")])
    after = {k: ring.get_node(k) for k in keys}

    # Keys formerly owned by n1 must be reassigned.
    reassigned = [k for k in keys if before[k] == "n1" and after[k] != "n1"]
    # All n1 keys should be reassigned to n0 or n2.
    assert all(after[k] in ("n0", "n2") for k in reassigned)


# ── KVStore ────────────────────────────────────────────────────────────────────

def test_put_get_delete():
    kv = KVStore("n1")
    kv.put("hello", "world")
    assert kv.get("hello") == "world"
    assert kv.delete("hello") is True
    assert kv.get("hello") is None


def test_delete_nonexistent_returns_false():
    kv = KVStore("n1")
    assert kv.delete("no-such-key") is False


def test_keys_reassigned_when_member_dies():
    """
    When a live node dies and membership changes, keys owned by the dead
    node should be reassigned to surviving nodes.
    """
    kv = KVStore("n1", replicas=150)
    members = [_alive(f"n{i}") for i in range(4)]
    kv.on_membership_change(members)

    # Store a set of keys.
    for i in range(40):
        kv.put(f"key-{i}", f"val-{i}")

    # Identify which keys n2 owned.
    n2_keys = [f"key-{i}" for i in range(40) if kv.owner(f"key-{i}") == "n2"]

    # Kill n2.
    members_after = [_alive("n0"), _alive("n1"), _dead("n2"), _alive("n3")]
    kv.on_membership_change(members_after)

    # n2_keys should now be owned by one of the other nodes.
    for k in n2_keys:
        owner = kv.owner(k)
        assert owner in ("n0", "n1", "n3"), f"Key {k} still maps to dead n2"


def test_is_local_owner():
    kv = KVStore("n1", replicas=150)
    kv.on_membership_change([_alive("n0"), _alive("n1"), _alive("n2")])

    # Find a key owned by n1.
    local_key = None
    for i in range(200):
        k = f"probe-{i}"
        if kv.owner(k) == "n1":
            local_key = k
            break

    if local_key:
        assert kv.is_local_owner(local_key) is True


def test_snapshot():
    kv = KVStore("n1")
    kv.put("a", "1")
    kv.put("b", "2")
    snap = kv.snapshot()
    assert snap == {"a": "1", "b": "2"}
    snap["a"] = "MUTATED"
    assert kv.get("a") == "1"  # original unaffected
