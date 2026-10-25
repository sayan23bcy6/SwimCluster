"""
test_membership.py – Unit tests for member state machine and conflict resolution.
"""

import pytest
from swimcluster.membership import Member, MembershipList, NodeState, merge_member


# ── merge_member tests ────────────────────────────────────────────────────────

def _member(node_id, state, incarnation):
    return Member(node_id=node_id, host="127.0.0.1", port=7946,
                  state=state, incarnation=incarnation)


def test_new_member_added():
    incoming = _member("n1", NodeState.ALIVE, 0)
    result, changed = merge_member(None, incoming)
    assert changed is True
    assert result.node_id == "n1"


def test_higher_incarnation_wins():
    existing = _member("n1", NodeState.SUSPECT, 3)
    incoming = _member("n1", NodeState.ALIVE,   5)  # higher inc + ALIVE
    result, changed = merge_member(existing, incoming)
    assert changed is True
    assert result.state       == NodeState.ALIVE
    assert result.incarnation == 5


def test_lower_incarnation_is_ignored():
    existing = _member("n1", NodeState.ALIVE, 5)
    incoming = _member("n1", NodeState.DEAD,  3)  # DEAD but stale
    result, changed = merge_member(existing, incoming)
    assert changed is False
    assert result.state == NodeState.ALIVE


def test_equal_incarnation_suspect_overrides_alive():
    existing = _member("n1", NodeState.ALIVE,   2)
    incoming = _member("n1", NodeState.SUSPECT, 2)
    result, changed = merge_member(existing, incoming)
    assert changed is True
    assert result.state == NodeState.SUSPECT


def test_equal_incarnation_dead_overrides_suspect():
    existing = _member("n1", NodeState.SUSPECT, 2)
    incoming = _member("n1", NodeState.DEAD,    2)
    result, changed = merge_member(existing, incoming)
    assert changed is True
    assert result.state == NodeState.DEAD


def test_equal_incarnation_dead_overrides_alive():
    existing = _member("n1", NodeState.ALIVE, 2)
    incoming = _member("n1", NodeState.DEAD,  2)
    result, changed = merge_member(existing, incoming)
    assert changed is True
    assert result.state == NodeState.DEAD


def test_equal_state_and_incarnation_no_change():
    existing = _member("n1", NodeState.ALIVE, 2)
    incoming = _member("n1", NodeState.ALIVE, 2)
    result, changed = merge_member(existing, incoming)
    assert changed is False


# ── MembershipList tests ──────────────────────────────────────────────────────

def test_upsert_new_member():
    ml = MembershipList("self")
    m  = _member("n1", NodeState.ALIVE, 0)
    changed = ml.upsert(m)
    assert changed is True
    assert ml.get("n1") is not None


def test_mark_suspect_transitions_state():
    ml = MembershipList("self")
    ml.upsert(_member("n1", NodeState.ALIVE, 0))
    ml.mark_suspect("n1", 0)
    assert ml.get("n1").state == NodeState.SUSPECT


def test_mark_dead_transitions_state():
    ml = MembershipList("self")
    ml.upsert(_member("n1", NodeState.ALIVE, 0))
    ml.mark_dead("n1", 0)
    assert ml.get("n1").state == NodeState.DEAD


def test_mark_alive_with_higher_incarnation_clears_suspect():
    ml = MembershipList("self")
    ml.upsert(_member("n1", NodeState.SUSPECT, 1))
    changed = ml.mark_alive("n1", "127.0.0.1", 7946, 2)
    assert changed is True
    assert ml.get("n1").state == NodeState.ALIVE


def test_alive_members_excludes_self():
    ml = MembershipList("self")
    ml.upsert(_member("self", NodeState.ALIVE, 0))
    ml.upsert(_member("n1",   NodeState.ALIVE, 0))
    alive = ml.alive_members()
    ids = [m.node_id for m in alive]
    assert "self" not in ids
    assert "n1" in ids


def test_probeable_includes_suspect_excludes_dead():
    ml = MembershipList("self")
    ml.upsert(_member("n1", NodeState.ALIVE,   0))
    ml.upsert(_member("n2", NodeState.SUSPECT, 0))
    ml.upsert(_member("n3", NodeState.DEAD,    0))
    probeable = {m.node_id for m in ml.probeable_members()}
    assert "n1" in probeable
    assert "n2" in probeable
    assert "n3" not in probeable


def test_counts():
    ml = MembershipList("self")
    ml.upsert(_member("n1", NodeState.ALIVE,   0))
    ml.upsert(_member("n2", NodeState.ALIVE,   0))
    ml.upsert(_member("n3", NodeState.SUSPECT, 0))
    ml.upsert(_member("n4", NodeState.DEAD,    0))
    a, s, d = ml.counts()
    assert a == 2
    assert s == 1
    assert d == 1


def test_serialise_roundtrip():
    ml = MembershipList("self")
    ml.upsert(_member("n1", NodeState.ALIVE, 3))
    ml.upsert(_member("n2", NodeState.DEAD,  1))
    snapshot = ml.to_list()

    ml2 = MembershipList("self2")
    ml2.load_from_list(snapshot)
    assert ml2.get("n1").state       == NodeState.ALIVE
    assert ml2.get("n1").incarnation == 3
    assert ml2.get("n2").state       == NodeState.DEAD


def test_load_from_list_skips_malformed():
    ml = MembershipList("self")
    # Missing required fields – should not raise.
    ml.load_from_list([{"id": "n1"}, {"bad": "data"}])
    # No nodes added.
    assert ml.get("n1") is None
