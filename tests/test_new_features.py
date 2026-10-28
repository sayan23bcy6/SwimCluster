"""
test_new_features.py -- New tests covering items A.2, B.4, B.5, item 11, and
the gossip queue preferences.

All tests here use InMemoryTransport unless marked @pytest.mark.integration.
"""

from __future__ import annotations

import asyncio
import pytest

from swimcluster.config import Config
from swimcluster.gossip import GossipQueue
from swimcluster.membership import Member, MembershipList, NodeState
from swimcluster.messages import (
    MsgType, alive, dead, join, ping, suspect, validate,
    member_record,
)
from swimcluster.swim import SWIM
from swimcluster.transport import InMemoryTransport


# ── Helpers ────────────────────────────────────────────────────────────────────

def make_config(node_id: str, **kwargs) -> Config:
    defaults = dict(
        node_id=node_id,
        host=node_id,
        port=7946,
        protocol_period=0.05,
        ping_timeout=0.02,
        ping_req_timeout=0.04,
        suspicion_mult=2,
        indirect_k=2,
        gossip_fanout=5,
        gossip_retransmit_mult=2,
    )
    defaults.update(kwargs)
    cfg = Config()
    for k, v in defaults.items():
        setattr(cfg, k, v)
    return cfg


def make_cluster(node_ids):
    """Build multiple SWIM nodes sharing the InMemoryTransport registry."""
    InMemoryTransport.reset()
    nodes = {}
    for nid in node_ids:
        cfg       = make_config(nid)
        transport = InMemoryTransport(nid)
        membership = MembershipList(nid)
        membership.mark_alive(nid, nid, 7946, 0)
        swim_inst = SWIM(cfg, transport, membership)
        transport.set_handler(swim_inst.handle_message)
        nodes[nid] = (swim_inst, transport, membership, cfg)

    for nid, (s, t, m, c) in nodes.items():
        for peer_id in node_ids:
            if peer_id != nid:
                m.mark_alive(peer_id, peer_id, 7946, 0)
    return nodes


# ── item A.2: JOIN_ACK round-trip (InMemory) ──────────────────────────────────

@pytest.mark.asyncio
async def test_join_ack_reaches_joiner_via_inmemory():
    """
    Simulates a TCP JOIN round-trip between two nodes (item A.2).
    The seed should receive a JOIN, return a JOIN_ACK, and the joiner
    should bootstrap its membership from it.
    """
    InMemoryTransport.reset()

    # Seed node.
    seed_cfg  = make_config("seed")
    seed_t    = InMemoryTransport("seed")
    seed_ml   = MembershipList("seed")
    seed_ml.mark_alive("seed", "seed", 7946, 0)
    seed_swim = SWIM(seed_cfg, seed_t, seed_ml)
    seed_t.set_handler(seed_swim.handle_message)
    await seed_t.start()
    await seed_swim.start()

    # Joiner node.
    joiner_cfg  = make_config("joiner")
    joiner_t    = InMemoryTransport("joiner")
    joiner_ml   = MembershipList("joiner")
    joiner_ml.mark_alive("joiner", "joiner", 7946, 0)
    joiner_swim = SWIM(joiner_cfg, joiner_t, joiner_ml)
    joiner_t.set_handler(joiner_swim.handle_message)
    await joiner_t.start()
    await joiner_swim.start()

    # Send JOIN from joiner to seed.
    join_msg = join("joiner", 1, "joiner", 7946)
    reply = await joiner_t.send_tcp(join_msg, ("seed", 7946))

    # The reply must be a valid JOIN_ACK.
    assert reply is not None, "No reply received from seed"
    assert reply["type"] == MsgType.JOIN_ACK.value, f"Unexpected reply type: {reply['type']}"

    # Process the JOIN_ACK on the joiner.
    await joiner_swim.handle_message(reply, ("seed", 7946))

    # Joiner should now know about seed.
    seed_in_joiner = joiner_ml.get("seed")
    assert seed_in_joiner is not None, "Joiner did not learn seed's membership"
    assert seed_in_joiner.state == NodeState.ALIVE

    await seed_t.stop()
    await joiner_t.stop()


# ── item B.4: Suspicion timers -- re-suspicion after refutation ────────────────

@pytest.mark.asyncio
async def test_resuspicion_after_refutation_starts_new_timer():
    """
    Suspect X, refute X (marking ALIVE), then suspect X again.
    The second suspicion must start a new timer (item B.4).
    """
    nodes = make_cluster(["n1", "n2"])
    for nid, (s, t, m, c) in nodes.items():
        await t.start()
        await s.start()

    swim_n1, _, membership_n1, cfg = nodes["n1"]
    n2_member = membership_n1.get("n2")

    # First suspicion.
    swim_n1._suspect_node(n2_member)
    assert membership_n1.get("n2").state == NodeState.SUSPECT
    assert "n2" in swim_n1._suspect_timers

    # Refutation: ALIVE with higher incarnation cancels timer.
    refute_inc = swim_n1._incarnation + 100  # higher than current
    alive_msg = alive("n2", 99, "n2", incarnation=refute_inc)
    await swim_n1.handle_message(alive_msg, ("n2", 7946))

    # n2 must now be ALIVE and timer gone.
    assert membership_n1.get("n2").state == NodeState.ALIVE
    assert "n2" not in swim_n1._suspect_timers

    # Second suspicion -- get a fresh member view.
    n2_member_v2 = membership_n1.get("n2")
    # Force state back so _suspect_node can act.
    # But _suspect_node skips if state==SUSPECT AND timer exists.
    # Since the timer was cleared, we can suspect directly.
    # Reset n2 to ALIVE so we can suspect it again cleanly.
    membership_n1.mark_alive("n2", "n2", 7946, n2_member_v2.incarnation + 1)
    n2_member_v2 = membership_n1.get("n2")

    # _suspect_node should start a new timer because timer is gone.
    swim_n1._suspect_node(n2_member_v2)


    # Timer must exist again.
    assert "n2" in swim_n1._suspect_timers, "Second suspicion did not start a new timer"

    for nid, (s, t, m, c) in nodes.items():
        await t.stop()


# ── item B.4: Dead clears suspect timer ───────────────────────────────────────

@pytest.mark.asyncio
async def test_dead_clears_suspect_timer():
    """When a node is marked DEAD, its suspect timer must be cancelled (item B.4)."""
    nodes = make_cluster(["n1", "n2"])
    for nid, (s, t, m, c) in nodes.items():
        await t.start()
        await s.start()

    swim_n1, _, membership_n1, cfg = nodes["n1"]
    n2_member = membership_n1.get("n2")
    swim_n1._suspect_node(n2_member)

    assert "n2" in swim_n1._suspect_timers

    # Mark dead directly.
    swim_n1._mark_dead("n2", n2_member.incarnation)

    assert "n2" not in swim_n1._suspect_timers
    assert membership_n1.get("n2").state == NodeState.DEAD

    for nid, (s, t, m, c) in nodes.items():
        await t.stop()


# ── item B.5: Rejoin after restart ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rejoin_after_being_marked_dead():
    """
    A node marked DEAD at incarnation N rejoins and becomes ALIVE (item B.5).
    The restarted node has a higher incarnation (time-based init), so the
    ALIVE message overrides the DEAD state.
    """
    import time

    nodes = make_cluster(["n1", "n2"])
    for nid, (s, t, m, c) in nodes.items():
        await t.start()
        await s.start()

    swim_n1, _, membership_n1, _ = nodes["n1"]
    swim_n2, _, membership_n2, _ = nodes["n2"]

    # Mark n2 as DEAD on n1 at a low incarnation.
    dead_inc = 5
    membership_n1.mark_dead("n2", dead_inc)
    assert membership_n1.get("n2").state == NodeState.DEAD

    # Simulate n2 restarting with a higher incarnation (time-based init).
    # Use a definitely-higher incarnation.
    rejoin_inc = int(time.time()) + 1000
    alive_msg = alive("n2", 1, "n2", incarnation=rejoin_inc)
    await swim_n1.handle_message(alive_msg, ("n2", 7946))

    # n1 must now see n2 as ALIVE.
    n2_state = membership_n1.get("n2")
    assert n2_state is not None
    assert n2_state.state == NodeState.ALIVE
    assert n2_state.incarnation == rejoin_inc

    for nid, (s, t, m, c) in nodes.items():
        await t.stop()


# ── item 11: GossipQueue prefers least-sent items ────────────────────────────

def test_gossip_queue_prefers_least_sent():
    """
    get_updates must prefer least-sent items (item 11).
    Items with sends==0 should be chosen before items with sends==5.
    """
    q = GossipQueue(fanout=2, retransmit_mult=10)

    # Add item A (will be sent 5 times).
    rec_a = member_record("nodeA", "127.0.0.1", 7946, "ALIVE", 0)
    q.enqueue(rec_a)
    for _ in range(5):
        q.get_updates(cluster_size=10)
    # Re-enqueue so it is still in the queue with sends=5.
    # Actually sends counter is on the GossipItem, not the record.
    # Re-enqueue to reset sends to 0.
    q.enqueue(rec_a)  # this replaces and resets sends to 0

    # Add item B fresh with sends=0.
    rec_b = member_record("nodeB", "127.0.0.1", 7947, "SUSPECT", 1)
    q.enqueue(rec_b)

    # Manually set item A's sends higher to test ordering.
    # Access queue internals.
    for item in q._queue:
        if item.record.get("id") == "nodeA":
            item.sends = 5
            break

    # With fanout=1, the item with least sends (nodeB, sends=0) should be chosen.
    q.fanout = 1
    chosen = q.get_updates(cluster_size=10)
    assert len(chosen) == 1
    assert chosen[0]["id"] == "nodeB", (
        f"Expected nodeB (sends=0) to be chosen first, got {chosen[0]['id']}"
    )


def test_gossip_queue_least_sent_order_with_multiple():
    """Additional check: among 3 items sorted by sends, fanout=2 picks 2 lowest."""
    q = GossipQueue(fanout=2, retransmit_mult=10)

    for nid, sends in [("n1", 3), ("n2", 1), ("n3", 0)]:
        rec = member_record(nid, "127.0.0.1", 7946, "ALIVE", 0)
        q.enqueue(rec)
        # Artificially set sends.
        for item in q._queue:
            if item.record.get("id") == nid:
                item.sends = sends
                break

    chosen = q.get_updates(cluster_size=10)
    chosen_ids = [c["id"] for c in chosen]
    # n3 (sends=0) and n2 (sends=1) should be chosen before n1 (sends=3).
    assert "n3" in chosen_ids
    assert "n2" in chosen_ids
    assert "n1" not in chosen_ids


# ── item 10: validate rejects unknown message type ───────────────────────────

def test_validate_rejects_unknown_message_type():
    """validate() must return False for unknown message types (item 10)."""
    msg = {
        "type": "TOTALLY_UNKNOWN",
        "sender": "node1",
        "seq": 1,
    }
    assert validate(msg) is False


def test_validate_accepts_known_types():
    """validate() must accept all known MsgType values."""
    from swimcluster.messages import ping, ack, alive, suspect, dead, join, join_ack
    assert validate(ping("n1", 1, "n2")) is True
    assert validate(ack("n1", 1, "n2")) is True
    assert validate(alive("n1", 1, "n1", 0)) is True
    assert validate(suspect("n1", 1, "n2", 0)) is True
    assert validate(dead("n1", 1, "n2", 0)) is True
    assert validate(join("n1", 1, "host", 7946)) is True
    assert validate(join_ack("n1", 1, [])) is True


# ── item B.6: Self-refutation via gossip ──────────────────────────────────────

@pytest.mark.asyncio
async def test_self_refutation_via_gossip():
    """
    When a SUSPECT gossip record for the local node arrives at inc >= our
    incarnation, _apply_gossip triggers self-refutation (item B.6).
    """
    InMemoryTransport.reset()

    cfg       = make_config("n1")
    transport = InMemoryTransport("n1")
    ml        = MembershipList("n1")
    ml.mark_alive("n1", "n1", 7946, 0)
    # Add a peer so ALIVE broadcast has a destination.
    ml.mark_alive("n2", "n2", 7946, 0)
    swim_inst = SWIM(cfg, transport, ml)
    transport.set_handler(swim_inst.handle_message)
    await transport.start()
    await swim_inst.start()

    # Register peer n2 so broadcasts don't silently drop.
    n2_cfg  = make_config("n2")
    n2_t    = InMemoryTransport("n2")
    n2_ml   = MembershipList("n2")
    n2_ml.mark_alive("n2", "n2", 7946, 0)
    n2_swim = SWIM(n2_cfg, n2_t, n2_ml)
    n2_t.set_handler(n2_swim.handle_message)
    await n2_t.start()

    initial_inc = swim_inst._incarnation

    # Inject gossip containing SUSPECT record for n1 at our current incarnation.
    gossip_payload = [member_record("n1", "n1", 7946, "SUSPECT", initial_inc)]
    ping_with_gossip = ping("n2", 1, "n1", gossip=gossip_payload)
    await swim_inst.handle_message(ping_with_gossip, ("n2", 7946))

    # Give the asyncio task a moment to run.
    await asyncio.sleep(0.05)

    # Incarnation must have incremented.
    assert swim_inst._incarnation > initial_inc, (
        f"Self-refutation did not increment incarnation: "
        f"{swim_inst._incarnation} <= {initial_inc}"
    )
    # Local membership entry must be ALIVE at new incarnation.
    local_m = ml.get("n1")
    assert local_m is not None
    assert local_m.state == NodeState.ALIVE
    assert local_m.incarnation == swim_inst._incarnation

    await transport.stop()
    await n2_t.stop()


def test_node_stays_alive_after_load_from_list_with_stale_dead_record():
    """
    Item A.7: A node created with a startup incarnation greater than a stale
    DEAD record at a lower incarnation for the same node id must stay ALIVE
    after load_from_list of a seed snapshot that contains that DEAD record.
    """
    node_id = "rejoining_node"
    startup_inc = 1000
    ml = MembershipList(node_id)
    ml.mark_alive(node_id, "127.0.0.1", 7946, startup_inc)

    # Seed snapshot contains stale DEAD record for this node at lower incarnation.
    seed_snapshot = [
        {"id": "seed", "host": "127.0.0.1", "port": 7945, "state": "ALIVE", "incarnation": 0},
        {"id": node_id, "host": "127.0.0.1", "port": 7946, "state": "DEAD", "incarnation": 5},
    ]

    ml.load_from_list(seed_snapshot)

    local_entry = ml.get(node_id)
    assert local_entry is not None
    assert local_entry.state == NodeState.ALIVE
    assert local_entry.incarnation == startup_inc
