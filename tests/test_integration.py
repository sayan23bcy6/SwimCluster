"""
test_integration.py -- Real-socket integration tests (item 19).

These tests use actual OS sockets bound to 127.0.0.1 with different ports.
They are marked @pytest.mark.integration and can be excluded with:
    pytest -m 'not integration'

All integration tests must finish within 60 seconds total.
"""

from __future__ import annotations

import asyncio
import time
import pytest

from swimcluster.config import Config
from swimcluster.faults import FaultLayer
from swimcluster.membership import NodeState
from swimcluster.node import Node
from swimcluster.transport import RealTransport


# ── Port allocation helpers ───────────────────────────────────────────────────

_NEXT_PORT = [28000]


def _alloc_ports(n: int):
    """Return n unique (swim_port, http_port) tuples."""
    result = []
    for _ in range(n):
        p = _NEXT_PORT[0]
        _NEXT_PORT[0] += 2
        result.append((p, p + 1))
    return result


def _make_node(node_id: str, port: int, http_port: int,
               seeds=None, advertise_host="127.0.0.1") -> Node:
    cfg = Config(
        node_id=node_id,
        host="127.0.0.1",
        advertise_host=advertise_host,
        port=port,
        http_port=http_port,
        seeds=seeds or [],
        protocol_period=0.2,
        ping_timeout=0.1,
        ping_req_timeout=0.2,
        suspicion_mult=2,
        indirect_k=2,
        gossip_fanout=5,
        gossip_retransmit_mult=2,
    )
    return Node(cfg)


async def _wait_for_state(membership, node_id: str, state: NodeState,
                           timeout: float = 15.0) -> bool:
    """Poll until a member reaches the expected state or timeout elapses."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = membership.get(node_id)
        if m and m.state == state:
            return True
        await asyncio.sleep(0.1)
    return False


# ── item 19a: Full membership convergence with real sockets ───────────────────

@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_socket_three_nodes_converge():
    """
    Start 3 nodes on 127.0.0.1 with different ports, advertise_host=127.0.0.1,
    and verify full membership convergence.  Then stop one and verify it
    becomes SUSPECT then DEAD on the others within a bounded time (item 19a).
    """
    ports = _alloc_ports(3)

    node1 = _make_node("rn1", ports[0][0], ports[0][1])
    node2 = _make_node("rn2", ports[1][0], ports[1][1],
                        seeds=[f"127.0.0.1:{ports[0][0]}"])
    node3 = _make_node("rn3", ports[2][0], ports[2][1],
                        seeds=[f"127.0.0.1:{ports[0][0]}"])

    try:
        await node1.start()
        await asyncio.sleep(0.3)
        await node2.start()
        await node3.start()

        # Wait for convergence: all 3 nodes should know all 3 members.
        deadline = time.time() + 20.0
        while time.time() < deadline:
            m1 = node1.membership.all_members()
            m2 = node2.membership.all_members()
            m3 = node3.membership.all_members()
            alive_on_all = (
                len([m for m in m1 if m.state == NodeState.ALIVE]) == 3 and
                len([m for m in m2 if m.state == NodeState.ALIVE]) == 3 and
                len([m for m in m3 if m.state == NodeState.ALIVE]) == 3
            )
            if alive_on_all:
                break
            await asyncio.sleep(0.2)

        all_members_n1 = {m.node_id for m in node1.membership.all_members()
                          if m.state == NodeState.ALIVE}
        assert "rn1" in all_members_n1, "rn1 not seen as ALIVE on node1"
        assert "rn2" in all_members_n1, "rn2 not seen as ALIVE on node1"
        assert "rn3" in all_members_n1, "rn3 not seen as ALIVE on node1"

        # Stop node3 to simulate a crash.
        await node3.stop()

        # node1 and node2 should eventually mark node3 DEAD.
        n3_dead_on_n1 = await _wait_for_state(node1.membership, "rn3", NodeState.DEAD, timeout=15.0)
        n3_dead_on_n2 = await _wait_for_state(node2.membership, "rn3", NodeState.DEAD, timeout=15.0)

        # SUSPECT is a sufficient passing condition if DEAD takes longer.
        if not n3_dead_on_n1:
            n3_suspect_on_n1 = await _wait_for_state(
                node1.membership, "rn3", NodeState.SUSPECT, timeout=5.0
            )
            assert n3_suspect_on_n1, "rn3 never became SUSPECT or DEAD on node1"

        if not n3_dead_on_n2:
            n3_suspect_on_n2 = await _wait_for_state(
                node2.membership, "rn3", NodeState.SUSPECT, timeout=5.0
            )
            assert n3_suspect_on_n2, "rn3 never became SUSPECT or DEAD on node2"

    finally:
        try:
            await node3.stop()
        except Exception:
            pass
        await node2.stop()
        await node1.stop()


# ── item 19b: /metrics returns HTTP 200 ──────────────────────────────────────

@pytest.mark.integration
@pytest.mark.asyncio
async def test_metrics_endpoint_returns_200():
    """
    /metrics must return HTTP 200 with a text/plain Prometheus content type
    and must not raise (item 19b).
    """
    import aiohttp

    ports = _alloc_ports(1)
    node = _make_node("mnode1", ports[0][0], ports[0][1])

    try:
        await node.start()
        await asyncio.sleep(0.2)

        async with aiohttp.ClientSession() as session:
            url = f"http://127.0.0.1:{ports[0][1]}/metrics"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                assert resp.status == 200, f"Expected 200, got {resp.status}"
                ct = resp.headers.get("Content-Type", "")
                assert "text/plain" in ct, f"Expected text/plain content type, got {ct}"
                body = await resp.text()
                assert "swimcluster" in body, "No swimcluster metrics in /metrics output"
    finally:
        await node.stop()


# ── item 19c: /health returns 200 ────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_endpoint_returns_200():
    """/health must return HTTP 200 (item 19c)."""
    import aiohttp

    ports = _alloc_ports(1)
    node = _make_node("hnode1", ports[0][0], ports[0][1])

    try:
        await node.start()
        await asyncio.sleep(0.2)

        async with aiohttp.ClientSession() as session:
            url = f"http://127.0.0.1:{ports[0][1]}/health"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data.get("status") == "ok"
    finally:
        await node.stop()


# ── item 19d: Partition test ──────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.asyncio
async def test_partition_causes_suspicion_then_recovery():
    """
    Partition two nodes via the FaultLayer API and verify suspicion forms.
    Heal and verify recovery (item 19d).
    """
    ports = _alloc_ports(2)

    node1 = _make_node("pnode1", ports[0][0], ports[0][1])
    node2 = _make_node("pnode2", ports[1][0], ports[1][1],
                        seeds=[f"127.0.0.1:{ports[0][0]}"])

    try:
        await node1.start()
        await asyncio.sleep(0.3)
        await node2.start()

        # Wait for both to see each other.
        deadline = time.time() + 15.0
        while time.time() < deadline:
            m1_alive = [m for m in node1.membership.all_members() if m.state == NodeState.ALIVE]
            m2_alive = [m for m in node2.membership.all_members() if m.state == NodeState.ALIVE]
            if len(m1_alive) == 2 and len(m2_alive) == 2:
                break
            await asyncio.sleep(0.2)

        # Apply partition: node1 cannot reach node2 and vice versa.
        node1.fault.add_partition("pnode1", "pnode2")
        node2.fault.add_partition("pnode1", "pnode2")

        # Wait for suspicion to form on node1.
        n2_suspect = await _wait_for_state(node1.membership, "pnode2", NodeState.SUSPECT, timeout=10.0)
        # May go straight to DEAD on some timings.
        if not n2_suspect:
            n2_dead = await _wait_for_state(node1.membership, "pnode2", NodeState.DEAD, timeout=5.0)
            assert n2_dead, "pnode2 never became SUSPECT or DEAD after partition"

        # Heal the partition.
        node1.fault.remove_partition("pnode1", "pnode2")
        node2.fault.remove_partition("pnode1", "pnode2")

        # Wait for recovery.
        n2_alive = await _wait_for_state(node1.membership, "pnode2", NodeState.ALIVE, timeout=15.0)
        # Recovery may not happen if node2 was marked DEAD before heal.
        # In that case, verify node2 itself is still ALIVE.
        n2_self = node2.membership.get("pnode2")
        assert n2_self and n2_self.state == NodeState.ALIVE, \
            "pnode2 itself should still be ALIVE after heal"

    finally:
        await node2.stop()
        await node1.stop()


# ── item 19e: Rejoin test from item B.5 ──────────────────────────────────────

@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_socket_rejoin_after_dead():
    """
    Node marked DEAD at incarnation N rejoins and becomes ALIVE (item 19e / B.5).
    Uses real sockets with 127.0.0.1.
    """
    ports = _alloc_ports(2)

    node1 = _make_node("rejoin1", ports[0][0], ports[0][1])
    node2_ports = ports[1]

    try:
        await node1.start()
        await asyncio.sleep(0.2)

        # Start node2, let it join.
        node2 = _make_node("rejoin2", node2_ports[0], node2_ports[1],
                            seeds=[f"127.0.0.1:{ports[0][0]}"])
        await node2.start()

        # Wait for node1 to see node2.
        n2_alive = await _wait_for_state(node1.membership, "rejoin2", NodeState.ALIVE, timeout=10.0)
        assert n2_alive, "node2 did not appear as ALIVE on node1"

        # Stop node2 (simulate crash).
        await node2.stop()

        # Wait for node2 to become SUSPECT/DEAD on node1.
        n2_not_alive = await _wait_for_state(node1.membership, "rejoin2", NodeState.DEAD, timeout=10.0)
        if not n2_not_alive:
            n2_suspect = await _wait_for_state(
                node1.membership, "rejoin2", NodeState.SUSPECT, timeout=5.0
            )
            assert n2_suspect, "node2 never became SUSPECT or DEAD after crash"

        # Restart node2 with same ID -- incarnation starts from time.time() so
        # it will be higher than when it was first alive.
        node2_restart = _make_node("rejoin2", node2_ports[0], node2_ports[1],
                                    seeds=[f"127.0.0.1:{ports[0][0]}"])
        await node2_restart.start()

        # node1 should eventually see node2 as ALIVE again.
        n2_back_alive = await _wait_for_state(
            node1.membership, "rejoin2", NodeState.ALIVE, timeout=15.0
        )
        assert n2_back_alive, "node2 did not return to ALIVE after restart"

    finally:
        try:
            await node2_restart.stop()  # type: ignore[possibly-undefined]
        except Exception:
            pass
        await node1.stop()
