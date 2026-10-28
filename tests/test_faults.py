"""
test_faults.py – Tests for the fault injection layer.
"""

import asyncio
import pytest
from swimcluster.faults import FaultLayer, FaultConfig


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_fault(local_id="n1", **kwargs) -> FaultLayer:
    cfg = FaultConfig(**kwargs)
    return FaultLayer(local_id, cfg)


# ── Packet loss ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_loss_prob_zero_always_allows():
    fl = make_fault(loss_prob=0.0)
    for _ in range(20):
        assert await fl.allow_send(("n2", 7946)) is True


@pytest.mark.asyncio
async def test_loss_prob_one_always_drops():
    fl = make_fault(loss_prob=1.0)
    for _ in range(20):
        assert await fl.allow_send(("n2", 7946)) is False


@pytest.mark.asyncio
async def test_loss_prob_partial():
    """With loss_prob=0.5, roughly half the packets are dropped."""
    fl = make_fault(loss_prob=0.5)
    results = [await fl.allow_send(("n2", 7946)) for _ in range(200)]
    allowed = sum(results)
    # Allow loose range for randomness.
    assert 60 <= allowed <= 140, f"Expected ~100 allowed, got {allowed}"


# ── Partition ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_partition_blocks_traffic():
    fl = make_fault()
    fl.add_partition("n1", "n2")
    assert await fl.allow_send(("n2", 7946)) is False


@pytest.mark.asyncio
async def test_partition_is_bidirectional():
    """Partition added from n2's perspective should also block n1's sends."""
    fl_n2 = FaultLayer("n2", FaultConfig())
    fl_n2.add_partition("n1", "n2")  # key normalised to sorted tuple
    assert await fl_n2.allow_send(("n1", 7946)) is False


@pytest.mark.asyncio
async def test_partition_heal_restores_traffic():
    fl = make_fault()
    fl.add_partition("n1", "n2")
    fl.remove_partition("n1", "n2")
    assert await fl.allow_send(("n2", 7946)) is True


@pytest.mark.asyncio
async def test_partition_does_not_affect_other_pairs():
    fl = make_fault(local_id="n1")
    fl.add_partition("n1", "n2")
    # Traffic to n3 should be unaffected.
    assert await fl.allow_send(("n3", 7946)) is True


# ── Clear all ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_clear_all_resets_everything():
    fl = make_fault(loss_prob=1.0)
    fl.add_partition("n1", "n2")
    fl.clear_all()
    assert await fl.allow_send(("n2", 7946)) is True


# ── Status ────────────────────────────────────────────────────────────────────

def test_status_returns_dict():
    fl = make_fault(loss_prob=0.3)
    fl.add_partition("n1", "n2")
    status = fl.status()
    assert status["loss_prob"] == 0.3
    assert len(status["partitions"]) == 1


# ── Runtime configuration ─────────────────────────────────────────────────────

def test_set_loss_clamps_to_valid_range():
    fl = FaultLayer("n1")
    fl.set_loss(-1.0)
    assert fl.config.loss_prob == 0.0
    fl.set_loss(2.0)
    assert fl.config.loss_prob == 1.0


def test_set_delay():
    fl = FaultLayer("n1")
    fl.set_delay(0.01, 0.05)
    assert fl.config.delay_min == 0.01
    assert fl.config.delay_max == 0.05


@pytest.mark.asyncio
async def test_partition_on_node1_blocks_bidirectional_by_node_id():
    """
    Item D.11: A partition added on node1 blocks traffic in both directions
    (outgoing from node1 and incoming to node1 from the partitioned peer),
    and is matched by node ID, not IP.
    """
    from swimcluster.transport import InMemoryTransport
    from swimcluster.messages import ping

    InMemoryTransport.reset()
    fault1 = FaultLayer("node1")
    fault2 = FaultLayer("node2")
    t1 = InMemoryTransport("node1", fault_layer=fault1)
    t2 = InMemoryTransport("node2", fault_layer=fault2)

    received_at_1 = []
    received_at_2 = []

    async def handler1(msg, addr):
        received_at_1.append(msg)

    async def handler2(msg, addr):
        received_at_2.append(msg)

    t1.set_handler(handler1)
    t2.set_handler(handler2)
    await t1.start()
    await t2.start()

    # Partition node1 and node2 ONLY on node1
    fault1.add_partition("node1", "node2")

    # Send outgoing from node1 to node2 (using IP address in addr, but node_id in target_node_id)
    msg1 = ping("node1", 1, "node2")
    await t1.send_udp(msg1, ("192.168.1.2", 7946), target_node_id="node2")
    assert len(received_at_2) == 0, "Outgoing from node1 should be blocked"

    # Send incoming from node2 to node1
    msg2 = ping("node2", 2, "node1")
    await t2.send_udp(msg2, ("192.168.1.1", 7946), target_node_id="node1")
    assert len(received_at_1) == 0, "Incoming to node1 should be blocked"

    await t1.stop()
    await t2.stop()
