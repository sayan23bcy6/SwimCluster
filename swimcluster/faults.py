"""
faults.py -- Fault injection: packet loss, artificial delay, and network partitions.

FaultLayer wraps the transport and intercepts outgoing messages.  It can:
  * Drop packets with a configurable probability.
  * Add a random delay in [delay_min, delay_max] seconds before delivery.
  * Block all traffic between two specific node pairs (partition).

Partitions are checked against **node IDs**, not IP addresses, so the fault
layer works correctly regardless of how nodes are addressed on the network.
Partition blocking is applied both on the send side (allow_send_to_node) and
on the receive side (UDPProtocol.datagram_received checks _is_partitioned).

Fault parameters can be changed at runtime via the HTTP admin endpoint
without restarting the node.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Set, Tuple

log = logging.getLogger(__name__)


@dataclass
class FaultConfig:
    """Current fault-injection settings."""
    loss_prob:  float       = 0.0   # probability [0, 1] of dropping any packet
    delay_min:  float       = 0.0   # min artificial delay (seconds)
    delay_max:  float       = 0.0   # max artificial delay (seconds)
    # Set of (node_a, node_b) pairs -- traffic is blocked in both directions.
    partitions: Set[Tuple[str, str]] = field(default_factory=set)


class FaultLayer:
    """
    Intercepts outbound sends and applies the current FaultConfig.

    Designed to be passed to the Transport constructor.  The transport
    calls allow_send_to_node(node_id) before every UDP send; returning
    False means the packet is dropped.

    Partitions are bidirectional and keyed by node ID pairs, not IPs.
    """

    def __init__(self, local_id: str, config: FaultConfig | None = None) -> None:
        self._local_id = local_id
        self.config    = config or FaultConfig()

    async def allow_send(self, addr: Tuple[str, int]) -> bool:
        """
        Backward-compatible wrapper: derive node_id from addr[0] and delegate.
        addr[0] is treated as the target node_id (works for InMemoryTransport
        where addr[0] == node_id and for RealTransport callers that pass IPs
        but may not have a node_id available).
        """
        return await self.allow_send_to_node(addr[0])

    async def allow_send_to_node(self, target_node_id: str) -> bool:
        """
        Return True if the packet should be delivered, False to drop it.
        Also applies the artificial delay when the packet is kept.

        target_node_id must be the logical node ID of the recipient.
        """
        # Check partition -- block traffic between partitioned node ID pairs.
        if self._is_partitioned(target_node_id):
            log.debug("Fault: partition blocking %s -> %s",
                      self._local_id, target_node_id)
            return False

        # Apply random packet loss.
        if self.config.loss_prob > 0 and random.random() < self.config.loss_prob:
            log.debug("Fault: dropping packet %s -> %s (loss)",
                      self._local_id, target_node_id)
            return False

        # Apply artificial delay.
        delay = self._jitter()
        if delay > 0:
            await asyncio.sleep(delay)

        return True

    def _is_partitioned(self, target_id: str) -> bool:
        """Return True if traffic between us and target should be blocked."""
        pair_a = (self._local_id, target_id)
        pair_b = (target_id, self._local_id)
        return pair_a in self.config.partitions or pair_b in self.config.partitions

    def _jitter(self) -> float:
        lo, hi = self.config.delay_min, self.config.delay_max
        if hi <= lo or hi <= 0:
            return 0.0
        return random.uniform(lo, hi)

    # -- Runtime configuration -------------------------------------------------

    def set_loss(self, prob: float) -> None:
        """Set packet loss probability, clamped to [0, 1]."""
        self.config.loss_prob = max(0.0, min(1.0, prob))
        log.info("Fault: loss_prob set to %.2f", self.config.loss_prob)

    def set_delay(self, min_s: float, max_s: float) -> None:
        """Set artificial delay range in seconds."""
        self.config.delay_min = max(0.0, min_s)
        self.config.delay_max = max(0.0, max_s)
        log.info("Fault: delay set to [%.3f, %.3f]s", min_s, max_s)

    def add_partition(self, node_a: str, node_b: str) -> None:
        """Block all traffic between node_a and node_b (bidirectional by node ID)."""
        pair = tuple(sorted([node_a, node_b]))
        self.config.partitions.add(pair)  # type: ignore[arg-type]
        log.warning("Fault: partition added between %s and %s", node_a, node_b)

    def remove_partition(self, node_a: str, node_b: str) -> None:
        """Restore traffic between a previously partitioned pair."""
        pair = tuple(sorted([node_a, node_b]))
        self.config.partitions.discard(pair)  # type: ignore[arg-type]
        log.info("Fault: partition removed between %s and %s", node_a, node_b)

    def clear_all(self) -> None:
        """Remove all fault injection (reset to clean network)."""
        self.config = FaultConfig()
        log.info("Fault: all faults cleared")

    def status(self) -> dict:
        """Return current fault config as a plain dict for the admin endpoint."""
        return {
            "loss_prob":  self.config.loss_prob,
            "delay_min":  self.config.delay_min,
            "delay_max":  self.config.delay_max,
            "partitions": [list(p) for p in self.config.partitions],
        }
