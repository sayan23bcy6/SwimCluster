"""
gossip.py -- Piggyback membership updates on every outgoing message.

Rather than sending dedicated gossip messages, SWIM piggybacks
membership updates onto whatever messages are already going out
(PINGs, ACKs, PING_REQs).  Each update is retransmitted a bounded
number of times (retransmit_limit = retransmit_mult * ceil(log2(N+1)))
before being dropped to prevent unbounded propagation.

A GossipQueue holds pending updates ordered by send count so the
least-sent items are always preferred, ensuring fast convergence.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class GossipItem:
    """One membership-change event waiting to be piggybacked."""
    record: Dict[str, Any]  # member_record dict from messages.py
    sends:  int = 0         # how many times this item has been sent


class GossipQueue:
    """
    Priority queue (FIFO within same send-count) of pending gossip items.
    Items are dropped once they have been sent retransmit_limit times.
    """

    def __init__(self, fanout: int = 5, retransmit_mult: int = 4) -> None:
        self.fanout           = fanout            # items per outgoing message
        self.retransmit_mult  = retransmit_mult   # multiplied by log2(N)
        self._queue: deque[GossipItem] = deque()

    def _retransmit_limit(self, cluster_size: int) -> int:
        """
        Compute the maximum send count for an update.
        log2(N+1) gives O(log N) convergence; the multiplier adds safety margin.
        """
        n = max(cluster_size, 1)
        return self.retransmit_mult * math.ceil(math.log2(n + 1))

    def enqueue(self, record: Dict[str, Any]) -> None:
        """
        Add a new membership update.  If the same node already has a
        pending update, replace it (newer state supersedes older).
        """
        node_id = record.get("id")
        # Remove any stale entry for the same node.
        self._queue = deque(
            item for item in self._queue if item.record.get("id") != node_id
        )
        self._queue.appendleft(GossipItem(record=record))

    def get_updates(self, cluster_size: int) -> List[Dict[str, Any]]:
        """
        Return up to fanout updates for the next outgoing message,
        incrementing their send counter and dropping exhausted items.

        Items are sorted by sends ascending so the least-sent items are
        always preferred (item 11), ensuring fast convergence for new events.
        """
        limit   = self._retransmit_limit(cluster_size)
        # Sort queue by sends ascending so least-sent items come first.
        sorted_items = sorted(self._queue, key=lambda it: it.sends)
        chosen  = []
        updated = deque()

        for item in sorted_items:
            if len(chosen) < self.fanout:
                item.sends += 1
                chosen.append(item.record)
                if item.sends < limit:
                    updated.append(item)
                # else: drop -- sent enough times
            else:
                updated.append(item)

        self._queue = updated
        return chosen

    def pending_count(self) -> int:
        """How many updates are still waiting to be fully disseminated."""
        return len(self._queue)
