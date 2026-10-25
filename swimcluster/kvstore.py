"""
kvstore.py – Distributed key-value service using consistent hashing.

Consistent hashing maps keys to cluster members by:
  1. Hashing each member's node_id to K virtual positions on a ring.
  2. Hashing each key to a position on the same ring.
  3. Walking clockwise to find the first virtual node whose position ≥ key hash.

When membership changes (a node joins, dies, or becomes alive again), the
ring is rebuilt and keys that now map to a different node are flagged.
Each node owns the full key-value data but only considers itself the
"primary" for keys that hash to it.

HTTP endpoints (handled in node.py):
  GET  /kv/{key}
  PUT  /kv/{key}  body: plain text value
  DELETE /kv/{key}
"""

from __future__ import annotations

import hashlib
import logging
from typing import Dict, List, Optional, Tuple

from swimcluster.membership import Member, NodeState

log = logging.getLogger(__name__)


class ConsistentHashRing:
    """
    Consistent hash ring with virtual nodes.

    Virtual nodes spread load evenly and reduce key redistribution when
    membership changes.  Each physical node is hashed to 'replicas'
    positions on the ring.
    """

    def __init__(self, replicas: int = 150) -> None:
        self._replicas = replicas
        # Sorted list of (hash_value, node_id) for binary search.
        self._ring: List[Tuple[int, str]] = []

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode(), usedforsecurity=False).hexdigest(), 16)

    def rebuild(self, members: List[Member]) -> None:
        """Rebuild the ring from the current live (ALIVE) member list."""
        self._ring = []
        for member in members:
            if member.state != NodeState.ALIVE:
                continue  # dead/suspect nodes do not own keys
            for i in range(self._replicas):
                vnode_key = f"{member.node_id}#{i}"
                h = self._hash(vnode_key)
                self._ring.append((h, member.node_id))
        self._ring.sort(key=lambda t: t[0])
        log.debug("Hash ring rebuilt with %d members, %d vnodes",
                  len(members), len(self._ring))

    def get_node(self, key: str) -> Optional[str]:
        """Return the node_id that owns 'key', or None if the ring is empty."""
        if not self._ring:
            return None
        h = self._hash(key)
        # Binary search for the first vnode ≥ h.
        lo, hi = 0, len(self._ring)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._ring[mid][0] < h:
                lo = mid + 1
            else:
                hi = mid
        # Wrap around to the first vnode if we went past the end.
        idx = lo % len(self._ring)
        return self._ring[idx][1]

    def is_empty(self) -> bool:
        return len(self._ring) == 0


class KVStore:
    """
    Local key-value store.  Consistent hashing determines which node is
    primary for each key, but every node holds all data for simplicity.
    """

    def __init__(self, local_id: str, replicas: int = 150) -> None:
        self._local_id = local_id
        self._ring     = ConsistentHashRing(replicas=replicas)
        self._data: Dict[str, str] = {}

    # ── Membership change hook ─────────────────────────────────────────────────

    def on_membership_change(self, members: List[Member]) -> None:
        """
        Called by the node whenever the membership list changes.
        Rebuilds the ring; logs any key ownership transfers.
        """
        old_owners = {k: self._ring.get_node(k) for k in self._data}
        self._ring.rebuild(members)
        for k, old_owner in old_owners.items():
            new_owner = self._ring.get_node(k)
            if old_owner != new_owner:
                log.info(
                    "KV key %r ownership: %s → %s",
                    k, old_owner, new_owner,
                )

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def get(self, key: str) -> Optional[str]:
        return self._data.get(key)

    def put(self, key: str, value: str) -> None:
        self._data[key] = value

    def delete(self, key: str) -> bool:
        """Returns True if the key existed and was deleted."""
        if key in self._data:
            del self._data[key]
            return True
        return False

    def owner(self, key: str) -> Optional[str]:
        """Return the node_id that is the primary for this key."""
        return self._ring.get_node(key)

    def is_local_owner(self, key: str) -> bool:
        """True if this node is the primary for 'key'."""
        return self.owner(key) == self._local_id

    def all_keys(self) -> List[str]:
        return list(self._data.keys())

    def snapshot(self) -> Dict[str, str]:
        return dict(self._data)
