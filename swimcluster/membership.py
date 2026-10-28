"""
membership.py – Member state machine and conflict resolution.

Each node in the cluster is represented by a Member object.  State
transitions follow the SWIM ordering rules:

    ALIVE  →  SUSPECT  →  DEAD
                ↑
          (refutation resets to ALIVE with higher incarnation)

Conflict resolution uses two rules applied in order:
  1. Higher incarnation number always wins.
  2. At equal incarnation: DEAD > SUSPECT > ALIVE.

This means a node can only escape SUSPECT/DEAD by proving it is alive
at a strictly higher incarnation number.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class NodeState(str, Enum):
    ALIVE   = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD    = "DEAD"


# Numeric rank used for tie-breaking at equal incarnation numbers.
# Higher rank beats lower rank.
_STATE_RANK: Dict[NodeState, int] = {
    NodeState.ALIVE:   0,
    NodeState.SUSPECT: 1,
    NodeState.DEAD:    2,
}


@dataclass
class Member:
    """Represents one cluster member and all information known about it."""
    node_id:     str
    host:        str
    port:        int
    state:       NodeState = NodeState.ALIVE
    incarnation: int       = 0

    # Wall-clock time when this member was last updated locally.
    # Used to decide when to expire a suspect timer.
    updated_at:  float     = field(default_factory=time.time)

    def addr(self) -> tuple[str, int]:
        """Convenience: (host, port) tuple for socket calls."""
        return (self.host, self.port)

    def to_dict(self) -> dict:
        """Serialise to a plain dict for wire transport."""
        return {
            "id":          self.node_id,
            "host":        self.host,
            "port":        self.port,
            "state":       self.state.value,
            "incarnation": self.incarnation,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Member":
        """Deserialise from a plain dict received over the wire."""
        return cls(
            node_id=d["id"],
            host=d["host"],
            port=d["port"],
            state=NodeState(d["state"]),
            incarnation=d["incarnation"],
        )


def merge_member(existing: Optional[Member], incoming: Member) -> tuple[Member, bool]:
    """
    Apply SWIM conflict resolution to decide which state to keep.

    Returns (winning_member, changed) where 'changed' is True when the
    existing record was actually updated.  The returned object is always
    the same instance as 'existing' (mutated in place) when it wins,
    or 'incoming' when the remote update wins.
    """
    if existing is None:
        # Brand-new member we have never seen before.
        return incoming, True

    # Rule 1: higher incarnation always wins.
    if incoming.incarnation > existing.incarnation:
        existing.state       = incoming.state
        existing.incarnation = incoming.incarnation
        existing.updated_at  = time.time()
        return existing, True

    if incoming.incarnation < existing.incarnation:
        # Stale update – ignore it.
        return existing, False

    # Rule 2: same incarnation – higher state rank wins.
    incoming_rank = _STATE_RANK[incoming.state]
    existing_rank = _STATE_RANK[existing.state]

    if incoming_rank > existing_rank:
        existing.state      = incoming.state
        existing.updated_at = time.time()
        return existing, True

    return existing, False  # existing state is dominant or equal


class MembershipList:
    """
    Thread-safe-ish membership list.  asyncio is single-threaded so we
    don't need locks, but we still avoid mutating while iterating.
    """

    def __init__(self, local_id: str) -> None:
        self.local_id = local_id
        self._members: Dict[str, Member] = {}

    # ── Mutation ──────────────────────────────────────────────────────────────

    def upsert(self, incoming: Member) -> bool:
        """
        Merge 'incoming' into the list using SWIM ordering rules.
        Returns True if the local view changed (useful for gossip scheduling).
        """
        existing = self._members.get(incoming.node_id)
        winner, changed = merge_member(existing, incoming)
        self._members[incoming.node_id] = winner
        return changed

    def mark_suspect(self, node_id: str, incarnation: int) -> bool:
        """Transition a member to SUSPECT at the given incarnation."""
        m = self._members.get(node_id)
        if m is None:
            return False
        return self.upsert(Member(
            node_id=m.node_id, host=m.host, port=m.port,
            state=NodeState.SUSPECT, incarnation=incarnation,
        ))

    def mark_dead(self, node_id: str, incarnation: int) -> bool:
        """Transition a member to DEAD at the given incarnation."""
        m = self._members.get(node_id)
        if m is None:
            return False
        return self.upsert(Member(
            node_id=m.node_id, host=m.host, port=m.port,
            state=NodeState.DEAD, incarnation=incarnation,
        ))

    def mark_alive(self, node_id: str, host: str, port: int,
                   incarnation: int) -> bool:
        """Transition a member to ALIVE at the given incarnation."""
        return self.upsert(Member(
            node_id=node_id, host=host, port=port,
            state=NodeState.ALIVE, incarnation=incarnation,
        ))

    # ── Queries ───────────────────────────────────────────────────────────────

    def get(self, node_id: str) -> Optional[Member]:
        return self._members.get(node_id)

    def alive_members(self) -> List[Member]:
        """Return a snapshot of all ALIVE members (excluding local node)."""
        return [
            m for m in self._members.values()
            if m.state == NodeState.ALIVE and m.node_id != self.local_id
        ]

    def probeable_members(self) -> List[Member]:
        """ALIVE + SUSPECT members excluding self – valid probe targets."""
        return [
            m for m in self._members.values()
            if m.state in (NodeState.ALIVE, NodeState.SUSPECT)
            and m.node_id != self.local_id
        ]

    def all_members(self) -> List[Member]:
        """Full snapshot including all states."""
        return list(self._members.values())

    def counts(self) -> tuple[int, int, int]:
        """Return (alive, suspect, dead) counts including the local node."""
        a = s = d = 0
        for m in self._members.values():
            if m.state == NodeState.ALIVE:
                a += 1
            elif m.state == NodeState.SUSPECT:
                s += 1
            else:
                d += 1
        return a, s, d

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_list(self) -> List[dict]:
        """Serialise the full membership list for wire transport (JOIN_ACK, SYNC)."""
        return [m.to_dict() for m in self._members.values()]

    def load_from_list(self, records: List[dict]) -> None:
        """
        Bootstrap the membership list from a remote full-state snapshot.
        Uses merge semantics so local knowledge is never regressed.
        """
        for rec in records:
            try:
                if rec.get("id") == self.local_id:
                    continue
                m = Member.from_dict(rec)
                self.upsert(m)
            except (KeyError, ValueError):
                pass  # silently skip malformed records
