"""
messages.py -- Message types, JSON encode/decode, and validation.

All SWIM messages travel as UTF-8 JSON so they are human-readable in
logs and easy to inspect with tcpdump.  Validation is strict: any field
missing or of the wrong type causes the message to be silently dropped.
"""

from __future__ import annotations

import json
import time
from enum import Enum
from typing import Any, Dict, List, Optional


# -- Message type constants ----------------------------------------------------

class MsgType(str, Enum):
    PING         = "PING"          # direct probe
    ACK          = "ACK"           # acknowledgement of a probe
    PING_REQ     = "PING_REQ"     # ask a helper to probe on our behalf
    ALIVE        = "ALIVE"         # refutation broadcast by a suspected node
    SUSPECT      = "SUSPECT"       # node is suspected dead
    DEAD         = "DEAD"          # node confirmed dead
    JOIN         = "JOIN"          # new node requests full membership list (TCP)
    JOIN_ACK     = "JOIN_ACK"     # seed replies with the full membership list
    SYNC         = "SYNC"          # periodic full-state sync (TCP)
    FAULT_SET    = "FAULT_SET"    # admin: set fault-injection parameters


# Set of valid type strings for fast O(1) lookup in validate().
_VALID_TYPES: frozenset = frozenset(t.value for t in MsgType)


# -- Wire format ---------------------------------------------------------------

def encode(msg: Dict[str, Any]) -> bytes:
    """Serialise a message dict to UTF-8 JSON bytes."""
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")


def decode(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Deserialise raw bytes to a message dict.
    Returns None if the data is not valid JSON -- caller must drop the message.
    """
    try:
        msg = json.loads(data.decode("utf-8"))
        if not isinstance(msg, dict):
            return None
        return msg
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


# -- Message constructors ------------------------------------------------------

def _base(sender_id: str, seq: int, msg_type: MsgType) -> Dict[str, Any]:
    """Common header fields present in every message."""
    return {
        "type": msg_type.value,
        "sender": sender_id,
        "seq": seq,
        "ts": time.time(),  # wall-clock timestamp for diagnostics
    }


def ping(sender_id: str, seq: int, target_id: str,
         gossip: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """PING asks the target to acknowledge that it is alive."""
    m = _base(sender_id, seq, MsgType.PING)
    m["target"] = target_id
    if gossip:
        m["gossip"] = gossip
    return m


def ack(sender_id: str, seq: int, target_id: str,
        gossip: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """ACK is the reply to a PING or to a PING_REQ result relay."""
    m = _base(sender_id, seq, MsgType.ACK)
    m["target"] = target_id
    if gossip:
        m["gossip"] = gossip
    return m


def ping_req(sender_id: str, seq: int, target_id: str,
             gossip: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    PING_REQ asks a helper node to send a PING to the target and relay
    the ACK back.  Carrying the original sequence number lets the
    originator correlate the response.
    """
    m = _base(sender_id, seq, MsgType.PING_REQ)
    m["target"] = target_id
    if gossip:
        m["gossip"] = gossip
    return m


def alive(sender_id: str, seq: int, node_id: str,
          incarnation: int, gossip: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    ALIVE is broadcast by a node that sees itself marked SUSPECT.
    It increments its incarnation number to override the suspicion.
    """
    m = _base(sender_id, seq, MsgType.ALIVE)
    m["node_id"] = node_id
    m["incarnation"] = incarnation
    if gossip:
        m["gossip"] = gossip
    return m


def suspect(sender_id: str, seq: int, node_id: str,
            incarnation: int, gossip: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    SUSPECT is gossiped when a node fails to respond to both direct and
    indirect probes.  The incarnation number anchors the suspicion so
    a later ALIVE with a higher incarnation can refute it.
    """
    m = _base(sender_id, seq, MsgType.SUSPECT)
    m["node_id"] = node_id
    m["incarnation"] = incarnation
    if gossip:
        m["gossip"] = gossip
    return m


def dead(sender_id: str, seq: int, node_id: str,
         incarnation: int, gossip: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    DEAD is gossiped after the suspicion timer expires without refutation.
    Once DEAD, the node is removed from the membership list.
    """
    m = _base(sender_id, seq, MsgType.DEAD)
    m["node_id"] = node_id
    m["incarnation"] = incarnation
    if gossip:
        m["gossip"] = gossip
    return m


def join(sender_id: str, seq: int,
         host: str, port: int) -> Dict[str, Any]:
    """JOIN is sent over TCP by a new node asking for the full member list."""
    m = _base(sender_id, seq, MsgType.JOIN)
    m["host"] = host
    m["port"] = port
    return m


def join_ack(sender_id: str, seq: int,
             members: List[Dict]) -> Dict[str, Any]:
    """JOIN_ACK carries the full membership snapshot back to the new node."""
    m = _base(sender_id, seq, MsgType.JOIN_ACK)
    m["members"] = members
    return m


def fault_set(sender_id: str, seq: int, params: Dict[str, Any]) -> Dict[str, Any]:
    """FAULT_SET is sent to the admin HTTP endpoint to configure fault injection."""
    m = _base(sender_id, seq, MsgType.FAULT_SET)
    m["params"] = params
    return m


# -- Validation ----------------------------------------------------------------

# Required fields for each message type; values are (field_name, expected_type).
_REQUIRED: Dict[str, List[tuple]] = {
    MsgType.PING.value:      [("target", str)],
    MsgType.ACK.value:       [("target", str)],
    MsgType.PING_REQ.value:  [("target", str)],
    MsgType.ALIVE.value:     [("node_id", str), ("incarnation", int)],
    MsgType.SUSPECT.value:   [("node_id", str), ("incarnation", int)],
    MsgType.DEAD.value:      [("node_id", str), ("incarnation", int)],
    MsgType.JOIN.value:      [("host", str), ("port", int)],
    MsgType.JOIN_ACK.value:  [("members", list)],
    MsgType.SYNC.value:      [("members", list)],
    MsgType.FAULT_SET.value: [("params", dict)],
}

_BASE_FIELDS: List[tuple] = [
    ("type", str),
    ("sender", str),
    ("seq", int),
]

# Valid gossip record state values.
_VALID_GOSSIP_STATES: frozenset = frozenset({"ALIVE", "SUSPECT", "DEAD"})


def validate(msg: Dict[str, Any]) -> bool:
    """
    Return True only when every required field is present and has the correct
    type.  Returning False signals the caller to drop the message silently.

    Also validates that the message type is one of the known MsgType values
    (item 10).
    """
    if not isinstance(msg, dict):
        return False

    # Check base fields
    for field_name, expected_type in _BASE_FIELDS:
        val = msg.get(field_name)
        if val is None or not isinstance(val, expected_type):
            return False

    # Reject unknown message types (item 10).
    msg_type = msg.get("type")
    if msg_type not in _VALID_TYPES:
        return False

    type_fields = _REQUIRED.get(msg_type, [])

    for field_name, expected_type in type_fields:
        val = msg.get(field_name)
        if val is None or not isinstance(val, expected_type):
            return False

    return True


def validate_gossip_records(gossip: list) -> List[Dict[str, Any]]:
    """
    Validate a gossip payload list, returning only well-formed records.

    Each record must be a dict with:
      id    (str)
      host  (str)
      port  (int)
      state (one of ALIVE/SUSPECT/DEAD)
      inc   (int)

    Malformed records are dropped individually (item 10).
    """
    valid = []
    if not isinstance(gossip, list):
        return valid
    for rec in gossip:
        if not isinstance(rec, dict):
            continue
        try:
            if (isinstance(rec.get("id"), str) and
                    isinstance(rec.get("host"), str) and
                    isinstance(rec.get("port"), int) and
                    rec.get("state") in _VALID_GOSSIP_STATES and
                    isinstance(rec.get("inc"), int)):
                valid.append(rec)
        except Exception:
            continue
    return valid


# -- Membership update record (used in gossip payloads) ------------------------

def member_record(node_id: str, host: str, port: int,
                  state: str, incarnation: int) -> Dict[str, Any]:
    """Compact membership record embedded in gossip payloads."""
    return {
        "id": node_id,
        "host": host,
        "port": port,
        "state": state,
        "inc": incarnation,
    }
