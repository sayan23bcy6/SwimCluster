"""
test_messages.py – Unit tests for message encoding, decoding, and validation.
"""

import pytest
from swimcluster import messages
from swimcluster.messages import MsgType, decode, encode, validate


def test_encode_decode_roundtrip():
    """encode → decode should reproduce the original dict."""
    msg = messages.ping("node1", 42, "node2")
    data = encode(msg)
    result = decode(data)
    assert result == msg


def test_decode_invalid_json_returns_none():
    """Non-JSON bytes should return None without raising."""
    assert decode(b"not json at all") is None
    assert decode(b"") is None
    assert decode(b"[1, 2, 3]") is None   # list, not dict


def test_decode_invalid_utf8_returns_none():
    assert decode(b"\xff\xfe") is None


def test_validate_ping_valid():
    msg = messages.ping("node1", 1, "node2")
    assert validate(msg) is True


def test_validate_ack_valid():
    msg = messages.ack("node1", 1, "node2")
    assert validate(msg) is True


def test_validate_ping_req_valid():
    msg = messages.ping_req("node1", 1, "node2")
    assert validate(msg) is True


def test_validate_alive_valid():
    msg = messages.alive("node1", 1, "node2", incarnation=3)
    assert validate(msg) is True


def test_validate_suspect_valid():
    msg = messages.suspect("node1", 1, "node2", incarnation=2)
    assert validate(msg) is True


def test_validate_dead_valid():
    msg = messages.dead("node1", 1, "node2", incarnation=2)
    assert validate(msg) is True


def test_validate_join_valid():
    msg = messages.join("node1", 1, "127.0.0.1", 7946)
    assert validate(msg) is True


def test_validate_join_ack_valid():
    msg = messages.join_ack("node1", 1, [])
    assert validate(msg) is True


def test_validate_missing_type_fails():
    msg = {"sender": "node1", "seq": 1, "target": "node2"}
    assert validate(msg) is False


def test_validate_wrong_type_for_field_fails():
    msg = messages.ping("node1", 1, "node2")
    msg["seq"] = "not-an-int"  # seq must be int
    assert validate(msg) is False


def test_validate_missing_required_field_fails():
    msg = messages.alive("node1", 1, "node2", incarnation=1)
    del msg["incarnation"]
    assert validate(msg) is False


def test_validate_not_a_dict_fails():
    assert validate([1, 2]) is False  # type: ignore[arg-type]
    assert validate(None) is False    # type: ignore[arg-type]


def test_gossip_embedded_in_ping():
    """Gossip payload should survive encode/decode."""
    gossip = [messages.member_record("n2", "127.0.0.1", 7947, "ALIVE", 0)]
    msg  = messages.ping("node1", 1, "node2", gossip=gossip)
    data = encode(msg)
    out  = decode(data)
    assert out["gossip"][0]["id"] == "n2"


def test_member_record_fields():
    rec = messages.member_record("n1", "10.0.0.1", 7946, "SUSPECT", 5)
    assert rec == {"id": "n1", "host": "10.0.0.1", "port": 7946,
                   "state": "SUSPECT", "inc": 5}
