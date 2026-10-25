"""
config.py – All cluster settings loaded from environment variables and CLI args.

Each setting has a reasonable default so a node can start without any
configuration.  Config is a plain dataclass so it can be passed around
without hidden global state.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Config:
    # ── Identity ──────────────────────────────────────────────────────────────
    node_id: str = ""                   # unique identifier, e.g. "node1"
    host: str = "0.0.0.0"              # UDP/TCP bind address
    port: int = 7946                    # base port (same for UDP gossip + TCP sync)
    http_port: int = 8080               # HTTP port for KV and metrics endpoints

    # ── Seed nodes ────────────────────────────────────────────────────────────
    # Comma-separated "host:port" strings the new node contacts over TCP on join.
    seeds: List[str] = field(default_factory=list)

    # ── SWIM timing ───────────────────────────────────────────────────────────
    protocol_period: float = 1.0        # seconds between probe rounds
    ping_timeout: float = 0.5          # wait for direct ACK
    ping_req_timeout: float = 1.0      # wait for indirect ACK via helpers
    suspicion_mult: int = 3            # suspicion timeout = suspicion_mult * protocol_period
    indirect_k: int = 3                # number of helpers for PING-REQ

    # ── Gossip ────────────────────────────────────────────────────────────────
    gossip_fanout: int = 5             # max updates piggybacked on each outgoing message
    gossip_retransmit_mult: int = 4    # drop an update after retransmit_mult * log2(N) sends

    # ── KV store ──────────────────────────────────────────────────────────────
    kvstore_replicas: int = 150        # virtual nodes in consistent-hash ring

    # ── Fault injection ───────────────────────────────────────────────────────
    fault_loss_prob: float = 0.0       # packet loss probability [0, 1]
    fault_delay_min: float = 0.0       # min artificial delay in seconds
    fault_delay_max: float = 0.0       # max artificial delay in seconds

    # ── Agent / LLM ───────────────────────────────────────────────────────────
    anthropic_api_key: str = ""        # from ANTHROPIC_API_KEY env var
    agent_model: str = "claude-opus-4-5"

    # ── Observability ─────────────────────────────────────────────────────────
    metrics_path: str = "/metrics"
    event_log_path: str = "events.jsonl"  # append-only JSON-lines event log


def from_env(overrides: Optional[dict] = None) -> Config:
    """
    Build a Config from environment variables, applying any dict overrides last.
    Environment variables use the SWIM_ prefix, e.g. SWIM_PORT=7946.
    """
    cfg = Config(
        node_id=os.getenv("SWIM_NODE_ID", ""),
        host=os.getenv("SWIM_HOST", "0.0.0.0"),
        port=int(os.getenv("SWIM_PORT", "7946")),
        http_port=int(os.getenv("SWIM_HTTP_PORT", "8080")),
        seeds=_parse_seeds(os.getenv("SWIM_SEEDS", "")),
        protocol_period=float(os.getenv("SWIM_PROTOCOL_PERIOD", "1.0")),
        ping_timeout=float(os.getenv("SWIM_PING_TIMEOUT", "0.5")),
        ping_req_timeout=float(os.getenv("SWIM_PING_REQ_TIMEOUT", "1.0")),
        suspicion_mult=int(os.getenv("SWIM_SUSPICION_MULT", "3")),
        indirect_k=int(os.getenv("SWIM_INDIRECT_K", "3")),
        gossip_fanout=int(os.getenv("SWIM_GOSSIP_FANOUT", "5")),
        gossip_retransmit_mult=int(os.getenv("SWIM_GOSSIP_RETRANSMIT_MULT", "4")),
        kvstore_replicas=int(os.getenv("SWIM_KVSTORE_REPLICAS", "150")),
        fault_loss_prob=float(os.getenv("SWIM_FAULT_LOSS_PROB", "0.0")),
        fault_delay_min=float(os.getenv("SWIM_FAULT_DELAY_MIN", "0.0")),
        fault_delay_max=float(os.getenv("SWIM_FAULT_DELAY_MAX", "0.0")),
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
        agent_model=os.getenv("SWIM_AGENT_MODEL", "claude-opus-4-5"),
        event_log_path=os.getenv("SWIM_EVENT_LOG", "events.jsonl"),
    )
    if overrides:
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
    return cfg


def _parse_seeds(raw: str) -> List[str]:
    """Split a comma-separated seed string into a clean list."""
    return [s.strip() for s in raw.split(",") if s.strip()]
