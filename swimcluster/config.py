"""
config.py â€“ All cluster settings loaded from environment variables and CLI args.

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
    # â”€â”€ Identity â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    node_id: str = ""                   # unique identifier, e.g. "node1"
    host: str = "0.0.0.0"              # UDP/TCP bind address
    port: int = 7946                    # base port (same for UDP gossip + TCP sync)
    http_port: int = 8080               # HTTP port for KV and metrics endpoints

    # â”€â”€ Seed nodes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Comma-separated "host:port" strings the new node contacts over TCP on join.
    seeds: List[str] = field(default_factory=list)

    # â”€â”€ SWIM timing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    protocol_period: float = 1.0        # seconds between probe rounds
    ping_timeout: float = 0.5          # wait for direct ACK
    ping_req_timeout: float = 1.0      # wait for indirect ACK via helpers
    suspicion_mult: int = 3            # suspicion timeout = suspicion_mult * protocol_period
    indirect_k: int = 3                # number of helpers for PING-REQ

    # â”€â”€ Gossip â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    gossip_fanout: int = 5             # max updates piggybacked on each outgoing message
    gossip_retransmit_mult: int = 4    # drop an update after retransmit_mult * log2(N) sends

    # â”€â”€ KV store â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    kvstore_replicas: int = 150        # virtual nodes in consistent-hash ring

    # â”€â”€ Fault injection â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    fault_loss_prob: float = 0.0       # packet loss probability [0, 1]
    fault_delay_min: float = 0.0       # min artificial delay in seconds
    fault_delay_max: float = 0.0       # max artificial delay in seconds

    # â”€â”€ Agent / LLM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    gemini_api_key: str = ""           # from GEMINI_API_KEY env var
    agent_model: str = "gemini-2.0-flash-lite"

    # â”€â”€ Observability â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
        gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
        agent_model=os.getenv("SWIM_AGENT_MODEL", "gemini-2.0-flash-lite"),
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
