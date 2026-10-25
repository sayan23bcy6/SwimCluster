"""
metrics.py – Prometheus metrics exposed on /metrics.

We use the official prometheus_client library.  All counters, gauges, and
histograms are registered as module-level singletons so any module can
import and increment them without dependency injection.

The HTTP handler for /metrics is wired into the aiohttp server in node.py.
"""

from __future__ import annotations

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    REGISTRY,
)

# ── Counters ──────────────────────────────────────────────────────────────────

probes_sent = Counter(
    "swimcluster_probes_sent_total",
    "Total number of direct PING probes sent",
)

probes_failed = Counter(
    "swimcluster_probes_failed_total",
    "Probes that received no ACK within the timeout",
)

indirect_probes_sent = Counter(
    "swimcluster_indirect_probes_sent_total",
    "Total number of PING_REQ messages sent to helpers",
)

suspect_events = Counter(
    "swimcluster_suspect_events_total",
    "Number of times a node was marked SUSPECT",
)

dead_events = Counter(
    "swimcluster_dead_events_total",
    "Number of times a node was declared DEAD",
)

refutations = Counter(
    "swimcluster_refutations_total",
    "Number of ALIVE refutations broadcast (node defended itself)",
)

messages_dropped_malformed = Counter(
    "swimcluster_messages_dropped_malformed_total",
    "Messages dropped due to validation failure",
)

# ── Gauges ────────────────────────────────────────────────────────────────────

members_alive = Gauge(
    "swimcluster_members_alive",
    "Current number of ALIVE cluster members",
)

members_suspect = Gauge(
    "swimcluster_members_suspect",
    "Current number of SUSPECT cluster members",
)

members_dead = Gauge(
    "swimcluster_members_dead",
    "Current number of DEAD cluster members",
)

# ── Histograms ────────────────────────────────────────────────────────────────

convergence_time_seconds = Histogram(
    "swimcluster_convergence_time_seconds",
    "Time from a node failure to all live nodes marking it DEAD",
    buckets=(0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def update_member_gauges(alive: int, suspect: int, dead: int) -> None:
    """Convenience wrapper to update all three membership gauges at once."""
    members_alive.set(alive)
    members_suspect.set(suspect)
    members_dead.set(dead)


def scrape() -> tuple[bytes, str]:
    """Return (body, content_type) suitable for an HTTP /metrics response."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
