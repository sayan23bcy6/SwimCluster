# Design Notes

This document explains every major protocol decision in SwimCluster and
the tradeoffs behind each choice.

---

## 1. Why SWIM?

Traditional heartbeat protocols require each node to exchange messages
with every other node.  That is O(N²) traffic per period, which is fine
for a five-node demo but collapses at 500 nodes.

SWIM (Scalable Weakly-consistent Infection-style process group Membership,
Das et al. 2002) achieves O(N) message overhead per period by:
  - Probing **one random member** per period (not all members).
  - Spreading state via **gossip** piggybacked on existing messages.

The cost is that convergence is eventual, not instantaneous.  All live
nodes will eventually agree on who is DEAD, but it takes O(log N)
protocol periods.

---

## 2. Direct Probe vs. Indirect Probe

**Direct probe** (PING → ACK):  
The simplest form of failure detection.  Drawback: a false failure is
declared whenever the probe packet is lost by the network, not the target.

**Indirect probe** (PING_REQ to k helpers):  
When the direct probe times out, the node asks k=3 helpers to each
independently probe the target.  If at least one helper succeeds, the
target is alive.  This reduces false positives dramatically: all k+1
paths would have to fail simultaneously for a false negative.

Tradeoff: indirect probing increases message volume by up to k per
period.  SwimCluster defaults to k=3, the value the original SWIM paper
recommends for robustness vs. overhead.

---

## 3. Incarnation Numbers and Refutation

Every member has an incarnation number starting at 0.  When a node is
suspected, the suspicion carries that number.  The suspected node
**refutes** by incrementing its incarnation and broadcasting ALIVE with
the new number.

Conflict resolution is total:
  1. Higher incarnation always wins.
  2. At equal incarnation: DEAD > SUSPECT > ALIVE.

This makes state merging idempotent and commutative: it does not matter
in what order gossip updates arrive; the result is always the same.

Why not use a simple timestamp?  Clocks drift.  Incarnation numbers are
logical, not wall-clock, so they are safe across restarts and clock skew.

---

## 4. Piggybacked Gossip

Dedicated gossip messages would double the message volume.  Instead,
SwimCluster piggybacks membership updates onto PINGs, ACKs, and PING_REQs
that are already going out.

Each update is retransmitted at most `retransmit_mult × ⌈log₂(N+1)⌉`
times.  After that it is dropped from the queue.  This bounds the total
number of transmissions per event to O(log N) per node, giving
O(N log N) total messages for a single event across the cluster.

Tradeoff: if a node is partitioned or slow, it may miss updates.  This
is acceptable because the suspicion/dead cycle will eventually converge
even without gossip, through repeated probing.

---

## 5. UDP for Probes, TCP for State Sync

**UDP** is connectionless and has zero per-message overhead.  SWIM probes
are small (<1 kB) and the protocol already handles loss through timeouts.
Using TCP here would add RTT overhead for connection setup on every probe.

**TCP** is used only for the JOIN handshake, where:
  - Reliability matters (new node must get a complete membership snapshot).
  - The payload can be large (full member list for N nodes).
  - The operation happens once, not every second.

Alternative considered: use UDP + manual sequence numbers and ACKs for
JOIN.  Rejected because it would replicate TCP badly, and this is
exactly what TCP is for.

---

## 6. Suspect Timer

Marking a node immediately DEAD after a failed probe is too aggressive.
The network could be temporarily congested, or the probe packet could
have been dropped.  A suspicion period gives the node time to refute.

The timer is `suspicion_mult × protocol_period`.  With the default of
3 × 1 s = 3 s, a node that stops responding has 3 seconds to send an
ALIVE before it is declared dead.  The paper recommends `log(N)` as a
base, but a fixed multiplier is simpler and works well for small-to-medium
clusters.

---

## 7. Consistent Hashing for the KV Service

Consistent hashing minimises key remapping when membership changes.  With
a naive modulo hash (`key % N`), adding or removing a node remaps O(N/K)
keys for N keys and K nodes — catastrophic in large clusters.

With consistent hashing, adding or removing one node remaps only `N/K`
keys on average, and virtual nodes (150 per physical node by default)
spread load evenly.

Each node stores all keys locally.  The ring determines ownership for
routing decisions (which node is primary), not data replication.  A
production system would add replica placement; that complexity is omitted
for clarity.

---

## 8. Fault Injection Design

Fault injection is a wrapper around the transport, not a monkey-patch of
the protocol.  This means:
  - Protocol logic is unmodified.
  - Faults can be enabled/disabled at runtime via the HTTP admin endpoint.
  - Tests can inject faults deterministically without sleep().

The FaultLayer is the last thing called before a UDP send, so it can
observe the final message content.  It implements three fault types:
  - **Packet loss**: random discard at a configured probability.
  - **Delay**: `asyncio.sleep()` before delivery, creating artificial RTT.
  - **Partition**: block all traffic between a specific pair of node IDs.

---

## 9. AI Agent Design

The agent is deliberately **read-only**.  It never calls any cluster API
that mutates state.  Rationale: an AI making autonomous cluster changes
is dangerous; a human should always approve remediation.

The LLM provider is hidden behind a `LLMProvider` interface with a single
`complete(prompt)` method.  Tests use `FakeLLMProvider` which returns a
fixed response without network calls.  The real implementation uses
`aiohttp` to call the Anthropic API — no SDK dependency.

Prompt engineering: the agent is given only a bounded time window of
event data and the current metrics snapshot.  Sending unbounded history
would be expensive and noisy; a 5-minute window captures the incident
without irrelevant older events.

---

## 10. asyncio and Single-Threaded Concurrency

All I/O, timers, and protocol state are on a single asyncio event loop.
This eliminates data-race conditions without locks.  The tradeoff is that
any blocking call (e.g., file I/O in the event log writer) would stall
the loop.  The event logger uses synchronous file writes and is called
infrequently enough that this is acceptable.  A production system would
use `loop.run_in_executor()` or an async file library.
