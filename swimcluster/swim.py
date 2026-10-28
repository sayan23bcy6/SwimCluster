"""
swim.py -- SWIM failure detector: probe, indirect probe, suspect/dead logic.

Protocol walk-through (one protocol period):
  1. Pick a random ALIVE/SUSPECT member to probe (the target).
  2. Send PING over UDP; wait up to ping_timeout for ACK.
  3. If no ACK: send PING_REQ to k random helpers (not the target itself).
     Each helper pings the target and relays the ACK.
  4. If still no ACK from any helper: mark target SUSPECT and start
     the suspicion timer (suspicion_mult x protocol_period).
  5. If the timer expires before a refutation arrives: mark DEAD and gossip.

Refutation: a node that sees itself marked SUSPECT increments its
incarnation number and broadcasts ALIVE.  This overrides the SUSPECT
state because higher incarnation always wins.

The SWIM class owns only protocol logic.  All I/O goes through the
transport abstraction so the same code runs in tests (InMemoryTransport)
and in production (RealTransport).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Dict, Optional, Set, Tuple

from swimcluster import messages, metrics
from swimcluster.config import Config
from swimcluster.gossip import GossipQueue
from swimcluster.membership import Member, MembershipList, NodeState
from swimcluster.transport import BaseTransport

log = logging.getLogger(__name__)


class SWIM:
    """
    SWIM failure-detector state machine.

    Owns:
    * The membership list (source of truth for this node's view).
    * The gossip queue (what gets piggybacked on outgoing messages).
    * Probe scheduling loop.
    * Incoming message dispatch.
    """

    def __init__(self, cfg: Config, transport: BaseTransport,
                 membership: MembershipList) -> None:
        self._cfg         = cfg
        self._transport   = transport
        self._membership  = membership
        self._gossip      = GossipQueue(cfg.gossip_fanout, cfg.gossip_retransmit_mult)

        # Sequence counter -- monotonically increasing per sender.
        self._seq         = 0

        # Pending direct pings: seq -> asyncio.Event (set when ACK arrives).
        self._pending_pings: Dict[int, asyncio.Event] = {}

        # Pending indirect pings: seq -> asyncio.Event.
        self._pending_indirect: Dict[int, asyncio.Event] = {}

        # Active suspicion timers: node_id -> asyncio.Task.
        self._suspect_timers: Dict[str, asyncio.Task] = {}

        # Convergence tracking: node_id -> time when first suspected.
        self._suspect_start: Dict[str, float] = {}

        # Running flag.
        self._running = False

        # Local member's current incarnation (for self-refutation).
        # Initialize to max(0, current unix time) so a restarted node always
        # has a higher incarnation than its previous run (item B.5).
        self._incarnation = max(0, int(time.time()))

        # Probing round-robin shuffle.
        self._probe_index = 0

        # Event log callback (set by node.py).
        self._event_log_cb = None

    # -- Lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        asyncio.get_event_loop().create_task(self._probe_loop())
        log.info("SWIM failure detector started for %s", self._cfg.node_id)

    async def stop(self) -> None:
        self._running = False
        for t in self._suspect_timers.values():
            t.cancel()

    def set_event_log(self, cb) -> None:
        """Register a callback(event_dict) for structured event logging."""
        self._event_log_cb = cb

    # -- Probe loop ------------------------------------------------------------

    async def _probe_loop(self) -> None:
        """Main SWIM protocol loop: one probe per protocol period."""
        while self._running:
            period_start = asyncio.get_event_loop().time()
            await self._do_probe()
            elapsed = asyncio.get_event_loop().time() - period_start
            sleep   = max(0.0, self._cfg.protocol_period - elapsed)
            await asyncio.sleep(sleep)

    async def _do_probe(self) -> None:
        """
        Single probe round.  Pick a random member, ping it, escalate to
        PING_REQ if needed, then mark SUSPECT/DEAD if still silent.
        """
        targets = self._membership.probeable_members()
        if not targets:
            return

        # Round-robin through a shuffled list for even coverage.
        target = random.choice(targets)

        seq = self._next_seq()
        event = asyncio.Event()
        self._pending_pings[seq] = event

        metrics.probes_sent.inc()
        log.debug("Probing %s (seq=%d)", target.node_id, seq)

        gossip_payload = self._gossip.get_updates(len(targets) + 1)
        msg = messages.ping(self._cfg.node_id, seq, target.node_id, gossip_payload)
        await self._transport.send_udp(msg, target.addr(),
                                       target_node_id=target.node_id)

        # Wait for direct ACK.
        try:
            await asyncio.wait_for(event.wait(), self._cfg.ping_timeout)
            return  # Got ACK -- target is alive.
        except asyncio.TimeoutError:
            pass
        finally:
            self._pending_pings.pop(seq, None)

        # -- Indirect probe ----------------------------------------------------
        metrics.probes_failed.inc()
        helpers = [
            m for m in targets
            if m.node_id != target.node_id
        ][:self._cfg.indirect_k]

        if helpers:
            indirect_seq = self._next_seq()
            indirect_event = asyncio.Event()
            self._pending_indirect[indirect_seq] = indirect_event

            metrics.indirect_probes_sent.inc(len(helpers))
            for helper in helpers:
                gp = self._gossip.get_updates(len(targets) + 1)
                preq = messages.ping_req(
                    self._cfg.node_id, indirect_seq, target.node_id, gp
                )
                await self._transport.send_udp(preq, helper.addr(),
                                               target_node_id=helper.node_id)

            try:
                await asyncio.wait_for(indirect_event.wait(),
                                       self._cfg.ping_req_timeout)
                return  # A helper got through -- target is alive.
            except asyncio.TimeoutError:
                pass
            finally:
                self._pending_indirect.pop(indirect_seq, None)

        # -- Suspect the target ------------------------------------------------
        self._suspect_node(target)

    # -- Suspect / Dead transitions --------------------------------------------

    def _suspect_node(self, member: Member) -> None:
        """Transition a member to SUSPECT and start the suspicion timer.

        Skips only if the member is currently SUSPECT *and* a timer already
        exists (item B.4: re-suspicion must be allowed after refutation).
        """
        if member.state == NodeState.DEAD:
            return
        # Only skip if already suspected with an active timer.
        if member.state == NodeState.SUSPECT and member.node_id in self._suspect_timers:
            return

        changed = self._membership.mark_suspect(
            member.node_id, member.incarnation
        )
        if not changed:
            return

        metrics.suspect_events.inc()
        self._suspect_start[member.node_id] = time.time()
        log.warning("Node %s is SUSPECT (incarnation=%d)",
                    member.node_id, member.incarnation)
        self._log_event("SUSPECT", member.node_id, member.incarnation)

        # Gossip the SUSPECT state.
        self._enqueue_gossip(member)
        self._update_member_gauges()

        # Start the suspicion timer.
        timeout = self._cfg.suspicion_mult * self._cfg.protocol_period
        task = asyncio.get_event_loop().create_task(
            self._suspicion_timer(member.node_id, member.incarnation, timeout)
        )
        self._suspect_timers[member.node_id] = task

    async def _suspicion_timer(self, node_id: str,
                               incarnation: int, timeout: float) -> None:
        """
        Wait for the suspicion period; if no refutation arrives, mark DEAD.
        """
        await asyncio.sleep(timeout)
        m = self._membership.get(node_id)
        if m is None or m.state == NodeState.DEAD:
            self._suspect_timers.pop(node_id, None)
            return
        # If the node refuted (incarnation bumped), do not mark dead.
        if m.incarnation > incarnation:
            log.info("Node %s refuted suspicion -- not marking dead", node_id)
            self._suspect_timers.pop(node_id, None)
            return

        self._mark_dead(node_id, incarnation)

    def _mark_dead(self, node_id: str, incarnation: int) -> None:
        """Declare a node DEAD and gossip the state change."""
        changed = self._membership.mark_dead(node_id, incarnation)
        if not changed:
            return

        metrics.dead_events.inc()
        elapsed = time.time() - self._suspect_start.pop(node_id, time.time())
        metrics.convergence_time_seconds.observe(elapsed)

        # Cancel and remove suspicion timer (item B.4).
        if t := self._suspect_timers.pop(node_id, None):
            t.cancel()

        log.error("Node %s is DEAD (incarnation=%d, convergence=%.2fs)",
                  node_id, incarnation, elapsed)
        self._log_event("DEAD", node_id, incarnation)

        m = self._membership.get(node_id)
        if m:
            self._enqueue_gossip(m)
        self._update_member_gauges()

    # -- Incoming message handler ----------------------------------------------

    async def handle_message(self, msg: Dict[str, Any],
                             addr: Tuple[str, int]) -> Optional[Dict[str, Any]]:
        """
        Dispatch an incoming, already-validated message.
        Returns a reply dict if one should be sent, else None.
        """
        mtype = msg.get("type")

        # Always process piggybacked gossip first.
        if gossip := msg.get("gossip"):
            self._apply_gossip(gossip)

        if mtype == messages.MsgType.PING.value:
            return await self._on_ping(msg, addr)

        elif mtype == messages.MsgType.ACK.value:
            self._on_ack(msg)

        elif mtype == messages.MsgType.PING_REQ.value:
            await self._on_ping_req(msg, addr)

        elif mtype == messages.MsgType.ALIVE.value:
            self._on_alive(msg)

        elif mtype == messages.MsgType.SUSPECT.value:
            self._on_suspect(msg)

        elif mtype == messages.MsgType.DEAD.value:
            self._on_dead(msg)

        elif mtype == messages.MsgType.JOIN.value:
            return self._on_join(msg)

        elif mtype == messages.MsgType.JOIN_ACK.value:
            self._on_join_ack(msg)

        return None

    async def _on_ping(self, msg: Dict, addr: Tuple[str, int]) -> Dict:
        """Respond to a PING with an ACK."""
        gp = self._gossip.get_updates(
            len(self._membership.probeable_members()) + 1
        )
        reply = messages.ack(
            self._cfg.node_id, msg["seq"], msg.get("target", ""), gp
        )
        # If this ping was addressed to us and we're suspected, refute.
        if msg.get("target") == self._cfg.node_id:
            await self._maybe_refute()
        return reply

    def _on_ack(self, msg: Dict) -> None:
        """Signal any waiting probe that an ACK arrived."""
        seq = msg.get("seq")
        # Direct ping ACK.
        if event := self._pending_pings.get(seq):
            event.set()
        # Indirect ping-req ACK relay.
        if event := self._pending_indirect.get(seq):
            event.set()

    async def _on_ping_req(self, msg: Dict, addr: Tuple[str, int]) -> None:
        """
        A helper receives PING_REQ: probe the target and relay the result.
        The sequence number of the original sender is preserved in the
        relay ACK so the originator can correlate it.
        """
        target_id   = msg.get("target")
        original_seq = msg.get("seq")
        target_m    = self._membership.get(target_id)
        if target_m is None:
            return

        seq = self._next_seq()
        event = asyncio.Event()
        self._pending_pings[seq] = event

        gp  = self._gossip.get_updates(len(self._membership.probeable_members()) + 1)
        ping_m = messages.ping(self._cfg.node_id, seq, target_id, gp)
        await self._transport.send_udp(ping_m, target_m.addr(),
                                       target_node_id=target_m.node_id)

        # Sender node_id for the relay ACK.
        sender_id = msg.get("sender", "")
        sender_m  = self._membership.get(sender_id)

        try:
            await asyncio.wait_for(event.wait(), self._cfg.ping_timeout)
            # Relay success back to the original sender using their seq.
            gp2  = self._gossip.get_updates(len(self._membership.probeable_members()) + 1)
            relay = messages.ack(self._cfg.node_id, original_seq, target_id, gp2)
            # Send relay to the ACK recipient (original sender).
            relay_node_id = sender_id if sender_id else ""
            await self._transport.send_udp(relay, addr,
                                           target_node_id=relay_node_id)
        except asyncio.TimeoutError:
            pass  # Silence -- originator will timeout on its own.
        finally:
            self._pending_pings.pop(seq, None)

    def _on_alive(self, msg: Dict) -> None:
        """Process an ALIVE refutation message."""
        node_id     = msg["node_id"]
        incarnation = msg["incarnation"]

        if node_id == self._cfg.node_id:
            return  # Our own broadcast echoed back -- ignore.

        m = self._membership.get(node_id)
        host = m.host if m else msg.get("sender", "")
        port = m.port if m else 0
        changed = self._membership.mark_alive(node_id, host, port, incarnation)
        if changed:
            # Cancel and remove any outstanding suspicion timer (item B.4).
            if t := self._suspect_timers.pop(node_id, None):
                t.cancel()
            self._suspect_start.pop(node_id, None)
            self._enqueue_gossip(self._membership.get(node_id))
            self._update_member_gauges()

    def _on_suspect(self, msg: Dict) -> None:
        """Process a SUSPECT gossip event; refute if we are the target."""
        node_id     = msg["node_id"]
        incarnation = msg["incarnation"]

        if node_id == self._cfg.node_id:
            # Someone suspects us -- schedule a refutation.
            asyncio.get_event_loop().create_task(self._self_refute(incarnation))
            return

        m = self._membership.get(node_id)
        if m is None:
            return
        changed = self._membership.mark_suspect(node_id, incarnation)
        if changed:
            metrics.suspect_events.inc()
            if node_id not in self._suspect_timers:
                self._suspect_start[node_id] = time.time()
                timeout = self._cfg.suspicion_mult * self._cfg.protocol_period
                task = asyncio.get_event_loop().create_task(
                    self._suspicion_timer(node_id, incarnation, timeout)
                )
                self._suspect_timers[node_id] = task
            self._enqueue_gossip(self._membership.get(node_id))
            self._update_member_gauges()

    def _on_dead(self, msg: Dict) -> None:
        """Accept a DEAD declaration from another node."""
        node_id     = msg["node_id"]
        incarnation = msg["incarnation"]
        if node_id == self._cfg.node_id:
            return  # We cannot accept our own death (we're still running).
        m = self._membership.get(node_id)
        if m and m.state != NodeState.DEAD:
            self._mark_dead(node_id, incarnation)

    def _on_join(self, msg: Dict) -> Dict:
        """Handle a JOIN request: add the new node and return the member list."""
        sender_id = msg["sender"]
        host      = msg["host"]
        port      = msg["port"]
        self._membership.mark_alive(sender_id, host, port, 0)
        self._enqueue_gossip(self._membership.get(sender_id))
        self._update_member_gauges()
        log.info("Node %s joined from %s:%s", sender_id, host, port)
        return messages.join_ack(
            self._cfg.node_id,
            self._next_seq(),
            self._membership.to_list(),
        )

    def _on_join_ack(self, msg: Dict) -> None:
        """Bootstrap membership from the seed's full list."""
        self._membership.load_from_list(msg.get("members", []))
        self._update_member_gauges()

    # -- Gossip helpers --------------------------------------------------------

    def _apply_gossip(self, updates: list) -> None:
        """Merge piggybacked membership updates into the local view.

        If a record for the local node arrives with state SUSPECT or DEAD
        at an incarnation >= ours, trigger self-refutation (item B.6).
        Drops individual malformed records while keeping valid ones (item 10).
        """
        valid_states = {"ALIVE", "SUSPECT", "DEAD"}
        for rec in updates:
            # Validate the gossip record (item 10).
            if not isinstance(rec, dict):
                continue
            try:
                node_id_r = rec["id"]
                host_r    = rec["host"]
                port_r    = rec["port"]
                state_r   = rec["state"]
                inc_r     = rec["inc"]
            except KeyError:
                continue
            if not isinstance(node_id_r, str):
                continue
            if not isinstance(host_r, str):
                continue
            if not isinstance(port_r, int):
                continue
            if state_r not in valid_states:
                continue
            if not isinstance(inc_r, int):
                continue

            # Self-refutation trigger (item B.6).
            if node_id_r == self._cfg.node_id:
                if state_r in ("SUSPECT", "DEAD") and inc_r >= self._incarnation:
                    asyncio.get_event_loop().create_task(
                        self._self_refute(inc_r)
                    )
                continue

            try:
                m = Member.from_dict({
                    "id":          node_id_r,
                    "host":        host_r,
                    "port":        port_r,
                    "state":       state_r,
                    "incarnation": inc_r,
                })
                changed = self._membership.upsert(m)
                if changed:
                    self._enqueue_gossip(m)
                    self._update_member_gauges()
                    # Cancel suspicion timer if ALIVE record arrived (item B.4).
                    if m.state == NodeState.ALIVE:
                        if t := self._suspect_timers.pop(m.node_id, None):
                            t.cancel()
                        self._suspect_start.pop(m.node_id, None)
            except (KeyError, ValueError):
                pass

    def _enqueue_gossip(self, member: Optional[Member]) -> None:
        if member is None:
            return
        from swimcluster.messages import member_record
        rec = member_record(
            member.node_id, member.host, member.port,
            member.state.value, member.incarnation,
        )
        self._gossip.enqueue(rec)

    # -- Self-refutation -------------------------------------------------------

    async def _maybe_refute(self) -> None:
        """Check if we are suspected and refute if so."""
        m = self._membership.get(self._cfg.node_id)
        if m and m.state == NodeState.SUSPECT:
            await self._self_refute(m.incarnation)

    async def _self_refute(self, suspected_at: int) -> None:
        """
        Increment our incarnation number and broadcast ALIVE.
        Higher incarnation overrides the SUSPECT state on all peers.

        Also enqueues our own ALIVE record in gossip so it piggybacks
        on the next outgoing messages (item B.6).

        Broadcasts to ALIVE and SUSPECT members (item B.6 -- not only ALIVE).
        """
        if self._incarnation <= suspected_at:
            self._incarnation = suspected_at + 1

        metrics.refutations.inc()
        log.info("Refuting suspicion -- broadcasting ALIVE (incarnation=%d)",
                 self._incarnation)
        self._log_event("ALIVE_REFUTATION", self._cfg.node_id, self._incarnation)

        # Update our own entry.
        local_m = self._membership.get(self._cfg.node_id)
        advertise_host = self._cfg.effective_advertise_host()
        self_host = local_m.host if local_m else advertise_host
        self_port = local_m.port if local_m else self._cfg.port
        self._membership.mark_alive(
            self._cfg.node_id, self_host, self_port, self._incarnation
        )

        # Enqueue our ALIVE record in gossip (item B.6).
        self_m = self._membership.get(self._cfg.node_id)
        self._enqueue_gossip(self_m)

        gp  = self._gossip.get_updates(len(self._membership.probeable_members()) + 1)
        msg = messages.alive(
            self._cfg.node_id, self._next_seq(),
            self._cfg.node_id, self._incarnation, gp
        )
        # Broadcast to all probeable (ALIVE + SUSPECT) members (item B.6).
        for member in self._membership.probeable_members():
            await self._transport.send_udp(msg, member.addr(),
                                           target_node_id=member.node_id)

    # -- Utilities -------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _update_member_gauges(self) -> None:
        a, s, d = self._membership.counts()
        metrics.update_member_gauges(a, s, d)

    def _log_event(self, event_type: str, node_id: str, incarnation: int) -> None:
        if self._event_log_cb:
            self._event_log_cb({
                "event":       event_type,
                "node_id":     node_id,
                "incarnation": incarnation,
                "ts":          time.time(),
                "reporter":    self._cfg.node_id,
            })
