"""
node.py -- Wires SWIM, transport, KV store, metrics, and HTTP together.

The Node class owns the event loop lifecycle.  It starts:
  * The transport (UDP + TCP).
  * The SWIM failure detector.
  * The aiohttp HTTP server for KV and admin endpoints.
  * A periodic metrics gauge update.

All inter-module communication happens through explicit method calls; no
global state is shared between modules.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

from aiohttp import web

from swimcluster import messages, metrics
from swimcluster.config import Config
from swimcluster.faults import FaultLayer, FaultConfig
from swimcluster.gossip import GossipQueue
from swimcluster.kvstore import KVStore
from swimcluster.membership import Member, MembershipList, NodeState
from swimcluster.swim import SWIM
from swimcluster.transport import BaseTransport, RealTransport

log = logging.getLogger(__name__)


class Node:
    """
    Cluster node: owns all subsystems and the main event loop lifecycle.
    """

    def __init__(self, cfg: Config,
                 transport: Optional[BaseTransport] = None,
                 fault_layer: Optional[FaultLayer] = None) -> None:
        self._cfg        = cfg
        self._fault      = fault_layer or FaultLayer(cfg.node_id)
        self._transport  = transport or RealTransport(
            cfg.host, cfg.port, self._fault
        )

        # Shared membership list.
        self._membership = MembershipList(cfg.node_id)

        # Add ourselves to the member list as ALIVE using the *advertised* host.
        advertise_host = cfg.effective_advertise_host()
        self._membership.mark_alive(
            cfg.node_id, advertise_host, cfg.port, incarnation=0
        )

        # SWIM failure detector.
        self._swim = SWIM(cfg, self._transport, self._membership)
        self._swim.set_event_log(self._log_event)

        # KV store.
        self._kv = KVStore(cfg.node_id, cfg.kvstore_replicas)

        # aiohttp app.
        self._app  = web.Application()
        self._runner: Optional[web.AppRunner] = None

        # TCP reply staging: the SWIM layer sets this before returning from _on_join.
        # RealTransport also has this attribute; we keep it in sync during dispatch.
        self._pending_tcp_reply: Optional[Dict] = None

        # Event log file.
        self._event_log_path = cfg.event_log_path

        self._setup_routes()

    # -- Lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        # Register incoming message handler.
        self._transport.set_handler(self._dispatch)

        await self._transport.start()
        await self._swim.start()

        # Join via seed nodes if configured.
        for seed in self._cfg.seeds:
            await self._join_seed(seed)

        # Start HTTP server.
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._cfg.host, self._cfg.http_port)
        await site.start()
        log.info("Node %s HTTP on %s:%s", self._cfg.node_id,
                 self._cfg.host, self._cfg.http_port)

        # Periodic membership gauge update.
        asyncio.get_event_loop().create_task(self._metrics_loop())

    async def stop(self) -> None:
        await self._swim.stop()
        await self._transport.stop()
        if self._runner:
            await self._runner.cleanup()

    # -- Join ------------------------------------------------------------------

    async def _join_seed(self, seed: str) -> None:
        """Contact a seed node over TCP to get the initial membership list."""
        try:
            host, port_str = seed.rsplit(":", 1)
            port = int(port_str)
        except ValueError:
            log.error("Invalid seed address: %s", seed)
            return

        # Use the advertised host/port in the JOIN message so peers know
        # how to reach us.
        advertise_host = self._cfg.effective_advertise_host()
        local_m = self._membership.get(self._cfg.node_id)
        addr_host = local_m.host if local_m else advertise_host
        addr_port = local_m.port if local_m else self._cfg.port

        msg = messages.join(self._cfg.node_id, 1, addr_host, addr_port)
        log.info("Joining cluster via seed %s:%s", host, port)
        reply = await self._transport.send_tcp(msg, (host, port))
        if reply and messages.validate(reply):
            await self._dispatch(reply, (host, port))
        else:
            log.warning("No valid JOIN_ACK from seed %s:%s", host, port)

    # -- Message dispatch ------------------------------------------------------

    async def _dispatch(self, msg: Dict[str, Any],
                        addr: Tuple[str, int]) -> None:
        """Route every incoming message through the SWIM handler."""
        mtype = msg.get("type")
        log.debug("Received %s from %s", mtype, addr)

        reply = await self._swim.handle_message(msg, addr)

        # If SWIM produced a reply (e.g. for PING or JOIN), send it.
        if reply is not None:
            if mtype == messages.MsgType.JOIN.value:
                # TCP reply -- store it for the transport to pick up.
                # RealTransport has _pending_tcp_reply defined in __init__.
                self._transport._pending_tcp_reply = reply  # type: ignore[union-attr]
                # Also store on InMemoryTransport path (uses _tcp_reply attr).
                if hasattr(self._transport, "_tcp_reply"):
                    self._transport._tcp_reply = reply  # type: ignore[union-attr]
            else:
                await self._transport.send_udp(reply, addr)

        # Rebuild KV ring on any membership-changing message.
        if mtype in (messages.MsgType.ALIVE.value,
                     messages.MsgType.SUSPECT.value,
                     messages.MsgType.DEAD.value,
                     messages.MsgType.JOIN_ACK.value):
            self._kv.on_membership_change(self._membership.all_members())

    # -- HTTP Routes -----------------------------------------------------------

    def _setup_routes(self) -> None:
        self._app.router.add_get("/health",          self._handle_health)
        self._app.router.add_get("/members",         self._handle_members)
        self._app.router.add_get("/metrics",         self._handle_metrics)
        self._app.router.add_get("/kv/{key}",        self._handle_kv_get)
        self._app.router.add_put("/kv/{key}",        self._handle_kv_put)
        self._app.router.add_delete("/kv/{key}",     self._handle_kv_delete)
        self._app.router.add_get("/admin/faults",    self._handle_fault_get)
        self._app.router.add_post("/admin/faults",   self._handle_fault_post)
        self._app.router.add_delete("/admin/faults", self._handle_fault_delete)

    async def _handle_health(self, _: web.Request) -> web.Response:
        return web.json_response({"node": self._cfg.node_id, "status": "ok"})

    async def _handle_members(self, _: web.Request) -> web.Response:
        members = [m.to_dict() for m in self._membership.all_members()]
        return web.json_response(members)

    async def _handle_metrics(self, _: web.Request) -> web.Response:
        body, ctype = metrics.scrape()
        return web.Response(body=body, headers={"Content-Type": ctype})

    async def _handle_kv_get(self, req: web.Request) -> web.Response:
        key = req.match_info["key"]
        val = self._kv.get(key)
        if val is None:
            raise web.HTTPNotFound(reason=f"Key '{key}' not found")
        return web.json_response({"key": key, "value": val,
                                  "owner": self._kv.owner(key)})

    async def _handle_kv_put(self, req: web.Request) -> web.Response:
        key  = req.match_info["key"]
        body = await req.text()
        self._kv.put(key, body)
        return web.json_response({"key": key, "status": "stored",
                                  "owner": self._kv.owner(key)})

    async def _handle_kv_delete(self, req: web.Request) -> web.Response:
        key = req.match_info["key"]
        deleted = self._kv.delete(key)
        if not deleted:
            raise web.HTTPNotFound(reason=f"Key '{key}' not found")
        return web.json_response({"key": key, "status": "deleted"})

    async def _handle_fault_get(self, _: web.Request) -> web.Response:
        return web.json_response(self._fault.status())

    async def _handle_fault_post(self, req: web.Request) -> web.Response:
        """
        Admin endpoint to set fault-injection parameters at runtime.
        Body: JSON with optional fields: loss_prob, delay_min, delay_max,
              partition_add: [node_a, node_b], partition_remove: [node_a, node_b]
        """
        try:
            data = await req.json()
        except Exception:
            raise web.HTTPBadRequest(reason="Invalid JSON body")

        if "loss_prob" in data:
            self._fault.set_loss(float(data["loss_prob"]))
        if "delay_min" in data and "delay_max" in data:
            self._fault.set_delay(float(data["delay_min"]),
                                  float(data["delay_max"]))
        if pa := data.get("partition_add"):
            self._fault.add_partition(pa[0], pa[1])
        if pr := data.get("partition_remove"):
            self._fault.remove_partition(pr[0], pr[1])

        return web.json_response(self._fault.status())

    async def _handle_fault_delete(self, _: web.Request) -> web.Response:
        self._fault.clear_all()
        return web.json_response({"status": "cleared"})

    # -- Internal helpers ------------------------------------------------------

    async def _metrics_loop(self) -> None:
        """Refresh Prometheus gauges every protocol period."""
        while True:
            a, s, d = self._membership.counts()
            metrics.update_member_gauges(a, s, d)
            await asyncio.sleep(self._cfg.protocol_period)

    def _log_event(self, event: Dict[str, Any]) -> None:
        """Append a structured event to the JSON-lines event log."""
        try:
            with open(self._event_log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(event) + "\n")
        except OSError as exc:
            log.warning("Could not write event log: %s", exc)

    # -- Public accessors for tests --------------------------------------------

    @property
    def membership(self) -> MembershipList:
        return self._membership

    @property
    def swim(self) -> SWIM:
        return self._swim

    @property
    def kv(self) -> KVStore:
        return self._kv

    @property
    def fault(self) -> FaultLayer:
        return self._fault
