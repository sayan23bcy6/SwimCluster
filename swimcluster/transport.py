"""
transport.py -- UDP socket for probes/gossip, TCP for full-state sync.

Design decisions:
  * UDP is used for probes and gossip because they are small, latency-
    sensitive, and the SWIM protocol already handles loss through
    timeouts and retransmission at the application layer.
  * TCP is used only for the JOIN handshake where we need reliable
    delivery of the full (potentially large) membership list.
  * The Transport interface is deliberately thin so that tests can
    swap in an InMemoryTransport without touching any protocol logic.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from swimcluster import messages, metrics

log = logging.getLogger(__name__)

# Type alias for the message-handler callback used by the SWIM layer.
Handler = Callable[[Dict[str, Any], Tuple[str, int]], Awaitable[None]]


# -- Abstract interface --------------------------------------------------------

class BaseTransport:
    """
    Minimal transport interface.  Concrete implementations provide UDP
    send/receive and TCP connect/listen without exposing socket details
    to the protocol layer.
    """

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    async def send_udp(self, msg: Dict[str, Any], addr: Tuple[str, int],
                       target_node_id: str = "") -> None:
        raise NotImplementedError

    async def send_tcp(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> Optional[Dict[str, Any]]:
        """Open a TCP connection, send msg, wait for one reply, close."""
        raise NotImplementedError

    def set_handler(self, handler: Handler) -> None:
        """Register the callback that receives decoded, validated messages."""
        self._handler = handler

    def _handler(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> Awaitable[None]:  # type: ignore[return-value]
        raise NotImplementedError


# -- Real UDP/TCP transport ----------------------------------------------------

class UDPProtocol(asyncio.DatagramProtocol):
    """asyncio DatagramProtocol that delegates received datagrams to SWIM."""

    def __init__(self, handler: Handler, fault_layer: Any) -> None:
        self._handler    = handler
        self._fault      = fault_layer   # FaultLayer or None
        self._transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        msg = messages.decode(data)
        if msg is None or not messages.validate(msg):
            log.debug("Dropping malformed UDP datagram from %s", addr)
            metrics.messages_dropped_malformed.inc()
            return
        # Check if the sender is partitioned with this node (receive side).
        sender_id = msg.get("sender", "")
        if self._fault and sender_id and self._fault._is_partitioned(sender_id):
            log.debug("Fault: receive-side partition dropping datagram from %s", sender_id)
            return
        # Schedule the async handler on the running loop.
        asyncio.get_event_loop().create_task(self._handler(msg, addr))

    def error_received(self, exc: Exception) -> None:
        log.warning("UDP error: %s", exc)


class RealTransport(BaseTransport):
    """
    Concrete transport using real OS sockets.
    UDP for probes/gossip, TCP for JOIN handshake.
    """

    MAX_UDP_SIZE = 65_507  # theoretical UDP payload limit

    def __init__(self, host: str, port: int, fault_layer: Any = None) -> None:
        self._host        = host
        self._port        = port
        self._fault       = fault_layer
        self._udp_proto: Optional[UDPProtocol] = None
        self._udp_transport: Optional[asyncio.DatagramTransport] = None
        self._tcp_server: Optional[asyncio.AbstractServer] = None
        self._handler_fn: Optional[Handler] = None
        # Staging slot for TCP reply -- always defined, never hasattr-checked.
        self._pending_tcp_reply: Optional[Dict] = None

    def set_handler(self, handler: Handler) -> None:  # type: ignore[override]
        self._handler_fn = handler

    async def start(self) -> None:
        loop = asyncio.get_event_loop()

        # Bind the UDP socket.
        self._udp_proto = UDPProtocol(self._dispatch, self._fault)
        self._udp_transport, _ = await loop.create_datagram_endpoint(
            lambda: self._udp_proto,  # type: ignore[return-value]
            local_addr=(self._host, self._port),
        )

        # Start the TCP server for JOIN / SYNC.
        self._tcp_server = await asyncio.start_server(
            self._handle_tcp_connection,
            host=self._host,
            port=self._port,
        )
        log.info("Transport listening on %s:%s (UDP+TCP)", self._host, self._port)

    async def stop(self) -> None:
        if self._udp_transport:
            self._udp_transport.close()
        if self._tcp_server:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()

    async def send_udp(self, msg: Dict[str, Any], addr: Tuple[str, int],
                       target_node_id: str = "") -> None:
        """Encode and fire a UDP datagram, applying fault injection if configured.

        target_node_id is the logical node ID of the recipient; FaultLayer
        checks partitions/loss against node IDs, not IP addresses.
        """
        # Use node ID for fault layer checks when available.
        fault_target = target_node_id if target_node_id else addr[0]
        if self._fault and not await self._fault.allow_send_to_node(fault_target):
            return  # packet dropped by fault injector

        data = messages.encode(msg)
        if len(data) > self.MAX_UDP_SIZE:
            log.warning("UDP message too large (%d bytes), dropping", len(data))
            return

        if self._udp_transport:
            self._udp_transport.sendto(data, addr)

    async def send_tcp(self, msg: Dict[str, Any],
                       addr: Tuple[str, int]) -> Optional[Dict[str, Any]]:
        """
        Open a TCP connection to 'addr', send 'msg', read one response,
        then close.  Used only for JOIN handshake so brevity is fine.
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(addr[0], addr[1]), timeout=5.0
            )
            writer.write(messages.encode(msg) + b"\n")
            await writer.drain()

            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            reply = messages.decode(raw.strip())
            writer.close()
            await writer.wait_closed()
            return reply
        except (OSError, asyncio.TimeoutError) as exc:
            log.warning("TCP send to %s failed: %s", addr, exc)
            return None

    async def _handle_tcp_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle an inbound TCP connection (JOIN / SYNC request)."""
        addr = writer.get_extra_info("peername")
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            msg = messages.decode(raw.strip())
            if msg is None or not messages.validate(msg):
                log.debug("Dropping malformed TCP message from %s", addr)
                metrics.messages_dropped_malformed.inc()
                return
            # Reset the reply slot before handling so we can detect new replies.
            self._pending_tcp_reply = None
            await self._dispatch(msg, addr)

            # If the handler stored a reply, send it.
            if self._pending_tcp_reply:
                reply = self._pending_tcp_reply
                self._pending_tcp_reply = None
                writer.write(messages.encode(reply) + b"\n")
                await writer.drain()
        except (OSError, asyncio.TimeoutError) as exc:
            log.warning("TCP handler error from %s: %s", addr, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass

    async def _dispatch(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Route a decoded, validated message to the registered handler."""
        if self._handler_fn:
            await self._handler_fn(msg, addr)


# -- In-memory transport (for tests) ------------------------------------------

class InMemoryTransport(BaseTransport):
    """
    Deterministic, in-process transport used by unit tests.

    Instead of real sockets, messages are delivered synchronously (via
    asyncio.gather) so tests can reason about ordering without sleep().
    A shared registry maps node_id -> InMemoryTransport so nodes can
    address each other by ID alone.
    """

    # Class-level registry shared by all test transports in a process.
    _registry: Dict[str, "InMemoryTransport"] = {}

    @classmethod
    def reset(cls) -> None:
        """Clear the registry between tests."""
        cls._registry.clear()

    def __init__(self, node_id: str, fault_layer: Any = None) -> None:
        self.node_id     = node_id
        self._fault      = fault_layer
        self._handler_fn: Optional[Handler] = None
        self._inbox: asyncio.Queue = asyncio.Queue()
        # Staging slot for TCP reply (in-memory path).
        self._tcp_reply: Optional[Dict] = None
        # Mirrors RealTransport's attribute so node.py can always write it.
        self._pending_tcp_reply: Optional[Dict] = None

    def set_handler(self, handler: Handler) -> None:  # type: ignore[override]
        self._handler_fn = handler

    async def start(self) -> None:
        InMemoryTransport._registry[self.node_id] = self
        log.debug("InMemoryTransport started for %s", self.node_id)

    async def stop(self) -> None:
        InMemoryTransport._registry.pop(self.node_id, None)

    async def send_udp(self, msg: Dict[str, Any], addr: Tuple[str, int],
                       target_node_id: str = "") -> None:
        """Deliver a message to the target node's handler in-process.

        target_node_id, if provided, overrides addr[0] for fault checks and
        registry lookup, matching how real nodes address each other by ID.
        """
        target_id = target_node_id if target_node_id else addr[0]
        target    = InMemoryTransport._registry.get(target_id)
        if target is None:
            return  # node not started yet -- drop silently

        # Use node ID for fault layer checks.
        fault_target = target_node_id if target_node_id else addr[0]
        if self._fault and not await self._fault.allow_send_to_node(fault_target):
            return

        if target._handler_fn:
            await target._handler_fn(msg, (self.node_id, addr[1]))

    async def send_tcp(self, msg: Dict[str, Any],
                       addr: Tuple[str, int]) -> Optional[Dict[str, Any]]:
        """Synchronous in-memory TCP: deliver and get reply.

        The handler may be either:
          - swim.handle_message (returns the reply directly), or
          - node._dispatch (returns None but sets _pending_tcp_reply).
        We check the return value first, then fall back to the staging slots.
        """
        target_id = addr[0]
        target    = InMemoryTransport._registry.get(target_id)
        if target is None or target._handler_fn is None:
            return None
        # Reset reply slots before invoking.
        target._tcp_reply = None
        target._pending_tcp_reply = None
        result = await target._handler_fn(msg, (self.node_id, addr[1]))
        # swim.handle_message returns the reply dict directly.
        if result is not None:
            return result
        # node._dispatch stores the reply in _pending_tcp_reply.
        return target._pending_tcp_reply or target._tcp_reply

