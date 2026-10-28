"""
cli.py -- Entry point: run a node, inject faults, run the agent.

Usage:
  swimcluster node --id node1 --port 7946 --seeds localhost:7947
  swimcluster fault partition --node-a node1 --node-b node2 --target http://localhost:8081
  swimcluster fault heal      --node-a node1 --node-b node2 --target http://localhost:8081
  swimcluster fault loss      --prob 0.3     --target http://localhost:8081
  swimcluster fault clear                    --target http://localhost:8081
  swimcluster agent           --window 5m    --event-log events.jsonl

The fault --target default is http://localhost:8081 (node1 host port).
Ports 8081-8085 are mapped to nodes 1-5 in the Docker Compose setup.

The agent --metrics-url default is http://localhost:8081/metrics (node1).
The agent --event-log accepts a file path, a glob pattern, or a directory
containing *.jsonl files so all node event logs can be read at once.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
)
log = logging.getLogger(__name__)


# -- Sub-commands --------------------------------------------------------------

def _parse_window(raw: str) -> float:
    """Parse a duration string like '5m', '30s', '1h' into seconds."""
    raw = raw.strip()
    if raw.endswith("m"):
        return float(raw[:-1]) * 60
    if raw.endswith("h"):
        return float(raw[:-1]) * 3600
    if raw.endswith("s"):
        return float(raw[:-1])
    return float(raw)


async def _cmd_node(args: argparse.Namespace) -> None:
    from swimcluster.config import from_env
    from swimcluster.node import Node

    overrides = {}
    if args.id:
        overrides["node_id"] = args.id
    if args.host:
        overrides["host"] = args.host
    if args.port:
        overrides["port"] = args.port
    if args.http_port:
        overrides["http_port"] = args.http_port
    if args.seeds:
        overrides["seeds"] = [s.strip() for s in args.seeds.split(",") if s.strip()]
    if args.advertise_host:
        overrides["advertise_host"] = args.advertise_host

    cfg  = from_env(overrides)
    node = Node(cfg)

    log.info("Starting node %s on %s:%s (http :%s)",
             cfg.node_id, cfg.host, cfg.port, cfg.http_port)

    await node.start()

    try:
        # Run until interrupted.
        await asyncio.Event().wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        await node.stop()


async def _cmd_fault(args: argparse.Namespace) -> None:
    import aiohttp

    target = args.target.rstrip("/")
    url    = f"{target}/admin/faults"

    async with aiohttp.ClientSession() as session:
        if args.fault_action == "partition":
            body = {"partition_add": [args.node_a, args.node_b]}
            async with session.post(url, json=body) as r:
                print(json.dumps(await r.json(), indent=2))

        elif args.fault_action == "heal":
            body = {"partition_remove": [args.node_a, args.node_b]}
            async with session.post(url, json=body) as r:
                print(json.dumps(await r.json(), indent=2))

        elif args.fault_action == "loss":
            body = {"loss_prob": args.prob}
            async with session.post(url, json=body) as r:
                print(json.dumps(await r.json(), indent=2))

        elif args.fault_action == "delay":
            body = {"delay_min": args.min_delay, "delay_max": args.max_delay}
            async with session.post(url, json=body) as r:
                print(json.dumps(await r.json(), indent=2))

        elif args.fault_action == "clear":
            async with session.delete(url) as r:
                print(json.dumps(await r.json(), indent=2))

        elif args.fault_action == "status":
            async with session.get(url) as r:
                print(json.dumps(await r.json(), indent=2))


async def _cmd_agent(args: argparse.Namespace) -> None:
    from swimcluster.agent import GeminiProvider, DiagnosticsAgent, FakeLLMProvider

    api_key = os.getenv("GEMINI_API_KEY", "")

    if not api_key:
        log.warning("GEMINI_API_KEY not set -- using FakeLLMProvider")
        provider = FakeLLMProvider()
    else:
        model    = os.getenv("SWIM_AGENT_MODEL", "gemini-2.0-flash-lite")
        provider = GeminiProvider(api_key, model)

    window = _parse_window(args.window)
    agent  = DiagnosticsAgent(
        provider,
        event_log_path=args.event_log,
        metrics_url=args.metrics_url,
    )

    log.info("Running agent on last %.0f seconds of events ...", window)
    diagnosis = await agent.run(window_seconds=window)
    print(json.dumps(diagnosis, indent=2))


# -- Argument parser -----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="swimcluster",
        description="SwimCluster -- self-healing cluster via SWIM protocol",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- node ------------------------------------------------------------------
    node_p = sub.add_parser("node", help="Run a cluster node")
    node_p.add_argument("--id",             default="",   help="Node ID")
    node_p.add_argument("--host",           default=None, help="Bind host")
    node_p.add_argument("--advertise-host", default=None, dest="advertise_host",
                        help="Advertised host for peers (default: same as --host)")
    node_p.add_argument("--port",      type=int, default=None, help="UDP/TCP port")
    node_p.add_argument("--http-port", type=int, default=None, dest="http_port",
                        help="HTTP API port")
    node_p.add_argument("--seeds",     default="",
                        help="Comma-separated seed addresses host:port")

    # -- fault -----------------------------------------------------------------
    fault_p = sub.add_parser("fault", help="Inject or clear faults")
    fault_p.add_argument("--target", default="http://localhost:8081",
                         help="HTTP base URL of the target node (default: http://localhost:8081)")
    fault_sub = fault_p.add_subparsers(dest="fault_action", required=True)

    part_p = fault_sub.add_parser("partition", help="Partition two nodes")
    part_p.add_argument("--node-a", required=True)
    part_p.add_argument("--node-b", required=True)

    heal_p = fault_sub.add_parser("heal", help="Heal a partition")
    heal_p.add_argument("--node-a", required=True)
    heal_p.add_argument("--node-b", required=True)

    loss_p = fault_sub.add_parser("loss", help="Set packet loss probability")
    loss_p.add_argument("--prob", type=float, required=True)

    delay_p = fault_sub.add_parser("delay", help="Set artificial delay range")
    delay_p.add_argument("--min-delay", type=float, default=0.0, dest="min_delay")
    delay_p.add_argument("--max-delay", type=float, default=0.1, dest="max_delay")

    fault_sub.add_parser("clear",  help="Remove all fault injection")
    fault_sub.add_parser("status", help="Show current fault config")

    # -- agent -----------------------------------------------------------------
    agent_p = sub.add_parser("agent", help="Run the AI diagnostics agent")
    agent_p.add_argument("--window",     default="5m",
                         help="Time window of events to analyse (e.g. 5m, 30s, 1h)")
    agent_p.add_argument("--event-log",  default="events.jsonl", dest="event_log",
                         help=("Path to JSON-lines event log, a glob pattern "
                               "(e.g. /app/data/events-*.jsonl), or a directory "
                               "of *.jsonl files"))
    agent_p.add_argument("--metrics-url",
                         default="http://localhost:8081/metrics", dest="metrics_url",
                         help="URL of the Prometheus /metrics endpoint (default: node1)")

    return parser


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    if args.command == "node":
        asyncio.run(_cmd_node(args))
    elif args.command == "fault":
        asyncio.run(_cmd_fault(args))
    elif args.command == "agent":
        asyncio.run(_cmd_agent(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
