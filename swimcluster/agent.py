"""
agent.py – AI diagnostics agent for SwimCluster.

The agent is READ-ONLY: it never changes cluster state.  It reads:
  1. A time-windowed slice of the JSON-lines event log.
  2. Prometheus metrics text from /metrics.

It calls an LLM (Anthropic Claude by default) and requests a structured
JSON diagnosis with keys: summary, likely_cause, evidence, suggested_checks.

The provider is hidden behind a small LLMProvider interface so tests can
inject a fake without network calls.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

log = logging.getLogger(__name__)


# ── LLM provider interface ────────────────────────────────────────────────────

class LLMProvider(ABC):
    """
    Minimal interface for LLM calls.  Only one method is required so tests
    can provide a trivial fake implementation.
    """

    @abstractmethod
    async def complete(self, prompt: str) -> str:
        """Send a prompt and return the model's text response."""
        ...


class AnthropicProvider(LLMProvider):
    """
    Calls the Anthropic Messages API using aiohttp (no SDK dependency).
    """

    API_URL = "https://api.anthropic.com/v1/messages"

    def __init__(self, api_key: str, model: str = "claude-opus-4-5") -> None:
        self._api_key = api_key
        self._model   = model

    async def complete(self, prompt: str) -> str:
        headers = {
            "x-api-key":         self._api_key,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        }
        payload = {
            "model":      self._model,
            "max_tokens": 1024,
            "messages":   [{"role": "user", "content": prompt}],
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.API_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(
                        f"Anthropic API error {resp.status}: {body}"
                    )
                data = await resp.json()
                return data["content"][0]["text"]


class FakeLLMProvider(LLMProvider):
    """
    Deterministic fake used in tests.  Returns a fixed JSON response.
    """

    async def complete(self, prompt: str) -> str:
        return json.dumps({
            "summary":          "Test diagnosis – no real LLM called.",
            "likely_cause":     "Fault injection or test scenario.",
            "evidence":         ["Event log shows SUSPECT events."],
            "suggested_checks": ["Check node health", "Review fault config"],
        })


# ── Event log reader ──────────────────────────────────────────────────────────

def read_events(path: str, window_seconds: float) -> List[Dict[str, Any]]:
    """
    Return all events from 'path' whose 'ts' field falls within the last
    'window_seconds' seconds.  Lines that are not valid JSON are skipped.
    """
    cutoff = time.time() - window_seconds
    events: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                    if evt.get("ts", 0) >= cutoff:
                        events.append(evt)
                except json.JSONDecodeError:
                    pass
    except FileNotFoundError:
        log.warning("Event log not found: %s", path)
    return events


async def read_metrics(metrics_url: str) -> str:
    """Fetch the Prometheus /metrics text from a node."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                metrics_url, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                return await resp.text()
    except Exception as exc:
        log.warning("Could not fetch metrics from %s: %s", metrics_url, exc)
        return ""


# ── Prompt builder ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are an expert distributed-systems reliability engineer.
You are diagnosing issues in a SwimCluster – a cluster using the SWIM protocol.

Below you will find:
  - Recent cluster events (JSON lines)
  - Current Prometheus metrics snapshot

Analyse the data and reply with ONLY a single JSON object with these keys:
  summary          – one sentence describing what happened
  likely_cause     – the most probable root cause
  evidence         – list of evidence strings from the data
  suggested_checks – list of actionable checks the operator should perform

Do not include any text outside the JSON object.
"""


def _build_prompt(events: List[Dict], metrics_text: str) -> str:
    events_json = "\n".join(json.dumps(e) for e in events) or "(no events in window)"
    return (
        f"{_SYSTEM_PROMPT}\n\n"
        f"=== CLUSTER EVENTS ===\n{events_json}\n\n"
        f"=== PROMETHEUS METRICS ===\n{metrics_text}\n"
    )


# ── Main agent ────────────────────────────────────────────────────────────────

class DiagnosticsAgent:
    """
    Read-only diagnostics agent.  Call run() to produce a diagnosis report.
    """

    def __init__(self, provider: LLMProvider,
                 event_log_path: str = "events.jsonl",
                 metrics_url: str = "http://localhost:8080/metrics") -> None:
        self._provider     = provider
        self._event_log    = event_log_path
        self._metrics_url  = metrics_url

    async def run(self, window_seconds: float = 300) -> Dict[str, Any]:
        """
        Collect data from the last 'window_seconds' seconds, call the LLM,
        and return the parsed diagnosis dict.
        """
        events       = read_events(self._event_log, window_seconds)
        metrics_text = await read_metrics(self._metrics_url)

        prompt   = _build_prompt(events, metrics_text)
        raw_resp = await self._provider.complete(prompt)

        # Extract the JSON object from the response.
        try:
            # Find the first { and last } to handle any surrounding text.
            start = raw_resp.index("{")
            end   = raw_resp.rindex("}") + 1
            diagnosis = json.loads(raw_resp[start:end])
        except (ValueError, json.JSONDecodeError) as exc:
            log.error("Could not parse LLM response as JSON: %s", exc)
            diagnosis = {
                "summary":          "Parse error",
                "likely_cause":     "LLM returned non-JSON response",
                "evidence":         [raw_resp[:500]],
                "suggested_checks": ["Retry the agent"],
            }

        return diagnosis
