"""
agent.py -- AI diagnostics agent for SwimCluster.

The agent is READ-ONLY: it never changes cluster state.  It reads:
  1. A time-windowed slice of the JSON-lines event log (one file or a
     glob pattern / directory of event files).
  2. Prometheus metrics text from /metrics.

It calls an LLM (Google Gemini by default) and requests a structured
JSON diagnosis with keys: summary, likely_cause, evidence, suggested_checks.

The provider is hidden behind a small LLMProvider interface so tests can
inject a fake without network calls.

API key security: the Gemini API key is sent in the ``x-goog-api-key``
request header, never in the URL query string.  Exception messages never
include the key value.
"""

from __future__ import annotations

import asyncio
import glob as glob_module
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

log = logging.getLogger(__name__)


# -- LLM provider interface ----------------------------------------------------

class LLMProvider(ABC):
    """
    Minimal interface for LLM calls.  Only one method is required so tests
    can provide a trivial fake implementation.
    """

    @abstractmethod
    async def complete(self, prompt: str) -> str:
        """Send a prompt and return the model's text response."""
        ...


class GeminiProvider(LLMProvider):
    """
    Calls the Google Gemini generateContent REST API using aiohttp (no SDK
    dependency).  Requires a GEMINI_API_KEY obtained from Google AI Studio.

    The key is sent in the ``x-goog-api-key`` header (item 12).  It never
    appears in the URL or in exception messages.
    """

    _BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash-lite") -> None:
        self._api_key = api_key
        self._model   = model

    async def complete(self, prompt: str) -> str:
        url = f"{self._BASE_URL}/{self._model}:generateContent"
        payload = {
            "contents": [
                {"role": "user", "parts": [{"text": prompt}]}
            ],
            "generationConfig": {"maxOutputTokens": 1024},
        }
        headers = {
            "x-goog-api-key": self._api_key,
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    # Do NOT include self._api_key in the error message.
                    raise RuntimeError(
                        f"Gemini API error {resp.status}: {body[:500]}"
                    )
                data = await resp.json()
                return data["candidates"][0]["content"]["parts"][0]["text"]


class FakeLLMProvider(LLMProvider):
    """
    Deterministic fake used in tests.  Returns a fixed JSON response.
    """

    async def complete(self, prompt: str) -> str:
        return json.dumps({
            "summary":          "Test diagnosis -- no real LLM called.",
            "likely_cause":     "Fault injection or test scenario.",
            "evidence":         ["Event log shows SUSPECT events."],
            "suggested_checks": ["Check node health", "Review fault config"],
        })


# -- Event log reader ----------------------------------------------------------

def _expand_event_paths(path_or_glob: str) -> List[str]:
    """
    Return a list of file paths from a single path, glob pattern, or directory.

    Examples:
      "events.jsonl"         -> ["events.jsonl"]
      "/app/data/events-*.jsonl" -> all matching files
      "/app/data"            -> all *.jsonl files in that directory
    """
    p = Path(path_or_glob)
    if p.is_dir():
        matches = sorted(str(f) for f in p.glob("events-*.jsonl"))
        return matches if matches else sorted(str(f) for f in p.glob("*.jsonl"))
    if not p.exists() and Path("data").is_dir():
        matches = sorted(str(f) for f in Path("data").glob("events-*.jsonl"))
        if matches:
            return matches
    expanded = glob_module.glob(path_or_glob)
    if expanded:
        return sorted(expanded)
    # Treat as a literal path (may not exist yet -- caller handles that).
    return [path_or_glob]


def read_events(path: str, window_seconds: float) -> List[Dict[str, Any]]:
    """
    Return all events from 'path' (file, glob, or directory) whose 'ts' field
    falls within the last 'window_seconds' seconds.

    Lines that are not valid JSON are skipped.  Multiple files are merged and
    returned in chronological order (item 13).
    """
    cutoff = time.time() - window_seconds
    events: List[Dict[str, Any]] = []
    file_paths = _expand_event_paths(path)

    for fpath in file_paths:
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
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
            log.warning("Event log not found: %s", fpath)

    # Sort by timestamp for deterministic output when merging multiple files.
    events.sort(key=lambda e: e.get("ts", 0))
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


# -- Prompt builder ------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an expert distributed-systems reliability engineer.
You are diagnosing issues in a SwimCluster -- a cluster using the SWIM protocol.

Below you will find:
  - Recent cluster events (JSON lines)
  - Current Prometheus metrics snapshot

Analyse the data and reply with ONLY a single JSON object with these keys:
  summary          -- one sentence describing what happened
  likely_cause     -- the most probable root cause
  evidence         -- list of evidence strings from the data
  suggested_checks -- list of actionable checks the operator should perform

Do not include any text outside the JSON object.
"""


def _build_prompt(events: List[Dict], metrics_text: str) -> str:
    events_json = "\n".join(json.dumps(e) for e in events) or "(no events in window)"
    return (
        f"{_SYSTEM_PROMPT}\n\n"
        f"=== CLUSTER EVENTS ===\n{events_json}\n\n"
        f"=== PROMETHEUS METRICS ===\n{metrics_text}\n"
    )


# -- Main agent ----------------------------------------------------------------

class DiagnosticsAgent:
    """
    Read-only diagnostics agent.  Call run() to produce a diagnosis report.
    """

    def __init__(self, provider: LLMProvider,
                 event_log_path: str = "events.jsonl",
                 metrics_url: str = "http://localhost:8081/metrics") -> None:
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
