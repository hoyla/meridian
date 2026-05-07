"""LLM narrative layer over the deterministic findings.

Pluggable backend: Ollama (default, local) or Gemini. The LLM sees the
deterministic findings + supporting rows — it does NOT compute statistics, only
clusters and narrates them. Numbers in its output are validated against the
underlying observation rows before being stored.
"""

import logging
import os
from typing import Protocol

log = logging.getLogger(__name__)


class LLMBackend(Protocol):
    def generate(self, system: str, prompt: str) -> str: ...


def make_backend() -> LLMBackend:
    backend = os.environ.get("LLM_BACKEND", "ollama").lower()
    if backend == "ollama":
        return OllamaBackend()
    if backend == "gemini":
        return GeminiBackend()
    raise ValueError(f"Unknown LLM_BACKEND: {backend}")


class OllamaBackend:
    def generate(self, system: str, prompt: str) -> str:
        raise NotImplementedError


class GeminiBackend:
    def generate(self, system: str, prompt: str) -> str:
        raise NotImplementedError


def frame_findings_for_run(scrape_run_id: int) -> int:
    """Cluster + narrate the deterministic findings for the given run."""
    raise NotImplementedError
