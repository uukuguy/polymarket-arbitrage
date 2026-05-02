"""polyarb.translation — Phase 1.1 T2 translation pipeline.

Vertical slice (built in plan 01.1-02):
    - config.TranslationConfig — pydantic-settings BaseSettings (env_prefix=TRANSLATION_)
    - cache.TranslationCache   — append-only question_translations CRUD
    - client.TranslationClient — AsyncOpenAI + Semaphore (long-lived)
    - translator.translate_pending — batch orchestrator (ConfigError vs TransientError)

Two-path error semantics (critical):
    - ConfigError   → standalone CLI exits 1; snapshot orchestrator records reason
                       but does NOT fail the run (sidecar)
    - TransientError → caller logs WARNING; cache.increment_retry handles dead-letter

Reference: 01.1-RESEARCH.md §1, 01.1-CONTEXT.md ## T2
"""

from __future__ import annotations

__all__: list[str] = []
