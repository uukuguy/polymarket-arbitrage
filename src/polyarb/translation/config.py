"""TranslationConfig — pydantic-settings BaseSettings for the LLM translation client.

env_prefix = "TRANSLATION_" (NOT "POLYARB_") — keeps the translation pipeline's
secret material in its OWN namespace so it can't accidentally pick up Phase-1
Settings env vars and vice versa. Verified non-conflicting with polyarb.config.Settings
which uses env_prefix="POLYARB_".

api_key is wrapped in pydantic.SecretStr so repr(cfg) cannot accidentally leak the
key into a log line (T-01.1-04 mitigation). Use .get_secret_value() (or the
``secret_api_key()`` helper) when handing the key to the AsyncOpenAI constructor.
"""

from __future__ import annotations

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class TranslationConfig(BaseSettings):
    """Configuration loaded from environment / .env (TRANSLATION_* prefix).

    Required (no default — fail fast if .env missing):
        - api_base       (e.g. https://api.deepseek.com/v1)
        - api_key        (SecretStr — never print)
        - model          (e.g. deepseek-chat / qwen-plus / gpt-4o-mini)

    Optional (sensible defaults from RESEARCH §1.3, §1.5):
        - max_concurrency = 10
        - batch_size      = 20
        - max_retries     = 3
        - request_timeout_s = 30.0
    """

    api_base: str
    api_key: SecretStr
    model: str
    max_concurrency: int = 10
    batch_size: int = 20
    max_retries: int = 3
    request_timeout_s: float = 30.0

    model_config = SettingsConfigDict(
        env_prefix="TRANSLATION_",
        env_file=".env",
        extra="ignore",
    )

    def secret_api_key(self) -> str:
        """Reveal the api_key — call only when handing to AsyncOpenAI."""
        return self.api_key.get_secret_value()
