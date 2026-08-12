"""Strict typed configuration rendered for the Pi runtime."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from .artifacts import canonical_json

_PI_MODEL_APIS = (
    "anthropic-messages",
    "openai-completions",
    "openai-responses",
    "openai-codex-responses",
    "azure-openai-responses",
    "google-generative-ai",
    "google-vertex",
    "mistral-conversations",
    "bedrock-converse-stream",
    "pi-messages",
)
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _strict_config_text(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value or any(character.isspace() or not character.isprintable() for character in value):
        raise ValueError(f"{label} must be a non-empty string without whitespace or controls")
    return value


@dataclass(frozen=True)
class PiModelConfig:
    """One model admitted by a typed Pi provider configuration."""

    id: str

    def __post_init__(self) -> None:
        _strict_config_text(self.id, label="Pi model id")

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> PiModelConfig:
        """Parse one strict model table from project configuration."""
        if not isinstance(values, Mapping):
            raise TypeError("Pi provider models must contain tables")
        unknown = sorted(set(values) - {"id"})
        if unknown:
            raise ValueError(f"Unknown Pi model configuration keys: {', '.join(unknown)}")
        if "id" not in values:
            raise ValueError("Pi model configuration is missing required key: id")
        return cls(id=values["id"])


@dataclass(frozen=True)
class PiProviderConfig:
    """A minimal typed provider rendered into Pi's ``models.json``."""

    id: str
    api: str
    base_url: str
    api_key_env: str
    models: tuple[PiModelConfig, ...]

    def __post_init__(self) -> None:
        _strict_config_text(self.id, label="Pi provider id")
        api = _strict_config_text(self.api, label="Pi provider api")
        if api not in _PI_MODEL_APIS:
            accepted = ", ".join(_PI_MODEL_APIS)
            raise ValueError(f"Pi provider api must be one of: {accepted}; got {api!r}")
        base_url = _strict_config_text(self.base_url, label="Pi provider base_url")
        try:
            parsed_url = urlsplit(base_url)
            hostname = parsed_url.hostname
            _ = parsed_url.port
        except ValueError as error:
            raise ValueError("Pi provider base_url must be a valid absolute HTTP(S) URL") from error
        if (
            parsed_url.scheme not in {"http", "https"}
            or not parsed_url.netloc
            or hostname is None
            or parsed_url.username is not None
            or parsed_url.password is not None
            or parsed_url.query
            or parsed_url.fragment
        ):
            raise ValueError(
                "Pi provider base_url must be an absolute HTTP(S) URL without credentials, "
                "query, or fragment"
            )
        api_key_env = _strict_config_text(
            self.api_key_env,
            label="Pi provider api_key_env",
        )
        if _ENV_NAME.fullmatch(api_key_env) is None:
            raise ValueError(
                "Pi provider api_key_env must be an ASCII environment variable name "
                "without a '$' prefix"
            )
        if not isinstance(self.models, (list, tuple)):
            raise TypeError("Pi provider models must be a non-empty list of PiModelConfig")
        models = tuple(self.models)
        if not models:
            raise ValueError("Pi provider models must not be empty")
        if not all(isinstance(model, PiModelConfig) for model in models):
            raise TypeError("Pi provider models must contain only PiModelConfig values")
        model_ids = [model.id for model in models]
        if len(set(model_ids)) != len(model_ids):
            raise ValueError("Pi provider model ids must be unique")
        object.__setattr__(self, "models", models)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> PiProviderConfig:
        """Parse one strict provider table from project configuration."""
        if not isinstance(values, Mapping):
            raise TypeError("Pi providers must contain tables")
        known = {"id", "api", "base_url", "api_key_env", "models"}
        unknown = sorted(set(values) - known)
        if unknown:
            raise ValueError(f"Unknown Pi provider configuration keys: {', '.join(unknown)}")
        missing = sorted(known - set(values))
        if missing:
            raise ValueError(
                f"Pi provider configuration is missing required keys: {', '.join(missing)}"
            )
        raw_models = values["models"]
        if not isinstance(raw_models, (list, tuple)):
            raise TypeError("Pi provider models must be a non-empty list of tables")
        return cls(
            id=values["id"],
            api=values["api"],
            base_url=values["base_url"],
            api_key_env=values["api_key_env"],
            models=tuple(PiModelConfig.from_mapping(model) for model in raw_models),
        )


def _pi_models_json_bytes(providers: tuple[PiProviderConfig, ...]) -> bytes:
    """Render typed providers through kigumi's canonical JSON format."""
    provider_values = {
        provider.id: {
            "api": provider.api,
            "apiKey": f"${provider.api_key_env}",
            "baseUrl": provider.base_url,
            "models": [{"id": model.id} for model in provider.models],
        }
        for provider in providers
    }
    return canonical_json({"providers": provider_values}).encode("utf-8")
