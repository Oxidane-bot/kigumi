from __future__ import annotations

import json

from kigumi import EvidencePolicy
from kigumi.evidence import capture_evidence, scrub_evidence


def test_evidence_policy_digest_is_canonical_and_mode_sensitive() -> None:
    first = EvidencePolicy()
    second = EvidencePolicy(request="redacted")
    assert first.canonical() == {
        "request": "full",
        "response": "full",
        "stderr": "full",
        "trajectory": "full",
    }
    assert first.digest == EvidencePolicy().digest
    assert first.digest != second.digest


def test_full_evidence_scrubs_credentials_headers_and_url_queries(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROVIDER_API_KEY", "top-secret-value")
    value = {
        "authorization": "Bearer top-secret-value",
        "url": "https://example.invalid/path?api_key=top-secret-value&safe=yes",
        "content": "echo top-secret-value",
    }
    scrubbed = scrub_evidence(value, mode="full")
    rendered = json.dumps(scrubbed, sort_keys=True)
    assert "top-secret-value" not in rendered
    assert scrubbed["authorization"] == "***"
    assert "safe=yes" in scrubbed["url"]


def test_full_evidence_scrubs_unregistered_authorization_syntax_and_url_tokens() -> None:
    value = {
        "message": "request used Bearer standalone-secret-value",
        "url": ("https://service.invalid/search?token=query-secret-value&safe=yes then continue"),
    }
    scrubbed = scrub_evidence(value, mode="full")
    rendered = json.dumps(scrubbed, sort_keys=True)
    assert "standalone-secret-value" not in rendered
    assert "query-secret-value" not in rendered
    assert "safe=yes" in scrubbed["url"]
    assert "then continue" in scrubbed["url"]


def test_redacted_evidence_removes_content_values_but_keeps_structure() -> None:
    value = {
        "type": "tool_execution_end",
        "toolName": "read",
        "model": "model-1",
        "usage": {"total_tokens": 12},
        "arguments": {"path": "secret.txt"},
        "output": "sensitive result",
    }
    redacted = scrub_evidence(value, mode="redacted")
    assert redacted["type"] == "tool_execution_end"
    assert redacted["toolName"] == "read"
    assert redacted["model"] == "model-1"
    assert redacted["usage"] == {"total_tokens": 12}
    assert redacted["arguments"]["redacted"] is True
    assert redacted["output"]["redacted"] is True


def test_hash_only_evidence_does_not_return_raw_blob() -> None:
    captured = capture_evidence(
        b'{"content":"secret","type":"message"}\n',
        media_type="application/x-ndjson",
        mode="hash_only",
    )
    assert captured.data is None
    assert captured.descriptor == {
        "sha256": captured.descriptor["sha256"],
        "bytes": 38,
        "media_type": "application/x-ndjson",
        "mode": "hash_only",
    }
