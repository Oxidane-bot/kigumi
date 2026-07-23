from __future__ import annotations

from pathlib import Path

import pytest

from kigumi.agents import AgentResultView, validate_agent_artifact
from kigumi.blobs import BlobStore
from kigumi.store import gc_artifacts, materialize_artifact


def test_ordinary_materializer_ignores_attachment_and_view_verifies_it(tmp_path: Path) -> None:
    blobs = BlobStore(tmp_path / "artifacts" / "_cache" / "blobs")
    digest = blobs.put(b"notes")
    artifact = {
        "agent_schema": 1,
        "completion": {
            "status": "completed",
            "summary": "done",
            "outputs": ["notes/a.md"],
            "metrics": {},
        },
        "attachments": [
            {
                "kigumi_attachment": digest,
                "workspace_path": "notes/a.md",
                "bytes": 5,
                "media_type": "text/markdown",
            }
        ],
    }
    assert materialize_artifact(artifact, "agent", lambda path: tmp_path / path, blobs) == []
    view = AgentResultView(artifact, blobs)
    assert view.list() == ["notes/a.md"]
    assert view.select("notes/*.md") == ["notes/a.md"]
    assert view.read_text("notes/a.md") == "notes"
    assert view.publish("notes/a.md", "generated/notes.md") == {
        "files": {"generated/notes.md": "notes"}
    }
    (blobs.root / digest).write_bytes(b"wrong")
    with pytest.raises(ValueError, match="digest mismatch"):
        validate_agent_artifact(artifact, blobs)


def test_gc_retains_attachment_referenced_by_run(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    blobs = BlobStore(artifacts / "_cache" / "blobs")
    digest = blobs.put(b"evidence")
    run = artifacts / "runs" / "run-0001"
    run.mkdir(parents=True)
    (run / "agent.json").write_text(
        '{"attachments":[{"kigumi_attachment":"' + digest + '"}]}', encoding="utf-8"
    )
    result = gc_artifacts(artifacts, keep_last=1)
    assert result == 0
    assert (blobs.root / digest).is_file()
