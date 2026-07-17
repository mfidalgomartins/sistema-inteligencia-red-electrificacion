import sys

import grid_intelligence.__main__ as entrypoint


def test_entrypoint_default_executes_canonical_pipeline(monkeypatch, capsys):
    monkeypatch.setattr(entrypoint, "run_pipeline", lambda **_: {"mode": "canonical"})
    monkeypatch.setattr(sys, "argv", ["prog"])

    entrypoint.main()
    out = capsys.readouterr().out
    assert "mode: canonical" in out


def test_entrypoint_accepts_log_level(monkeypatch):
    monkeypatch.setattr(entrypoint, "run_pipeline", lambda **_: {})
    monkeypatch.setattr(sys, "argv", ["prog", "--log-level", "WARNING"])

    entrypoint.main()
