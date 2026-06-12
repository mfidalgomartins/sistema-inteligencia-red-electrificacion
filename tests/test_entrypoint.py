import sys

import src.__main__ as entrypoint


def test_entrypoint_default_executes_v2(monkeypatch, capsys):
    monkeypatch.setattr(entrypoint, "run_pipeline", lambda: {"mode": "canonical"})
    monkeypatch.setattr(sys, "argv", ["prog"])

    entrypoint.main()
    out = capsys.readouterr().out
    assert "mode: canonical" in out
