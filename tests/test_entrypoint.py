import sys

import src.__main__ as entrypoint


def test_entrypoint_default_executes_v2(monkeypatch, capsys):
    monkeypatch.setattr(entrypoint, "run_final_assembly_v2", lambda: {"mode": "v2"})
    monkeypatch.setattr(sys, "argv", ["prog"])

    entrypoint.main()
    out = capsys.readouterr().out
    assert "mode: v2" in out
