import json

import pandas as pd
import pytest

from grid_intelligence.release_docs import _read_csv, _read_json


def test_release_docs_reject_missing_required_csv(tmp_path):
    with pytest.raises(FileNotFoundError, match="artefacto requerido"):
        _read_csv(tmp_path / "missing.csv")


def test_release_docs_validate_json_root(tmp_path):
    path = tmp_path / "summary.json"
    path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")

    with pytest.raises(ValueError, match="objeto en la raíz"):
        _read_json(path)


def test_release_docs_read_valid_csv(tmp_path):
    path = tmp_path / "valid.csv"
    pd.DataFrame({"value": [1]}).to_csv(path, index=False)

    assert _read_csv(path).to_dict("records") == [{"value": 1}]
