import pandas as pd
import pytest

from grid_intelligence.common import chunked, get_paths, minmax, pct


def test_default_project_root_contains_pyproject():
    assert (get_paths().root / "pyproject.toml").is_file()


def test_minmax_handles_constant_and_missing_series():
    constant = minmax(pd.Series([3.0, 3.0]))
    missing = minmax(pd.Series([None, None]))

    assert constant.tolist() == [50.0, 50.0]
    assert missing.tolist() == [0.0, 0.0]


def test_chunked_validates_size_and_preserves_tail():
    assert list(chunked(range(5), 2)) == [[0, 1], [2, 3], [4]]
    with pytest.raises(ValueError, match="mayor que cero"):
        list(chunked([1], 0))


def test_pct_handles_zero_denominator():
    assert pct(4.0, 2.0) == 2.0
    assert pct(4.0, 0.0) == 0.0
