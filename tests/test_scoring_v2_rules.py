from src.scoring_v2 import _tier, _urgency


def test_tier_boundaries():
    assert _tier(39.99) == "bajo"
    assert _tier(40.0) == "medio"
    assert _tier(60.0) == "alto"
    assert _tier(80.0) == "critico"


def test_urgency_boundaries():
    assert _urgency(54.99) == "monitorizacion"
    assert _urgency(55.0) == "planificada"
    assert _urgency(70.0) == "alta"
    assert _urgency(85.0) == "inmediata"
