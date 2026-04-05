import pandas as pd

from src.validate_data_v2 import classify_release_readiness, compute_validation_assessment


def test_validation_assessment_pass_when_no_issues():
    issues = pd.DataFrame(columns=["severity"])
    out = compute_validation_assessment(issues)
    assert out["overall_status"] == "PASS"
    assert out["confidence_level"] == "alta"


def test_validation_assessment_warn_on_medium_issues():
    issues = pd.DataFrame(
        [
            {"severity": "media"},
            {"severity": "baja"},
        ]
    )
    out = compute_validation_assessment(issues)
    assert out["overall_status"] == "WARN"
    assert out["confidence_level"] == "media"
    assert out["n_med"] == 1


def test_validation_assessment_fail_on_high_issues():
    issues = pd.DataFrame(
        [
            {"severity": "alta"},
            {"severity": "media"},
        ]
    )
    out = compute_validation_assessment(issues)
    assert out["overall_status"] == "FAIL"
    assert out["confidence_level"] == "baja"
    assert out["n_high"] == 1


def test_release_classification_publish_blocked_if_blocker_fails():
    assessment = {"n_high": 0, "n_med": 0, "n_low": 0}
    gates = pd.DataFrame(
        [
            {"gate_name": "core", "passed": False, "is_blocker": True, "detail": "x"},
        ]
    )
    out = classify_release_readiness(assessment, gates)
    assert out["publish_state"] == "publish-blocked"
    assert out["technical_state"] == "not technically valid"


def test_release_classification_decision_support_only_on_single_medium_issue():
    assessment = {"n_high": 0, "n_med": 1, "n_low": 0}
    gates = pd.DataFrame(
        [
            {"gate_name": "core", "passed": True, "is_blocker": True, "detail": "x"},
        ]
    )
    out = classify_release_readiness(assessment, gates)
    assert out["decision_state"] == "decision-support only"
    assert out["committee_state"] == "not committee-grade"
    assert out["publish_state"] == "publish-with-caveats"
