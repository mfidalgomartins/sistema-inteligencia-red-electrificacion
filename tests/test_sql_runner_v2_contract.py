from src.sql_runner_v2 import EXPORT_OBJECTS, SQL_SEQUENCE_V2


def test_sql_sequence_v2_contract():
    assert len(SQL_SEQUENCE_V2) == 10
    assert SQL_SEQUENCE_V2[0] == "01_staging_core_tables.sql"
    assert SQL_SEQUENCE_V2[-1] == "10_validation_queries.sql"
    for i, file_name in enumerate(SQL_SEQUENCE_V2, start=1):
        assert file_name.startswith(f"{i:02d}_")


def test_export_objects_include_core_views_and_marts():
    required = {
        "mart_node_hour_operational_state",
        "mart_zone_day_operational",
        "mart_zone_month_operational",
        "vw_zone_operational_risk",
        "vw_assets_exposure",
        "vw_flexibility_gap",
        "vw_investment_candidates",
        "validation_checks",
    }
    assert required.issubset(set(EXPORT_OBJECTS.keys()))
