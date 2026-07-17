from grid_intelligence.sql_runner import EXPORT_OBJECTS, SQL_SEQUENCE


def test_sql_sequence_contract():
    assert len(SQL_SEQUENCE) == 10
    assert SQL_SEQUENCE[0] == "01_staging_core_tables.sql"
    assert SQL_SEQUENCE[-1] == "10_validation_queries.sql"
    for i, file_name in enumerate(SQL_SEQUENCE, start=1):
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
