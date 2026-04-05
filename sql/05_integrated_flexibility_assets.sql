-- Dialecto: DuckDB SQL
-- Nivel: integration
-- Objetivo: consolidar capacidad de flexibilidad, almacenamiento e intervención por zona.

CREATE OR REPLACE VIEW vw_int_flexibility_assets_zone AS
WITH zone_peak_demand AS (
    SELECT
        d.zona_id,
        MAX(d.demanda_zona_mw) AS peak_demand_zona_mw,
        AVG(d.demanda_zona_mw) AS avg_demand_zona_mw
    FROM (
        SELECT
            dh.timestamp,
            dh.zona_id,
            SUM(dh.demanda_mw) AS demanda_zona_mw
        FROM stg_demanda_horaria dh
        GROUP BY dh.timestamp, dh.zona_id
    ) d
    GROUP BY d.zona_id
),
zone_capacity AS (
    SELECT
        s.zona_id,
        SUM(a.capacidad_mw) AS capacidad_total_alimentadores_mw,
        SUM(s.capacidad_mw) AS capacidad_total_subestaciones_mw,
        SUM(s.capacidad_firme_mw) AS capacidad_firme_subestaciones_mw
    FROM stg_subestaciones s
    INNER JOIN stg_alimentadores a
        ON s.subestacion_id = a.subestacion_id
    GROUP BY s.zona_id
),
flex AS (
    SELECT
        rf.zona_id,
        SUM(rf.capacidad_flexible_mw) AS capacidad_flexible_mw,
        SUM(rf.capacidad_flexible_mw * rf.coste_activacion_eur_mwh) / NULLIF(SUM(rf.capacidad_flexible_mw), 0) AS coste_activacion_ponderado_eur_mwh,
        AVG(rf.tiempo_respuesta_min) AS tiempo_respuesta_medio_min,
        AVG(rf.disponibilidad_media) AS disponibilidad_flex_media,
        AVG(rf.fiabilidad_activacion) AS fiabilidad_flex_media,
        AVG(rf.madurez_operativa) AS madurez_operativa_flex_media
    FROM stg_recursos_flexibilidad rf
    GROUP BY rf.zona_id
),
storage AS (
    SELECT
        ad.zona_id,
        SUM(ad.capacidad_potencia_mw) AS storage_potencia_total_mw,
        SUM(ad.capacidad_energia_mwh) AS storage_energia_total_mwh,
        AVG(ad.eficiencia_roundtrip) AS storage_eficiencia_media,
        AVG(ad.coste_operacion_proxy) AS storage_coste_operacion_medio,
        AVG(ad.disponibilidad_media) AS storage_disponibilidad_media
    FROM stg_almacenamiento_distribuido ad
    GROUP BY ad.zona_id
),
interv AS (
    SELECT
        io.zona_id,
        SUM(io.capacidad_alivio_estimado_mw) AS capacidad_alivio_operativo_mw,
        AVG(io.coste_estimado) AS coste_medio_intervencion_eur,
        AVG(io.tiempo_despliegue_dias) AS tiempo_despliegue_medio_dias,
        AVG(io.complejidad_operativa) AS complejidad_operativa_media
    FROM stg_intervenciones_operativas io
    GROUP BY io.zona_id
),
inv AS (
    SELECT
        ip.zona_id,
        SUM(ip.capex_estimado) AS capex_total_cartera_eur,
        SUM(ip.aumento_capacidad_esperado) AS aumento_capacidad_total_mw,
        AVG(ip.horizonte_meses) AS horizonte_medio_meses,
        AVG(ip.facilidad_implementacion) AS facilidad_implementacion_media,
        AVG(ip.impacto_resiliencia) AS impacto_resiliencia_medio
    FROM stg_inversiones_posibles ip
    GROUP BY ip.zona_id
)
SELECT
    z.zona_id,
    z.zona_nombre,
    z.tipo_zona,
    z.region_operativa,
    z.criticidad_territorial,
    z.potencial_flexibilidad,
    z.tension_crecimiento_demanda,
    COALESCE(zpd.peak_demand_zona_mw, 0.0) AS peak_demand_zona_mw,
    COALESCE(zpd.avg_demand_zona_mw, 0.0) AS avg_demand_zona_mw,
    COALESCE(zc.capacidad_total_alimentadores_mw, 0.0) AS capacidad_total_alimentadores_mw,
    COALESCE(zc.capacidad_total_subestaciones_mw, 0.0) AS capacidad_total_subestaciones_mw,
    COALESCE(zc.capacidad_firme_subestaciones_mw, 0.0) AS capacidad_firme_subestaciones_mw,
    COALESCE(f.capacidad_flexible_mw, 0.0) AS capacidad_flexible_mw,
    COALESCE(f.coste_activacion_ponderado_eur_mwh, 0.0) AS coste_activacion_ponderado_eur_mwh,
    COALESCE(f.tiempo_respuesta_medio_min, 0.0) AS tiempo_respuesta_medio_min,
    COALESCE(f.disponibilidad_flex_media, 0.0) AS disponibilidad_flex_media,
    COALESCE(f.fiabilidad_flex_media, 0.0) AS fiabilidad_flex_media,
    COALESCE(f.madurez_operativa_flex_media, 0.0) AS madurez_operativa_flex_media,
    COALESCE(s.storage_potencia_total_mw, 0.0) AS storage_potencia_total_mw,
    COALESCE(s.storage_energia_total_mwh, 0.0) AS storage_energia_total_mwh,
    COALESCE(s.storage_eficiencia_media, 0.0) AS storage_eficiencia_media,
    COALESCE(s.storage_coste_operacion_medio, 0.0) AS storage_coste_operacion_medio,
    COALESCE(s.storage_disponibilidad_media, 0.0) AS storage_disponibilidad_media,
    COALESCE(i.capacidad_alivio_operativo_mw, 0.0) AS capacidad_alivio_operativo_mw,
    COALESCE(i.coste_medio_intervencion_eur, 0.0) AS coste_medio_intervencion_eur,
    COALESCE(i.tiempo_despliegue_medio_dias, 0.0) AS tiempo_despliegue_medio_dias,
    COALESCE(i.complejidad_operativa_media, 0.0) AS complejidad_operativa_media,
    COALESCE(v.capex_total_cartera_eur, 0.0) AS capex_total_cartera_eur,
    COALESCE(v.aumento_capacidad_total_mw, 0.0) AS aumento_capacidad_total_mw,
    COALESCE(v.horizonte_medio_meses, 0.0) AS horizonte_medio_meses,
    COALESCE(v.facilidad_implementacion_media, 0.0) AS facilidad_implementacion_media,
    COALESCE(v.impacto_resiliencia_medio, 0.0) AS impacto_resiliencia_medio,
    COALESCE(f.capacidad_flexible_mw, 0.0) + COALESCE(s.storage_potencia_total_mw, 0.0) * COALESCE(s.storage_disponibilidad_media, 0.0) AS soporte_flex_storage_mw,
    (COALESCE(f.capacidad_flexible_mw, 0.0) + COALESCE(s.storage_potencia_total_mw, 0.0) * COALESCE(s.storage_disponibilidad_media, 0.0)) / NULLIF(COALESCE(zpd.peak_demand_zona_mw, 0.0), 0.0) AS flex_coverage_ratio
FROM stg_zonas_red z
LEFT JOIN zone_peak_demand zpd
    ON z.zona_id = zpd.zona_id
LEFT JOIN zone_capacity zc
    ON z.zona_id = zc.zona_id
LEFT JOIN flex f
    ON z.zona_id = f.zona_id
LEFT JOIN storage s
    ON z.zona_id = s.zona_id
LEFT JOIN interv i
    ON z.zona_id = i.zona_id
LEFT JOIN inv v
    ON z.zona_id = v.zona_id;
