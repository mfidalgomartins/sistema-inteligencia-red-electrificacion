-- Dialecto: DuckDB SQL
-- Nivel: staging
-- Objetivo: normalizar tipos y exponer tablas base con contratos de columnas explícitos.

CREATE OR REPLACE VIEW stg_zonas_red AS
SELECT
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(zona_nombre AS VARCHAR) AS zona_nombre,
    CAST(comunidad_autonoma AS VARCHAR) AS comunidad_autonoma,
    CAST(provincia AS VARCHAR) AS provincia,
    CAST(tipo_zona AS VARCHAR) AS tipo_zona,
    CAST(region_operativa AS VARCHAR) AS region_operativa,
    CAST(densidad_demanda AS DOUBLE) AS densidad_demanda,
    CAST(penetracion_generacion_distribuida AS DOUBLE) AS penetracion_generacion_distribuida,
    CAST(criticidad_territorial AS DOUBLE) AS criticidad_territorial,
    CAST(potencial_flexibilidad AS DOUBLE) AS potencial_flexibilidad,
    CAST(riesgo_climatico AS DOUBLE) AS riesgo_climatico,
    CAST(tension_crecimiento_demanda AS DOUBLE) AS tension_crecimiento_demanda
FROM read_csv_auto('{raw_path}/zonas_red.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_subestaciones AS
SELECT
    CAST(subestacion_id AS VARCHAR) AS subestacion_id,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(nombre_subestacion AS VARCHAR) AS nombre_subestacion,
    CAST(capacidad_mw AS DOUBLE) AS capacidad_mw,
    CAST(capacidad_firme_mw AS DOUBLE) AS capacidad_firme_mw,
    CAST(antiguedad_anios AS INTEGER) AS antiguedad_anios,
    CAST(indice_criticidad AS DOUBLE) AS indice_criticidad,
    CAST(digitalizacion_nivel AS DOUBLE) AS digitalizacion_nivel,
    CAST(redundancia_nivel AS DOUBLE) AS redundancia_nivel
FROM read_csv_auto('{raw_path}/subestaciones.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_alimentadores AS
SELECT
    CAST(alimentador_id AS VARCHAR) AS alimentador_id,
    CAST(subestacion_id AS VARCHAR) AS subestacion_id,
    CAST(tipo_red AS VARCHAR) AS tipo_red,
    CAST(capacidad_mw AS DOUBLE) AS capacidad_mw,
    CAST(longitud_km AS DOUBLE) AS longitud_km,
    CAST(nivel_perdidas_estimado AS DOUBLE) AS nivel_perdidas_estimado,
    CAST(exposicion_climatica AS DOUBLE) AS exposicion_climatica,
    CAST(carga_base_esperada AS DOUBLE) AS carga_base_esperada,
    CAST(criticidad_operativa AS DOUBLE) AS criticidad_operativa
FROM read_csv_auto('{raw_path}/alimentadores.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_demanda_horaria AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(subestacion_id AS VARCHAR) AS subestacion_id,
    CAST(alimentador_id AS VARCHAR) AS alimentador_id,
    CAST(demanda_mw AS DOUBLE) AS demanda_mw,
    CAST(demanda_reactiva_proxy AS DOUBLE) AS demanda_reactiva_proxy,
    CAST(temperatura AS DOUBLE) AS temperatura,
    CAST(humedad AS DOUBLE) AS humedad,
    CAST(tipo_dia AS VARCHAR) AS tipo_dia,
    CAST(mes AS INTEGER) AS mes,
    CAST(hora AS INTEGER) AS hora,
    CAST(factor_estacional AS DOUBLE) AS factor_estacional,
    CAST(hora_punta_flag AS BOOLEAN) AS hora_punta_flag,
    CAST(tension_sistema_proxy AS DOUBLE) AS tension_sistema_proxy
FROM read_csv_auto('{raw_path}/demanda_horaria.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_generacion_distribuida AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(tecnologia AS VARCHAR) AS tecnologia,
    CAST(capacidad_instalada_mw AS DOUBLE) AS capacidad_instalada_mw,
    CAST(generacion_mw AS DOUBLE) AS generacion_mw,
    CAST(autoconsumo_estimado_mw AS DOUBLE) AS autoconsumo_estimado_mw,
    CAST(vertido_estimado_mw AS DOUBLE) AS vertido_estimado_mw,
    CAST(curtailment_estimado_mw AS DOUBLE) AS curtailment_estimado_mw
FROM read_csv_auto('{raw_path}/generacion_distribuida.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_demanda_ev AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(tipo_recarga AS VARCHAR) AS tipo_recarga,
    CAST(demanda_ev_mw AS DOUBLE) AS demanda_ev_mw,
    CAST(penetracion_ev AS DOUBLE) AS penetracion_ev,
    CAST(horario_recarga_dominante AS VARCHAR) AS horario_recarga_dominante
FROM read_csv_auto('{raw_path}/demanda_ev.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_demanda_electrificacion_industrial AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(cluster_industrial AS VARCHAR) AS cluster_industrial,
    CAST(demanda_industrial_adicional_mw AS DOUBLE) AS demanda_industrial_adicional_mw,
    CAST(perfil_operativo AS VARCHAR) AS perfil_operativo,
    CAST(elasticidad_flexibilidad_proxy AS DOUBLE) AS elasticidad_flexibilidad_proxy
FROM read_csv_auto('{raw_path}/demanda_electrificacion_industrial.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_eventos_congestion AS
SELECT
    CAST(evento_id AS VARCHAR) AS evento_id,
    CAST(timestamp_inicio AS TIMESTAMP) AS timestamp_inicio,
    CAST(timestamp_fin AS TIMESTAMP) AS timestamp_fin,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(subestacion_id AS VARCHAR) AS subestacion_id,
    CAST(alimentador_id AS VARCHAR) AS alimentador_id,
    CAST(severidad AS VARCHAR) AS severidad,
    CAST(energia_afectada_mwh AS DOUBLE) AS energia_afectada_mwh,
    CAST(carga_relativa_max AS DOUBLE) AS carga_relativa_max,
    CAST(causa_principal AS VARCHAR) AS causa_principal,
    CAST(impacto_servicio_flag AS BOOLEAN) AS impacto_servicio_flag
FROM read_csv_auto('{raw_path}/eventos_congestion.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_interrupciones_servicio AS
SELECT
    CAST(interrupcion_id AS VARCHAR) AS interrupcion_id,
    CAST(timestamp_inicio AS TIMESTAMP) AS timestamp_inicio,
    CAST(timestamp_fin AS TIMESTAMP) AS timestamp_fin,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(subestacion_id AS VARCHAR) AS subestacion_id,
    CAST(clientes_afectados AS BIGINT) AS clientes_afectados,
    CAST(energia_no_suministrada_mwh AS DOUBLE) AS energia_no_suministrada_mwh,
    CAST(causa AS VARCHAR) AS causa,
    CAST(nivel_severidad AS VARCHAR) AS nivel_severidad,
    CAST(relacion_congestion_flag AS BOOLEAN) AS relacion_congestion_flag
FROM read_csv_auto('{raw_path}/interrupciones_servicio.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_activos_red AS
SELECT
    CAST(activo_id AS VARCHAR) AS activo_id,
    CAST(tipo_activo AS VARCHAR) AS tipo_activo,
    CAST(subestacion_id AS VARCHAR) AS subestacion_id,
    CAST(alimentador_id AS VARCHAR) AS alimentador_id,
    CAST(edad_anios AS INTEGER) AS edad_anios,
    CAST(estado_salud AS DOUBLE) AS estado_salud,
    CAST(probabilidad_fallo_proxy AS DOUBLE) AS probabilidad_fallo_proxy,
    CAST(criticidad AS DOUBLE) AS criticidad,
    CAST(capex_reposicion_estimado AS DOUBLE) AS capex_reposicion_estimado,
    CAST(opex_mantenimiento_estimado AS DOUBLE) AS opex_mantenimiento_estimado
FROM read_csv_auto('{raw_path}/activos_red.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_recursos_flexibilidad AS
SELECT
    CAST(recurso_id AS VARCHAR) AS recurso_id,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(tipo_recurso AS VARCHAR) AS tipo_recurso,
    CAST(capacidad_flexible_mw AS DOUBLE) AS capacidad_flexible_mw,
    CAST(coste_activacion_eur_mwh AS DOUBLE) AS coste_activacion_eur_mwh,
    CAST(tiempo_respuesta_min AS INTEGER) AS tiempo_respuesta_min,
    CAST(disponibilidad_media AS DOUBLE) AS disponibilidad_media,
    CAST(fiabilidad_activacion AS DOUBLE) AS fiabilidad_activacion,
    CAST(madurez_operativa AS DOUBLE) AS madurez_operativa
FROM read_csv_auto('{raw_path}/recursos_flexibilidad.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_almacenamiento_distribuido AS
SELECT
    CAST(storage_id AS VARCHAR) AS storage_id,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(capacidad_energia_mwh AS DOUBLE) AS capacidad_energia_mwh,
    CAST(capacidad_potencia_mw AS DOUBLE) AS capacidad_potencia_mw,
    CAST(eficiencia_roundtrip AS DOUBLE) AS eficiencia_roundtrip,
    CAST(coste_operacion_proxy AS DOUBLE) AS coste_operacion_proxy,
    CAST(disponibilidad_media AS DOUBLE) AS disponibilidad_media
FROM read_csv_auto('{raw_path}/almacenamiento_distribuido.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_intervenciones_operativas AS
SELECT
    CAST(intervencion_id AS VARCHAR) AS intervencion_id,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(tipo_intervencion AS VARCHAR) AS tipo_intervencion,
    CAST(capacidad_alivio_estimado_mw AS DOUBLE) AS capacidad_alivio_estimado_mw,
    CAST(coste_estimado AS DOUBLE) AS coste_estimado,
    CAST(tiempo_despliegue_dias AS INTEGER) AS tiempo_despliegue_dias,
    CAST(complejidad_operativa AS DOUBLE) AS complejidad_operativa
FROM read_csv_auto('{raw_path}/intervenciones_operativas.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_inversiones_posibles AS
SELECT
    CAST(inversion_id AS VARCHAR) AS inversion_id,
    CAST(zona_id AS VARCHAR) AS zona_id,
    CAST(tipo_inversion AS VARCHAR) AS tipo_inversion,
    CAST(capex_estimado AS DOUBLE) AS capex_estimado,
    CAST(opex_incremental_estimado AS DOUBLE) AS opex_incremental_estimado,
    CAST(reduccion_riesgo_esperada AS DOUBLE) AS reduccion_riesgo_esperada,
    CAST(aumento_capacidad_esperado AS DOUBLE) AS aumento_capacidad_esperado,
    CAST(horizonte_meses AS INTEGER) AS horizonte_meses,
    CAST(facilidad_implementacion AS DOUBLE) AS facilidad_implementacion,
    CAST(impacto_resiliencia AS DOUBLE) AS impacto_resiliencia
FROM read_csv_auto('{raw_path}/inversiones_posibles.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW stg_escenario_macro AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(escenario AS VARCHAR) AS escenario,
    CAST(crecimiento_demanda_indice AS DOUBLE) AS crecimiento_demanda_indice,
    CAST(penetracion_ev_indice AS DOUBLE) AS penetracion_ev_indice,
    CAST(electrificacion_industrial_indice AS DOUBLE) AS electrificacion_industrial_indice,
    CAST(presion_capex_indice AS DOUBLE) AS presion_capex_indice
FROM read_csv_auto('{raw_path}/escenario_macro.csv', HEADER = TRUE);
