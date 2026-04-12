# Arquitectura del Dashboard Ejecutivo

## Objetivo
Convertir el HTML en una herramienta de decisión para utility: diagnóstico + priorización + trade-offs + escenarios.

## Principios
- Lectura en 20 segundos para dirección.
- Trazabilidad dato → insight → intervención.
- Filtros globales con impacto real en KPIs, gráficos, escenarios y tabla final.
- Narrativa especializada en red eléctrica (congestión, ENS, resiliencia, flexibilidad, electrificación y CAPEX).

## Módulos
1. Header ejecutivo y contexto metodológico.
2. Executive summary (qué pasa / por qué / qué decisión).
3. KPI cards con foco operativo y económico.
4. Estado de red y congestión (incluye heatmap horario por región).
5. Resiliencia y calidad de servicio.
6. Flexibilidad, almacenamiento y comparador multicriterio.
7. Electrificación y curtailment.
8. Priorización y criticidad por zona/subestación/alimentador.
9. Escenarios y simulador what-if táctico.
10. Benchmark de umbrales con semáforos.
11. Plan por horizonte y drill-down territorial.
12. Tabla accionable con export de priorización.

## Consistencia de producto
- Dashboard oficial único: `dashboard_inteligencia_red.html`.
- Se elimina duplicidad de artefactos para evitar divergencia en comités.
