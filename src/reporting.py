from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from .config import CONFIG, Config


def write_reports(
    priorities: pd.DataFrame,
    scenario_summary: pd.DataFrame,
    validation_checks: pd.DataFrame,
    kpi_network_overview: pd.DataFrame,
    config: Config = CONFIG,
) -> Dict[str, Path]:
    config.outputs_reports.mkdir(parents=True, exist_ok=True)

    top10 = priorities.head(10)
    overview = kpi_network_overview.iloc[0]
    failed_checks = validation_checks[validation_checks["passed"] == 0]

    memo = f"""
# Memo Ejecutivo

## Contexto
El sistema analítico identifica pérdidas de capacidad operativa y eficiencia económica en red de distribución por congestión, envejecimiento de activos, crecimiento de demanda y electrificación territorial.

## Hallazgos clave
- Feeders monitorizados: **{int(overview['feeders'])}**
- Horas de congestión agregadas: **{int(overview['total_congestion_hours']):,}**
- ENS anual agregada: **{overview['total_ens_mwh']:.2f} MWh**
- Curtailment anual agregado: **{overview['total_curtailment_mwh']:.2f} MWh**
- Incremento de pico esperado acumulado a 2030: **{overview['incremental_peak_2030_mw']:.2f} MW**

## Recomendación de estrategia de inversión
1. Priorizar feeders de categoría **Crítica** y **Alta** con mayor brecha entre pico previsto y límite térmico.
2. Aplicar combinación de refuerzo físico y flexibilidad en zonas con baja factibilidad de permisos.
3. Acelerar automatización en activos con alta ENS y degradación.
4. Mantener portafolio de almacenamiento donde el curtailment es estructuralmente elevado.

## Top 10 prioridades
{top10[['priority_rank','feeder_id','territory_id','priority_tier','priority_score','recommended_action','estimated_capex_k_eur']].to_markdown(index=False)}

## Comparativa de escenarios
{scenario_summary.to_markdown(index=False)}
"""

    validation_md = "# Validation Report\n\n"
    validation_md += "## Resultado de controles\n"
    validation_md += validation_checks.to_markdown(index=False)
    validation_md += "\n\n"

    if failed_checks.empty:
        validation_md += "Todos los controles definidos en SQL han pasado correctamente.\n"
    else:
        validation_md += "## Controles fallidos\n"
        validation_md += failed_checks.to_markdown(index=False)
        validation_md += "\n"

    assumptions_md = """
# Supuestos Económicos

## Costes unitarios de referencia
- Refuerzo físico de red: 710 kEUR por MVA equivalente.
- Automatización avanzada de feeder: 230 kEUR por feeder.
- Flexibilidad contratada: 95 kEUR por MW de alivio.
- Almacenamiento en batería: 530 kEUR por MW de potencia.

## Valor económico de impactos evitables
- Curtailment evitado: 90 EUR/MWh.
- Energía no suministrada (ENS) evitada: 4.000 EUR/MWh.
- Penalización operativa por congestión: 75 EUR por hora equivalente de congestión.

## Horizonte y lógica financiera
- Horizonte principal de evaluación: 2026-2030.
- El score económico combina ratio beneficio/capex con pérdidas operativas esperadas.
- La priorización final pondera necesidad técnico-operativa y factibilidad de ejecución territorial.
"""

    memo_path = config.outputs_reports / "memo_ejecutivo_es.md"
    validation_path = config.outputs_reports / "validation_report.md"
    assumptions_path = config.outputs_reports / "economic_assumptions.md"

    memo_path.write_text(memo.strip() + "\n", encoding="utf-8")
    validation_path.write_text(validation_md.strip() + "\n", encoding="utf-8")
    assumptions_path.write_text(assumptions_md.strip() + "\n", encoding="utf-8")

    return {
        "memo": memo_path,
        "validation": validation_path,
        "assumptions": assumptions_path,
    }
