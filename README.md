# Sistema de Inteligência de Rede para Eletrificação Territorial

Sistema reproduzível de apoio à decisão para priorizar intervenções em redes de distribuição sob pressão de eletrificação.

**[Abrir dashboard interativo](https://mfidalgomartins.github.io/sistema-inteligencia-red-electrificacion/)** · **[Relatório analítico PDF](https://mfidalgomartins.github.io/sistema-inteligencia-red-electrificacion/outputs/reports/informe_analitico_red_electrificacion.pdf)**

![Ranking de prioridade das zonas](outputs/graphs/04_ranking_prioridad_zonas.png)

## O que entrega

- Ranking de 24 zonas por risco técnico, impacto de serviço, exposição de ativos, pressão de eletrificação e prioridade económica.
- Recomendação entre reforço de rede, flexibilidade, armazenamento, substituição de ativos e intervenção operacional.
- Comparação de oito cenários localizados de procura, CAPEX, flexibilidade, geração distribuída e degradação de ativos.
- Dashboard HTML autónomo, relatório analítico em PDF e 19 gráficos de publicação.

## Âmbito dos dados

O dataset é sintético e determinístico: cobre 24 zonas e 4.807.056 observações de procura horária entre `2024-01-01 00:00` e `2025-12-31 23:00`. A camada SQL DuckDB integra topologia, procura, eletrificação, geração distribuída, congestão, interrupções, flexibilidade, armazenamento, ativos e alternativas de investimento.

## Execução local

Requer Python 3.12.

```bash
make setup
make release
```

`make release` executa testes, reconstrói o pipeline analítico, gera os artefactos públicos e aplica os quality gates. Comandos individuais:

```bash
make test
make run
make publication
make validate
make smoke
make verify-publication
```

O pipeline canónico, `python -m src`, executa:

1. geração determinística de dados sintéticos;
2. reconstrução da camada SQL DuckDB;
3. features, forecasting e deteção de anomalias;
4. scoring multicritério e cenários;
5. análise, visualizações e dashboard;
6. validação, manifest e smoke checks.

Os dados em `data/raw` e `data/processed`, assim como os diagnósticos técnicos em `outputs/reports`, são regeneráveis e não são versionados.

## Estrutura

```text
src/                  pipeline, modelos e gerador sintético
sql/                  staging, integração, marts, KPIs e validações
tests/                contratos unitários, analíticos e de release
docs/                 métricas, pressupostos, arquitetura e governance
notebooks/            leitura reproduzível dos artefactos processados
outputs/graphs/        19 gráficos PNG para publicação
outputs/dashboard/     dashboard HTML autónomo
outputs/reports/       relatório analítico PDF
scripts/               construção dos artefactos públicos
```

## Metodologia e contratos

- As horas de congestão zonais contam horas distintas com pelo menos um nó congestionado.
- A carga de ponta zonal é o máximo horário da procura agregada da zona.
- A procura horária já incorpora EV e indústria; estes componentes não são somados novamente.
- A exposição de ativos e todos os scores publicados estão limitados a `0-100`.
- A confiança do forecast usa NMAE comparável entre zonas.
- O release é bloqueado por falhas analíticas críticas ou rankings de cenário indistinguíveis.

Definições completas:

- [Definições SQL](docs/sql_metric_definitions.md)
- [Dicionário de métricas](docs/metric_dictionary.md)
- [Framework de scoring](docs/scoring_framework.md)
- [Governance e quality gates](docs/governance_framework.md)

## Artefactos públicos

- [Dashboard autónomo](outputs/dashboard/grid-electrification-command-center.html)
- [Relatório analítico](outputs/reports/informe_analitico_red_electrificacion.pdf)
- [Gráficos de publicação](outputs/graphs/)

## Limitações

Os dados e custos são sintéticos. O sistema demonstra arquitetura analítica e priorização relativa, mas não substitui estudos elétricos detalhados, calibração SCADA/AMI, avaliação regulatória nem aprovação de investimento.

## Stack

Python, pandas, NumPy, DuckDB, Matplotlib, ReportLab, Chart.js e pytest.

## Licença

[MIT](LICENSE)
