# Validation Report

## Resultado de controles
| check_name                  |   passed |   observed_value |   expected_or_reference |
|:----------------------------|---------:|-----------------:|------------------------:|
| raw_feeders_unique          |        1 |              160 |                     160 |
| hourly_no_null_key          |        1 |                0 |                       0 |
| hourly_non_negative_demand  |        1 |                0 |                       0 |
| mart_feeder_count_alignment |        1 |              160 |                     160 |
| congestion_rate_bounds      |        1 |                0 |                       0 |

Todos los controles definidos en SQL han pasado correctamente.
