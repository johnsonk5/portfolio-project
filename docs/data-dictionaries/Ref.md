# Reference Data Dictionary

## `ref.simulation_types`

*DuckDB Table in ref schema*

| Column | Type | Description |
| --- | --- | --- |
| `simulation_type_id` | `int` | Stable numeric simulation type identifier. |
| `simulation_type_code` | `object` | Stable simulation type code. |
| `description` | `object` | Human-readable description. |
| `fill_price_basis` | `object` | Fill price basis: `close`, `next_open`, `open`, or `vwap`. |
| `slippage_model` | `object` | Slippage model: `none`, `fixed_bps`, or `volatility_based`. |
| `slippage_bps` | `float` | Fixed or baseline slippage assumption in basis points. |
| `commission_model` | `object` | Commission model code or label. |
| `lookahead_safe_flag` | `bool` | Whether the fill convention avoids same-bar lookahead. |
| `is_active` | `bool` | Active simulation type flag. |
| `default_flag` | `bool` | Whether this simulation type is the default baseline. |
| `notes` | `object` | Optional implementation or usage notes. |
| `slippage_params_json` | `object` | JSON-serialized parameters for complex slippage models. |

## `ref.run_types`

*DuckDB Table in ref schema*

| Column | Type | Description |
| --- | --- | --- |
| `run_type_code` | `object` | Run type: `backtest`, `simulation`, `paper`, or `live`. |
| `description` | `object` | Human-readable description. |
| `is_active` | `bool` | Active run type flag. |
