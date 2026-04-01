# SQL Practice — LCS Senior Living Data

A portfolio project demonstrating analytical SQL and data engineering skills using a synthetic dataset modeled after a senior living operations context (residents, care events, billing, and facility management). Built with DuckDB, Python, and R.

## Motivation

I built this project to practice SQL patterns commonly needed in data engineering and analytics roles — window functions, CTEs, ranking, and multi-table joins — applied to a domain I find genuinely interesting: operational data from long-term care facilities. The dataset mirrors the kinds of questions a care operations team or a data platform team would actually ask.

## Tech Stack

| Tool | Role |
|------|------|
| **DuckDB** | Local analytical database engine |
| **Python** | Synthetic data generation (`generate_lcs_data.py`) |
| **SQL** | Analytical queries (window functions, CTEs, rankings) |
| **R** | R-equivalent translations for each SQL exercise |
| **Quarto** | Side-by-side R vs. SQL comparison document |

## Dataset Schema

Six tables loaded into a `raw` schema in DuckDB:

```
raw.facilities    — facility_id, name, location, capacity, type
raw.residents     — resident_id, facility_id, name, age, care_level, admission_date, discharge_date
raw.staff         — staff_id, facility_id, role, hire_date
raw.admissions    — admission records linking residents to facilities
raw.care_events   — care_event_id, resident_id, event_type, staff_id, duration_minutes, event_date
raw.billing       — billing_id, resident_id, base_charge, additional_services, total_charge, payment_status
```

Data is generated via `generate_lcs_data.py` and loaded into DuckDB using `load_lcs.data.sql`.

## SQL Exercises

### `01_window_functions.sql`
**Question:** Which residents have been here longest? How do stays compare across facilities?

Demonstrates:
- `ROW_NUMBER()`, `RANK()`, and `LAG()` over partitioned windows
- `DATEDIFF` with `COALESCE` to handle current (non-discharged) residents
- Cumulative sums (`SUM(...) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`)
- Facility-level aggregation on top of window-computed fields

### `02_rank_care_events.sql`
**Question:** Which residents require the most staff attention?

Demonstrates:
- `DENSE_RANK()` over aggregated counts
- Grouping before ranking to identify high-utilization residents

### `03_ctes.sql`
**Question:** Who are the most complex, highest-revenue residents?

Demonstrates:
- Multi-step CTE chains (4 CTEs: `resident_care_frequency`, `resident_tenure`, `resident_complexity`, `billing_summary`)
- A composite complexity score built from care frequency, event diversity, and length of stay
- Joining complexity scores to billing data to surface high-need, high-revenue residents
- Payment status logic using conditional aggregation (`MAX(CASE WHEN ...)`)

## R-to-SQL Translation

Each `.sql` file has a companion `.r` file implementing the same logic using `dplyr` and `dbplyr`. The `R_to_SQL.QMD` Quarto document renders them side-by-side, which illustrates how the same analytical intent maps across two paradigms — useful for teams that operate in both environments.

## Setup

```bash
# Generate synthetic data
python generate_lcs_data.py

# Load into DuckDB
duckdb lcs_data.duckdb < load_lcs.data.sql

# Run a query
duckdb lcs_data.duckdb < 03_ctes.sql
```

## Skills Demonstrated

- Analytical SQL: window functions, CTEs, ranking, conditional aggregation
- Data modeling: schema design with a realistic normalized structure
- Cross-tool fluency: same logic expressed in SQL and R
- Domain reasoning: queries answer real operational questions, not contrived exercises
