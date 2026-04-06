# SQL Practice — LCS Senior Living Data

A portfolio project demonstrating analytical SQL and data engineering skills using a synthetic dataset modeled after a senior living operations context (residents, care events, billing, and facility management). Built with DuckDB, Python, and R.

## Motivation

I built this project to practice SQL patterns commonly needed in data engineering and analytics roles — window functions, CTEs, ranking, and multi-table joins — applied to a SYNTHETIC operational data from long-term care facilities. 

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


| resident_id | first_name | last_name | facility_id | complexity_score |
|------------:|------------|-----------|------------:|-----------------:|
| 3           | Mary       | Jones     | 3           | 22.01            |
| 103         | Barbara    | Brown     | 4           | 21.57            |
| 63          | Patricia   | Garcia    | 5           | 20.6             |
| 68          | James      | Brown     | 2           | 20.51            |
| 42          | John       | Williams  | 1           | 19.64            |
| 15          | Mary       | Rodriguez | 4           | 19.26            |
| 52          | Robert     | Smith     | 4           | 19.22            |
| 136         | Patricia   | Williams  | 3           | 17.98            |
| 49          | Mary       | Rodriguez | 2           | 17.61            |
| 85          | John       | Davis     | 1           | 17.48            |
| 44          | Michael    | Johnson   | 2           | 17.31            |
| 61          | James      | Miller    | 2           | 17.14            |
| 31          | Barbara    | Miller    | 3           | 17.03            |
| 32          | Mary       | Williams  | 4           | 16.92            |
| 142         | Mary       | Rodriguez | 5           | 16.49            |
| 41          | Patricia   | Davis     | 4           | 16.39            |
| 28          | Mary       | Rodriguez | 2           | 15.98            |
| 88          | Robert     | Miller    | 1           | 15.74            |
| 16          | Patricia   | Garcia    | 2           | 15.58            |
| 101         | James      | Jones     | 4           | 15.3             |
| 108         | Patricia   | Jones     | 4           | 15.16            |
| 21          | James      | Garcia    | 2           | 15.03            |
| 138         | Barbara    | Smith     | 2           | 15.03            |
| 27          | Patricia   | Smith     | 3           | 14.94            |
| 48          | Barbara    | Williams  | 5           | 14.9             |
| 8           | Elizabeth  | Smith     | 3           | 14.75            |
| 64          | Patricia   | Brown     | 2           | 14.7             |
| 14          | Robert     | Johnson   | 2           | 14.49            |
| 54          | John       | Garcia    | 5           | 14.42            |
| 107         | James      | Garcia    | 3           | 14.26            |

----

| resident_id | first_name | last_name | facility_id | complexity_score | total_billing | payment_status |
|------------:|------------|-----------|------------:|-----------------:|--------------:|----------------|
| 3           | Mary       | Jones     | 3           | 22.01            | 82738         | Overdue        |
| 103         | Barbara    | Brown     | 4           | 21.57            | 52704         | Pending        |
| 63          | Patricia   | Garcia    | 5           | 20.6             | 62731         | Pending        |
| 68          | James      | Brown     | 2           | 20.51            | 87729         | Overdue        |
| 42          | John       | Williams  | 1           | 19.64            | 63255         | Overdue        |
| 15          | Mary       | Rodriguez | 4           | 19.26            | 68649         | Pending        |
| 52          | Robert     | Smith     | 4           | 19.22            | 48546         | Paid           |
| 136         | Patricia   | Williams  | 3           | 17.98            | 80187         | Pending        |
| 49          | Mary       | Rodriguez | 2           | 17.61            | 107619        | Pending        |
| 85          | John       | Davis     | 1           | 17.48            | 52798         | Pending        |
| 44          | Michael    | Johnson   | 2           | 17.31            | 64267         | Pending        |
| 61          | James      | Miller    | 2           | 17.14            | 59711         | Overdue        |
| 31          | Barbara    | Miller    | 3           | 17.03            | 51764         | Overdue        |
| 32          | Mary       | Williams  | 4           | 16.92            | 103988        | Pending        |
| 142         | Mary       | Rodriguez | 5           | 16.49            | 73646         | Pending        |
| 41          | Patricia   | Davis     | 4           | 16.39            | 101840        | Pending        |
| 28          | Mary       | Rodriguez | 2           | 15.98            | 40693         | Overdue        |
| 88          | Robert     | Miller    | 1           | 15.74            | 91924         | Overdue        |
| 16          | Patricia   | Garcia    | 2           | 15.58            | 74441         | Pending        |
| 101         | James      | Jones     | 4           | 15.3             | 92618         | Overdue        |
| 108         | Patricia   | Jones     | 4           | 15.16            | 42621         | Pending        |
| 21          | James      | Garcia    | 2           | 15.03            | 107344        | Pending        |
| 138         | Barbara    | Smith     | 2           | 15.03            | 67822         | Pending        |
| 27          | Patricia   | Smith     | 3           | 14.94            | 78601         | Pending        |
| 48          | Barbara    | Williams  | 5           | 14.9             | 36411         | Pending        |
| 8           | Elizabeth  | Smith     | 3           | 14.75            | 88605         | Pending        |
| 64          | Patricia   | Brown     | 2           | 14.7             | 95513         | Pending        |
| 14          | Robert     | Johnson   | 2           | 14.49            | 56780         | Pending        |
| 54          | John       | Garcia    | 5           | 14.42            | 75646         | Pending        |
| 107         | James      | Garcia    | 3           | 14.26            | 97624         | Pending        |


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

# React Dashboard
A React + FastAPI dashboard has been added to visualize these query outputs.

### Setup & Run
1. Install backend dependencies and run API:
   ```bash
   cd backend
   python3 -m venv .venv
   source .venv/bin/activate
   pip install fastapi uvicorn duckdb pydantic
   uvicorn main:app --reload
   ```
2. Install frontend dependencies and run React:
   ```bash
   cd frontend
   npm install
   npm run dev
   ```
