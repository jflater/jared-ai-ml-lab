# sql_practice — LCS Senior Living Data

My background is in agricultural research: multi-source datasets, quality control pipelines, and communicating findings across a wide audience. This project is a deliberate translation of those skills into the tools and patterns used in data engineering — SQL, Python, and DuckDB.

The dataset is synthetic but modeled after real operational complexity. Six tables, realistic foreign key relationships, and the kinds of messy questions that show up in actual analysis: who are the highest-acuity residents? which care staff carry the heaviest load? what does revenue look like for residents approaching discharge?

---

## Schema

![ER Diagram](Untitled.png)

Five operational entities feed into three transaction tables:

**`facilities`** is the anchor. Every resident, staff member, and operational record ties back to a facility. This mirrors real senior living operations, where a management company oversees multiple sites with different types and capacities.

**`residents`** holds demographic and admission data. `care_level` and `monthly_rate` make this the primary driver for complexity and revenue analysis.

**`staff`** tracks employees by role and hire date within a facility. The `staff_id` flows into `care_events`, which is where workload analysis lives.

**`admissions`** records each facility admission with type and primary reason — a bridge table that supports tracking residents across multiple admissions or facility transfers.

**`care_events`** is the most granular table: one row per care interaction (medication, assessment, activity), with the delivering staff member and duration.

**`billing`** captures monthly charges per resident, broken into base and additional service fees, with payment status and date.

This was generated with dbdiagram.io, I'm looking into more options that are
more programmatic.  

---

## Generating the data

```bash
python generate_lcs_data.py
duckdb lcs_data.duckdb < load_lcs.data.sql
```

`generate_lcs_data.py` builds a one-year synthetic dataset: 5 facilities, 150 residents, 40 staff, with a ~15% discharge rate to simulate realistic turnover. `load_lcs.data.sql` ingests the CSVs into a `raw` schema in DuckDB using `read_csv_auto()`.

---

## SQL exercises

### Exercise 1 — Window functions (`01_window_functions.sql`)

Identifies longest-tenured residents using `ROW_NUMBER()`, `RANK()`, and `LAG()` across admission dates, with cumulative aggregations for tenure and billing totals. The R equivalent is in `01_window_window_functions.r`.

### Exercise 2 — Ranking care load (`02_rank_care_events.sql`)

Ranks residents by total care events using `DENSE_RANK()` over aggregated counts. Surfaces which residents require the most staff time — useful for staffing and acuity modeling. See `02_rank_care_events.r` for the dplyr translation.

### Exercise 3 — Chained CTEs (`03_ctes.sql`)

A multi-step CTE chain that computes care frequency, length of stay, a composite complexity score, and billing summaries in sequence — then joins them into a single ranked output of high-complexity, high-revenue residents. Output is in `03_ctes_output.md`. The R version is `03_ctes.r`.


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
---

## R-to-SQL comparison (`R_to_SQL.QMD`)

A Quarto document placing dplyr and SQL side by side for the same analytical questions. The goal was to make the translation clear.

---

## CMS provider data pipeline (`ingest.py`)

A Python ingestion script that queries the [CMS Provider Data API](https://data.cms.gov/provider-data) — the public dataset for nursing home and senior living providers.

Key design decisions:
- **Class-based client** (`CMSProviderAPIClient`) with a persistent session and configurable batch size and timeout
- **Paginated fetch** using SQL `LIMIT`/`OFFSET` queries against the DKAN SQL endpoint — handles large result sets without memory issues
- **Schema validation** on the returned DataFrame: checks for empty results and flags any column exceeding 90% null values before writing to disk
- **Structured logging** at INFO level so pipeline progress is visible without print statements

Output lands in `data/cms_provider_data.csv`.

```bash
python ingest.py
```

---

## Files

| File | Description |
|------|-------------|
| `generate_lcs_data.py` | Synthetic dataset generation |
| `load_lcs.data.sql` | DuckDB CSV ingestion script |
| `lcs_data.duckdb` | Populated DuckDB database |
| `01_window_functions.sql` / `.r` | Window function queries + R equivalent |
| `02_rank_care_events.sql` / `.r` | Care load ranking + R equivalent |
| `03_ctes.sql` / `.r` | Chained CTE analysis + R equivalent |
| `03_ctes_output.md` | Query results |
| `R_to_SQL.QMD` / `.html` | Side-by-side language comparison |
| `ingest.py` | CMS provider API pipeline |
| `er_diagram.png` | Schema entity relationship diagram |
| `data/raw/` | Source CSVs (facilities, residents, staff, admissions, care_events, billing) |
| `data/cms_provider_data.csv` | CMS API output |
